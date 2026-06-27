use crate::{Connection, queries, replace_all_instances, resultset_to_table_state};
use crate::state::{TableState, LSN, lsn_to_hex, ConnectionState};
use crate::connection::ConnectionConfig;
use crate::utils::lsn_from_resultset;
use crate::connection::ResultSet;


pub struct ConnectionManagerBuilder {
	connection: Option<ConnectionConfig>,
	database: Option<String>
}

pub struct ConnectionManager {
	state: ConnectionState,
	connection: Connection
}


impl ConnectionManagerBuilder {

	pub async fn build(mut self) -> Result<ConnectionManager, String> {
		if !self.connection.is_some() {
			return Err("Connection Configuration is not valid!".to_string());
		}
		let connection = match Connection::new(self.connection.unwrap()).build().await {
			Ok(res) => res,
			Err(e) => return Err(e)
		};

		let state = ConnectionState::new(self.database.unwrap());

		Ok(ConnectionManager {
			state,
			connection
		})
	}

	pub fn connection(mut self, conn: ConnectionConfig) -> Self {
		self.connection = Some(conn);
		self
	}

	pub fn database(mut self, database: String) -> Self {
		self.database = Some(database);
		self
	}
}

impl ConnectionManager {

	pub fn new() -> ConnectionManagerBuilder {
		ConnectionManagerBuilder {
			connection: None,
			database: None
		}
	}

	pub async fn close(mut self) -> Result<(), String> {
		self.connection.close().await?;

		Ok(())
	}

	pub fn get_monitored_tables(&self) -> Vec<(String, String)> {
		self.state.tables().iter().map(|x| (self.state.database(), x.clone())).collect()
	}


	///
	/// Part dedicated to getting CDC tables and registering them
	///

	/// Get all change tables and save them as to be monitored
	/// Used for initialization
	pub async fn register_all_change_tables(&mut self) -> Result<(), String> {

		let change_tables = self.pull_change_tables().await?;
		for table in change_tables {
			let _ = self.state.add_table(table);
		}

		self.reset_lsn().await?;

		Ok(())
	}

	/// Register only the change tables present in the parameter list
	pub async fn register_change_tables(&mut self, tables: &Vec<String>) -> Result<(), String> {

		let mut change_tables = self.pull_change_tables().await?;
		change_tables = change_tables
			.into_iter()
			.filter(|v| tables.contains(&v.data_table))
			.collect();

		if change_tables.len() != tables.len() {
			println!("WARN! Not all change tables to be registered are being registered\nThose will be not considered");
		}

		for table in change_tables {
			let _ = self.state.add_table(table);
		}

		Ok(())
	}

	/// Get a vector of all the change tables in the CDC
	async fn pull_change_tables(&mut self) -> Result<Vec<TableState>, String> {
		let q = replace_all_instances(
			queries::DATABASE_PLACEHOLDER,
			queries::SELECT_CDC_ENABLED_TABLES,
			&(queries::to_parameter(&self.state.database()))
		);
		let q = replace_all_instances(
			queries::DATABASE_PARAMETER,
			&q,
			&self.state.database()
		);

		let tables = self.connection.query(q).await?;
		let tables = match resultset_to_table_state(Some(self.state.database()), tables) {
			Ok(r) => r,
			Err(e) => { return Err(e); }
		};

		Ok(tables)
	}

	/// Updates the tracked LSN to the current latest one
	pub async fn reset_lsn(&mut self) -> Result<(), String> {
		let q = replace_all_instances(
			queries::DATABASE_PLACEHOLDER,
			queries::SELECT_MAX_LSN,
			&queries::to_parameter(&self.state.database())
		);
		let res = self.connection.query(q).await.unwrap();
		let max_lsn = lsn_from_resultset(res).unwrap();

		self.state.update_lsn_tracking(max_lsn);
		Ok(())
	}


	///
	/// Here's where the magic happens
	///

	/// Checks whether there are changes in the latest LSN. A simple equality comparison
	/// is enough, since LSN is always greater than the previous one
	pub async fn has_changes(&mut self) -> Result<bool, String> {

		let q = replace_all_instances(
			queries::DATABASE_PLACEHOLDER,
			queries::SELECT_MAX_LSN,
			&queries::to_parameter(&self.state.database())
		);
		let res = self.connection.query(q).await.unwrap();
		let max_lsn = lsn_from_resultset(res).unwrap();

		if self.state.get_tracked_lsn().0.iter().zip(max_lsn.iter()).all(|(a,b)| a == b) {
			return Ok(false);
		}

		Ok(true)
	}

	/// Poll changes for all monitored change tables
	pub async fn poll_changes(&mut self) -> Result<Vec<String>, String> {

		let q = replace_all_instances(
			queries::DATABASE_PLACEHOLDER,
			queries::SELECT_MAX_LSN,
			&queries::to_parameter(&self.state.database())
		);
		let rs = self.connection.query(q).await.unwrap();
		let max_lsn = lsn_from_resultset(rs).unwrap();

		self.state.update_lsn_tracking(max_lsn);

		let mut res = Vec::new();
		for table in self.state.tables() {
			let table_diff = self.poll_changes_for_table(&table, &max_lsn).await?;
			if table_diff.len() > 0 {
				res.extend(table_diff.to_json_values());
			}
		}

		self.state.update_lsn_tracking(max_lsn);

		Ok(res)
	}

	/// Get all new changes for a given table
	async fn poll_changes_for_table(&mut self, table: &String, max_lsn: &LSN) -> Result<ResultSet, String> {

		let table_info = self.state.table_info(table).unwrap();
		let q = replace_all_instances(
			queries::DATABASE_PLACEHOLDER,
			queries::SELECT_ALL_CHANGES_FROM_TO,
			&queries::to_parameter(&self.state.database())
		);
		let q = replace_all_instances(
			queries::TABLE_PLACEHOLDER,
			&q,
			&table_info.cdc_table.to_string()
		);
		let q = replace_all_instances(
			queries::COLUMNS_PLACEHOLDER,
			&q,
			&table_info.columns
		);
		let q = replace_all_instances(
			queries::LSN1,
			&q,
			&lsn_to_hex(&table_info.first_lsn)
		);
		let q = replace_all_instances(
			queries::LSN2,
			&q,
			&lsn_to_hex(&self.state.get_latest_ref().0)
		);
		let q = replace_all_instances(
			queries::LSN3,
			&q,
			&lsn_to_hex(max_lsn)
		);

		let mut changes = self.connection.query(q).await?;
		changes.database = Some(self.state.database());
		changes.table = Some(table.to_string());

		Ok(changes)
	}
}
