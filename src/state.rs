use chrono::{Utc, DateTime};
use std::collections::HashMap;

use crate::connection::ResultSet;


pub(crate) type LSN = [u8; 10];

/// Convert LSN parameter into a binary string
pub(crate) fn lsn_to_hex(lsn: &LSN) -> String {

	let mut hex_string = String::with_capacity(lsn.len() * 2);
	// Append the leading 0s and push a hex upper case representation
	hex_string.push_str("0x");
	for byte in lsn.into_iter() {
		hex_string.push_str(&format!("{:02X}", byte));
	}
	hex_string
}


#[derive(Debug)]
pub struct ConnectionState {
	database: String,
	latest_poll: DateTime<Utc>,
	latest_lsn: LSN,
	tables: HashMap<String, usize>,
	cdc_tables: Vec<String>,
	first_lsn_table: Vec<LSN>,
	monitored_columns: Vec<String>,
	data_table: Vec<String>
}


impl ConnectionState {

	pub fn new(database: String) -> ConnectionState {
		ConnectionState {
			database,
			latest_poll: Utc::now(),
			latest_lsn: [0; 10],
			tables: HashMap::new(),
			cdc_tables: vec![],
			first_lsn_table: vec![],
			monitored_columns: vec![],
			data_table: vec![]
		}
	}

	/// Update the tracking LSN to the latest one
	pub fn update_lsn_tracking(&mut self, lsn: LSN) {
		self.latest_poll = Utc::now();
		self.latest_lsn = lsn.clone();
	}

	// Get database name
	pub fn database(&self) -> String {
		self.database.clone()
	}

	/// Register a new table with both start end last LSN references
	pub fn add_table(&mut self, table_info: TableState) -> usize {

		// Check that the table is not already present
		// If present, return the index without overriding the data
		match self.tables.get(&table_info.data_table) {
			Some(i) => return *i,
			_ => { }
		};

		let index = self.cdc_tables.len();
		self.tables.insert(table_info.data_table.clone(), index.clone());
		self.cdc_tables.push(table_info.cdc_table);
		self.first_lsn_table.push(table_info.first_lsn);
		self.monitored_columns.push(table_info.columns.clone());
		self.data_table.push(table_info.data_table.clone());

		index
	}

	/// Deletes ALL the information regarding the table passed as parameter
	pub fn remove_table(&mut self, table_name: String) -> Result<(), String> {

		let index = match self.tables.get(&table_name) {
			Some(i) => *i,
			None => return Err(format!("Table {} is not registered!", table_name).to_string())
		};

		self.first_lsn_table.remove(index.clone());
		self.cdc_tables.remove(index.clone());
		self.monitored_columns.remove(index.clone());
		self.data_table.remove(index.clone());
		self.tables.remove(&table_name);

		Ok(())
	}

	/// Return the latest <LSN, Datetime> tuple
	pub fn get_latest_ref(&self) -> (LSN, DateTime<Utc>) {
		(self.latest_lsn, self.latest_poll.clone())
	}

	/// Update last LSN polled value
	pub fn update_lsn(&mut self, lsn_time: DateTime<Utc>, lsn: LSN) -> Result<(), String> {

		self.latest_poll = lsn_time;
		self.latest_lsn = lsn;
		Ok(())
	}

	/// Get the latest reference (LSN and last polled)
	pub fn get_tracked_lsn(&self) -> (LSN, DateTime<Utc>) {
		(self.latest_lsn.clone(), self.latest_poll.clone())
	}

	/// Get a list of the tracked tables
	pub fn tables(&self) -> Vec<String> {
		self.tables.keys().cloned().collect()
	}

	/// Get the stored state about a table.
	pub fn table_info(&self, table: &String) -> Result<TableState, String> {

		let idx = match self.tables.get(table) {
			Some(i) => i,
			None => { return Err(format!("Table {} not registered!", table)); }
		};

		Ok(
			TableState {
				database: self.database.clone(),
				cdc_table: self.cdc_tables[*idx].clone(),
				first_lsn: self.first_lsn_table[*idx].clone(),
				data_table: self.data_table[*idx].clone(),
				columns: self.monitored_columns[*idx].clone()
		})
	}
}


pub fn resultset_to_table_state(db: Option<String>, rs: ResultSet) -> Result<Vec<TableState>, String> {

	let database = match db {
		Some(x) => x,
		None => "".to_string()
	};
	let mut res = Vec::new();
		
	for row in rs.data() {
		let lsn: [u8; 10] = row.get::<&[u8], _>("start_lsn").unwrap().try_into().unwrap();
		res.push(
			TableState {
				database: database.clone(),
				cdc_table: row.get::<&str, _>("table_name").unwrap().into(),
				first_lsn: lsn.clone(),
				data_table: row.get::<&str, _>("capture_instance").unwrap().to_string(),
				columns: row.get::<&str, _>("columns").unwrap().to_string()
			}
		);
	}

	Ok(res)
}


#[derive(Debug)]
pub(crate) struct TableState {
	pub database: String,
	pub cdc_table: String,
	pub first_lsn: LSN,
	pub data_table: String,
	pub columns: String
}
