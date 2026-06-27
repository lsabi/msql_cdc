use regex::Regex;

use serde::{Serialize, Deserialize};

use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tiberius::{Client, Config, AuthMethod, error::Error, Row};

use crate::sql_row_to_json;


// Define a new type for the client with the generic type incorporated
pub type Client_type = Client<Compat<TcpStream>>;

pub struct Connection {
	client: Client_type
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionConfig {
	host: Option<String>,
	port: Option<u16>,
	user: Option<String>,
	password: Option<String>,
	trust_ssl: Option<bool>
}

pub struct ConnectionBuilder {
	config: ConnectionConfig
}


impl Connection {

	pub fn new(config: ConnectionConfig) -> ConnectionBuilder {
		ConnectionBuilder {
			config
		}
	}

	pub async fn query(&mut self, query: String) -> Result<ResultSet, String> {

		let _res = self.client.simple_query(query).await.unwrap();
		let mssql_rows = _res.into_results().await.unwrap();
		let mut all = Vec::default();
        for batch in mssql_rows {
            for r in batch {
                all.push(Row::from(r));
            }
        }

		Ok(ResultSet::new(all))
	}

	pub async fn close(mut self) -> Result<(), String> {
		match self.client.close().await {
			Ok(_) => Ok(()),
			Err(e) => Err(format!("{:?}", e).to_string())
		}
	}
}


#[derive(Debug)]
pub(crate) struct ResultSet {
	data: Vec<Row>,
	pub(crate) database: Option<String>,
	pub(crate) table: Option<String>
}

impl ResultSet {

	pub fn new(data: Vec<Row>) -> Self {
		ResultSet {
			data,
			database: None,
			table: None
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn to_json_values(&self) -> Vec<String> {
		let mut res: Vec<String> = Vec::new();

		for row in &self.data {
			let mut info_vec = Vec::new();
			if self.database.is_some() {
				info_vec.push(format!("\"database\": \"{}\"", self.database.clone().unwrap().clone()));
			}
			if self.table.is_some() {
				info_vec.push(format!("\"table\": \"{}\"", self.table.clone().unwrap().clone()));
			}
			if info_vec.len() > 0 {
				res.push(format!("{{{}, {}}}", info_vec.join(","), sql_row_to_json(row)));
			} else {
				res.push(format!("{}", sql_row_to_json(row)));
			}
		}
		res
	}

	pub fn to_json(&self) -> String {
		format!("[{}]", self.to_json_values().join(", "))
	}

	pub fn data(&self) -> &Vec<Row> {
		&self.data
	}
}


fn replace_all_instances(r: &str, original_string: &str, replacement: &str) -> String {
	Regex::new(r)
		.unwrap()
		.replace_all(original_string, String::from(replacement))
		.to_string()
}


impl ConnectionConfig {

	pub fn new() -> Self {
		ConnectionConfig {
			host: None,
			port: None,
			user: None,
			password: None,
			trust_ssl: None
		}
	}
	pub fn host(mut self, host: String) -> Self {
		self.host = Some(host);
		self
	}

	pub fn port(mut self, port: u16) -> Self {
		self.port = Some(port);
		self
	}

	pub fn user(mut self, user: String) -> Self {
		self.user = Some(user);
		self
	}

	pub fn password(mut self, password: String) -> Self {
		self.password = Some(password);
		self
	}

	pub fn trust_ssl(mut self, trust: bool) -> Self {
		self.trust_ssl = Some(trust);
		self
	}
}


impl ConnectionBuilder {

	pub async fn build(self) -> Result<Connection, String> {

		let mut config = Config::new();
		let mut errors = Vec::new();

		if self.config.host.is_some() {
			config.host(self.config.host.unwrap());
		} else {
			errors.push("Host".to_string());
		}

		if self.config.port.is_some() {
			config.port(self.config.port.unwrap());
		} else {
			errors.push("Port".to_string());
		}

		if self.config.user.is_some() && self.config.password.is_some() {
			config.authentication(
				AuthMethod::sql_server(self.config.user.unwrap(), self.config.password.unwrap())
			);
		} else {
			errors.push("<Username, Password>".to_string());
		}

		if errors.len() > 0 {
			return Err(format!("The values below have not been declared!\n{}\n", errors.join("\n- ")));
		}

		if self.config.trust_ssl.is_some() && self.config.trust_ssl.unwrap() {
			config.trust_cert(); // on production, it is not a good idea to do this
		}
 
		let tcp = match TcpStream::connect(config.get_addr()).await {
			Ok(res) => res,
			Err(_) => return Err("Cannot get TCP Stream".to_string())
		};

		if tcp.set_nodelay(true).is_ok() { } else {
			println!("Cannot set no delay on the TCP socket!");
		}

		// To be able to use Tokio's tcp, we're using the `compat_write` from
		// the `TokioAsyncWriteCompatExt` to get a stream compatible with the
		// traits from the `futures` crate.
		let mut client = match Client::connect(config, tcp.compat_write()).await {
			Ok(c) => c,
			Err(Error::Routing { host, port }) => {
				return Err(format!("{}:{}", host, port))
			}
			Err(e) => return Err(format!("{}", e))
		};

		Ok(Connection {
			client
		})
	}
}
