extern crate log;
use log::{info, error, debug};

use serde_json;

use serde::{Serialize, Deserialize};
use tokio::{time::interval, signal::ctrl_c};
use tokio::signal::unix::{signal, SignalKind};
use std::{fs::read_to_string, time::Duration};

use cli::{StreamingClient, StreamingConfig};
use connector::{ConnectionConfig, ConnectionManager};


// Default query interval is 5 seconds
fn default_query_interval() -> u64 { 5000 }
// Default topic prefix
fn default_stream_name() -> String { "dev".to_string() }
// Default list of tables to be monitored
fn default_tables() -> Vec<String> { Vec::new() }


#[derive(Debug, Serialize, Deserialize)]
struct Streaming {
	config: StreamingConfig,
	#[serde(default = "default_stream_name")]
	stream_name: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
	connection: ConnectionConfig,
	#[serde(default = "default_query_interval")]
	query_interval: u64,
	database: String,
    #[serde(default = "default_tables")]
	tables: Vec<String>,
	streaming: Streaming
}


#[tokio::main]
async fn main() -> Result<(), String> {

	env_logger::init();

	let path = "./config.json";
    let data = match read_to_string(path) {
		Ok(d) => d,
		Err(e) => { error!("{:?}", e); return Err(format!("{:?}", e).to_string()); }
	};
	let configuration: Config = serde_json::from_str(&data).expect("Unable to parse");

	let mut streaming = match StreamingClient::new(&configuration.streaming.config).await {
		Ok(s) => s,
		Err(e) => return Err(format!("{}", e))
	};
	info!("Connection to IGGY established!");

	let mut cm = match ConnectionManager::new()
		.database(configuration.database.to_string())
		.connection(configuration.connection)
		.build()
		.await {
		Ok(r) => r,
		Err(e) => {
			error!("{:?}", e);
			return Err("".to_string());
		}
	};
	info!("Connection to the DB established!");

	if configuration.tables.len() == 0 {
		cm.register_all_change_tables().await?;
	} else {
		cm.register_change_tables(&configuration.tables).await?;
	}
	cm.reset_lsn().await?;

	streaming.add_stream(&configuration.streaming.stream_name).await;
	streaming.add_topic(&configuration.streaming.stream_name, &configuration.database).await;

	let mut interval = interval(Duration::from_millis(configuration.query_interval));

	// Interrupt signals for UNIX
	let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
    let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();

	loop {
		tokio::select! {
			_ = signal_terminate.recv() => {
				debug!("Received SIGTERM");
				break;
			},
			_ = signal_interrupt.recv() => {
				debug!("Received SIGINT");
				break;
			},
			_ = ctrl_c() => {
				info!("Received shutdown signal!");
				break;
			},
			_ = interval.tick() => { 
				let changes = get_changes(&mut cm).await?;
				if changes.len() == 0 {
					continue;
				}
				info!("There are {} changes that will be commited on the log!", changes.len());

				let queue = changes;
				if queue.len() > 0 {
					match streaming.send_payload(
						&configuration.streaming.stream_name,
						&configuration.database,
						&queue
					).await {
						Ok(_) => debug!("Payload sent to the server"),
						Err(e) => error!("{e}")
					};
				}
			}
		}
	}

	// Shutdown whatever is at the end of the program
	cm.close().await?;

	Ok(())
}


// Poll all new changes
async fn get_changes(cm: &mut ConnectionManager) -> Result<Vec<String>, String> {

	if !cm.has_changes().await? {
		return Ok(vec![]);
	}

	let res = cm.poll_changes().await?;
	Ok(res)
}
