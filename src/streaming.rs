extern crate log;
use log::{info, warn, error, debug};

use std::str::FromStr;

use serde::{Serialize, Deserialize};

use iggy::prelude::*;


fn max_retry() -> u32 { 3 }


#[derive(Debug, Serialize, Deserialize)]
pub struct StreamingConfig {
	host: String,
	port: u64,
	user: String,
	password: String,
    #[serde(default = "max_retry")]
	max_retry: u32
}


pub struct StreamingClient {
	client: IggyClient
}


impl StreamingConfig {
	pub fn get_tcp_server_addr(&self) -> String {
		format!("{}:{}", self.host, self.port)
	}
}


impl StreamingClient {

	pub async fn new(config: &StreamingConfig) -> Result<StreamingClient, String> {

		let client = match IggyClientBuilder::new()
			.with_tcp()
			.with_server_address(config.get_tcp_server_addr())
			.with_reconnection_max_retries(Some(config.max_retry))
			.build() {
				Ok(c) => c,
				Err(e) => { 
					error!("{}", e);
					return Err(format!("{}", e));
				}
			};

		if client.connect().await.is_ok() { } else {
			error!("Error, client could not connect to the server!");
			return Err("Cannot connect to server!".to_string());
		}

		debug!("Successful connection to the server");

		if client
			.login_user(&config.user, &config.password)
			.await.is_ok() { } else {
				error!("Error, the login could not succeed");
				return Err("Cannot login".to_string());
			};

		debug!("Successful login");

		Ok(StreamingClient {
			client
		})
	}

	pub async fn add_stream(&mut self, stream: &str) {

		match self.client.create_stream(&stream).await {
			Ok(_) => info!("Stream {} created", stream),
			Err(e) => warn!("Stream {stream} already exists and will not be created again.\n{e}"),
		};
	}

	pub async fn add_topic(&self, stream: &str, topic: &str) {

		match self.client
			.create_topic(
				&Identifier::named(stream).unwrap(),
				topic,
				1,
				CompressionAlgorithm::default(),
				None.into(),
				IggyExpiry::NeverExpire,
				MaxTopicSize::ServerDefault
			)
			.await
		{
			Ok(_) => info!("Topic {} for stream {} added", topic, stream),
			Err(_) => warn!("Topic {} already exists for stream {} and will not be created again.", topic, stream),
		};
	}

	pub async fn send_payload(
		&mut self,
		stream: &String,
		topic: &String,
		payload: &Vec<String>
	) -> Result<(), String> {

		let mut messages = Vec::new();
		for msg in payload {
			let message = IggyMessage::from_str(&msg).unwrap();
			messages.push(message);
		}
		debug!("Sending payload {:?}", payload);
		match self.client
			.send_messages(
				&Identifier::named(stream).unwrap(),
				&Identifier::named(topic).unwrap(),
				&Partitioning::partition_id(0),
				&mut messages
			).await {
				Ok(_) => Ok(()),
				Err(e) => Err(format!("{}", e))
			}
	}
}
