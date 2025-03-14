use std::sync::Arc;
use tokio::sync::Mutex;

use redsumer::prelude::*;
use redsumer::pubsub::pubsub::PubSubClient;
use tokio::task;

#[tokio::main]
async fn main() {
    let credentials: Option<ClientCredentials> = None;
    let host: &str = "localhost";
    let port: u16 = 6379;
    let db: i64 = 0;
    let channel: &str = "my-pubsub-channel";

    let args: ClientArgs =
        ClientArgs::new(credentials, host, port, db, CommunicationProtocol::RESP2);

    let pubsub = Arc::new(Mutex::new(
        PubSubClient::new(args, channel.to_string()).await.unwrap(),
    ));

    let pubsub_clone = Arc::clone(&pubsub);

    task::spawn(async move {
        loop {
            let mut pubsub_lock = pubsub_clone.lock().await;
            if let Some(msg) = pubsub_lock.get_message::<String>().await.unwrap() {
                println!("Recibido: {:?}", msg);
            }
        }
    })
    .await
    .unwrap();
}
