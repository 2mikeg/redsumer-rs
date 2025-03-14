use std::sync::{Arc, Mutex};

use redsumer::prelude::*;
use redsumer::pubsub::pubsub::PubSubClient;
use redsumer::redis::{RedisWrite, ToRedisArgs};
use serde::Serialize;
fn main() {
    #[derive(Serialize)]
    struct PubSubMessage {
        pub id: String,
        pub blob_name: String,
    }

    impl ToRedisArgs for PubSubMessage {
        fn write_redis_args<W>(&self, out: &mut W)
        where
            W: ?Sized + RedisWrite,
        {
            let json_string = serde_json::to_string(self).unwrap();

            out.write_arg(json_string.as_bytes());
        }
    }

    #[tokio::main]
    async fn main() {
        let credentials: Option<ClientCredentials> = None;
        let host: &str = "localhost";
        let port: u16 = 6379;
        let db: i64 = 0;
        let channel: &str = "my-pubsub-channel";

        let args: ClientArgs =
            ClientArgs::new(credentials, host, port, db, CommunicationProtocol::RESP2);

        let pubsub = PubSubClient::new(args, channel.to_string()).await.unwrap();
        let pubsub_th = Arc::new(Mutex::new(pubsub));

        loop {
            let m = &pubsub_th
                .lock()
                .unwrap()
                .get_message::<String>()
                .await
                .unwrap();
            match m {
                Some(m) => eprintln!("PubSub Message: {}", m),
                None => {}
            }
        }
    }
}
