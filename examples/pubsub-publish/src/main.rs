use redsumer::prelude::*;
use redsumer::pubsub::pubsub::PubSubClient;
use redsumer::redis::{RedisWrite, ToRedisArgs};
use serde::Serialize;

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
    let pub_sub_message = PubSubMessage {
        id: "id-1".to_string(),
        blob_name: "blob-name".to_string(),
    };

    let _ = pubsub
        .publish::<PubSubMessage>(pub_sub_message)
        .await
        .unwrap();
}
