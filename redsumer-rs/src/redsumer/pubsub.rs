use futures::StreamExt;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{AsyncCommands, Client, FromRedisValue, ToRedisArgs};
use redsumer_core::client::{ClientArgs, RedisClientBuilder};
use redsumer_core::result::RedsumerResult;
use tracing::{debug, info};

pub struct PubSubClient {
    /// Redis client.
    client: Client,

    /// Redis pubsub client.
    pubsub: PubSub,

    /// PubSub channel
    ps_channel: String,

    /// Multiplexed connection.
    con: MultiplexedConnection,
}

impl PubSubClient {
    /// Create a new [`PubSubClient`] instance.
    ///
    /// # Arguments:
    /// - **args**: Redis client args.
    /// - **chann**: Channel to subscribe to.
    ///
    /// # Returns:
    /// A new [`PubSubClient`] instance.
    pub async fn new(args: ClientArgs, chann: String) -> RedsumerResult<Self> {
        info!("Creating a new Pub/Sub instance on {}.", chann);

        let client = args.build()?;

        let mut pubsub = client.get_async_pubsub().await?;
        let con = client.get_multiplexed_tokio_connection().await?;

        let _ = pubsub.subscribe(chann.clone()).await?;
        debug!("Subscribed to channel: {}", chann);

        Ok(PubSubClient {
            client,
            pubsub,
            con,
            ps_channel: chann,
        })
    }

    fn get_client(&self) -> &Client {
        &self.client
    }

    fn get_channel(&self) -> &str {
        &self.ps_channel
    }

    pub async fn get_message<T: FromRedisValue>(self) -> RedsumerResult<Option<T>> {
        let mut stream = self.pubsub.into_on_message();
        let next = stream.next().await;

        match next {
            Some(msg) => {
                let msg = msg.get_payload::<T>()?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    pub async fn publish<T: ToRedisArgs + Send + Sync>(&self, msg: T) -> RedsumerResult<()> {
        let ch = &self.get_channel();
        let mut conn = self.con.clone();

        let listener = conn
            .publish::<String, T, i64>(ch.to_string(), msg)
            .await?
            .clone();

        debug!(
            "Published message to channel: {} with {} listener",
            ch,
            listener.clone()
        );
        Ok(())
    }
}
