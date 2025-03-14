use futures::StreamExt;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{AsyncCommands, FromRedisValue, ToRedisArgs};
use redsumer_core::client::{ClientArgs, RedisClientBuilder};
use redsumer_core::result::RedsumerResult;
use tracing::{debug, info};

/// A Redis Pub/Sub client
pub struct PubSubClient {
    /// The Redis Pub/Sub client instance.
    pubsub: PubSub,

    /// The Pub/Sub channel name.
    ps_channel: String,

    /// A multiplexed connection to Redis.
    con: MultiplexedConnection,
}

impl PubSubClient {
    /// Creates a new [`PubSubClient`] instance.
    ///
    /// # Arguments:
    /// - `args`: Redis client arguments.
    /// - `chann`: The channel name to subscribe to.
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

        Ok(Self {
            pubsub,
            con,
            ps_channel: chann,
        })
    }

    /// Returns the name of the subscribed channel.
    fn get_channel(&self) -> &str {
        &self.ps_channel
    }

    /// Retrieves a message from Redis Pub/Sub.
    ///
    /// # Type Parameters:
    /// - `T`: The expected message type, which must implement `FromRedisValue`.
    ///
    /// # Returns:
    /// - `Ok(Some(T))` if a message is received and successfully parsed.
    /// - `Ok(None)` if no message is available.
    /// - `Err(RedsumerResult)` in case of an error.
    pub async fn get_message<T: FromRedisValue>(&mut self) -> RedsumerResult<Option<T>> {
        let mut stream = self.pubsub.on_message();
        if let Some(msg) = stream.next().await {
            let msg = msg.get_payload::<T>()?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Publishes a message to the Pub/Sub channel.
    ///
    /// # Type Parameters:
    /// - `T`: The message type, which must implement `ToRedisArgs`, `Send`, and `Sync`.
    ///
    /// # Arguments:
    /// - `msg`: The message to publish.
    ///
    /// # Returns:
    /// - `Ok(())` if the message is successfully published.
    /// - `Err(RedsumerResult)` in case of an error.
    pub async fn publish<T: ToRedisArgs + Send + Sync>(&self, msg: T) -> RedsumerResult<()> {
        let ch = &self.get_channel();
        let mut conn = self.con.clone();

        let listener = conn
            .publish::<String, T, isize>(ch.to_string(), msg)
            .await?
            .clone();

        debug!(
            "Published message to channel: {} with {} listener(s)",
            ch, listener
        );
        Ok(())
    }
}
