//! Gossip-based pub/sub channels.

use bytes::Bytes;
use iroh::Endpoint;
use iroh_gossip::Gossip;

use bolo_core::error::BoloError;

/// Create a Gossip protocol handler.
pub fn create_gossip(endpoint: Endpoint) -> Gossip {
    Gossip::builder().spawn(endpoint)
}

/// A pub/sub channel on a gossip topic.
pub struct Channel {
    topic: iroh_gossip::api::GossipTopic,
}

impl Channel {
    /// Join a topic, connecting to the given bootstrap peers.
    pub async fn join(
        gossip: &Gossip,
        topic_id: iroh_gossip::TopicId,
        bootstrap: Vec<iroh::EndpointId>,
    ) -> Result<Self, BoloError> {
        let topic = gossip
            .subscribe_and_join(topic_id, bootstrap)
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to join topic: {e}")))?;
        Ok(Self { topic })
    }

    /// Subscribe to a topic without waiting for peers.
    pub async fn subscribe(
        gossip: &Gossip,
        topic_id: iroh_gossip::TopicId,
        bootstrap: Vec<iroh::EndpointId>,
    ) -> Result<Self, BoloError> {
        let topic = gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to subscribe to topic: {e}")))?;
        Ok(Self { topic })
    }

    /// Broadcast a message to all peers on the topic.
    pub async fn broadcast(&mut self, message: impl Into<Bytes>) -> Result<(), BoloError> {
        self.topic
            .broadcast(message.into())
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to broadcast: {e}")))
    }

    /// Split into sender and receiver halves.
    pub fn split(
        self,
    ) -> (
        iroh_gossip::api::GossipSender,
        iroh_gossip::api::GossipReceiver,
    ) {
        self.topic.split()
    }
}
