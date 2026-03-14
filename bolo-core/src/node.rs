//! Bolo node lifecycle — wraps iroh Endpoint + protocol Router.

use iroh::address_lookup::MdnsAddressLookup;
use iroh_blobs::BlobsProtocol;
use iroh_gossip::net::GOSSIP_ALPN;
use iroh_gossip::Gossip;

use crate::error::BoloError;

/// A running bolo node with iroh networking and registered protocols.
pub struct BoloNode {
    router: iroh::protocol::Router,
    endpoint: iroh::Endpoint,
    gossip: Gossip,
    _mdns: MdnsAddressLookup,
}

impl BoloNode {
    /// Spawn a new node with the given identity, blob protocol, and gossip.
    ///
    /// Enables mDNS for automatic LAN peer discovery.
    pub async fn spawn(
        secret_key: iroh::SecretKey,
        blobs: BlobsProtocol,
    ) -> Result<Self, BoloError> {
        let endpoint = iroh::Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![iroh_blobs::ALPN.to_vec(), GOSSIP_ALPN.to_vec()])
            .bind()
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to bind endpoint: {e}")))?;

        // Enable mDNS for LAN auto-discovery
        let mdns = MdnsAddressLookup::builder()
            .build(endpoint.id())
            .map_err(|e| BoloError::ConfigError(format!("failed to start mDNS: {e}")))?;
        endpoint.address_lookup().add(mdns.clone());

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();

        Ok(Self {
            router,
            endpoint,
            gossip,
            _mdns: mdns,
        })
    }

    /// Access the underlying iroh endpoint.
    pub fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    /// Access the gossip protocol handler.
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    /// Gracefully shut down the node.
    pub async fn shutdown(self) -> Result<(), BoloError> {
        self.router
            .shutdown()
            .await
            .map_err(|e| BoloError::ConfigError(format!("shutdown error: {e}")))?;
        self.endpoint.close().await;
        Ok(())
    }
}
