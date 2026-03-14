//! Integration test: two nodes transfer a blob over the network.
//!
//! This is the Phase 0a MVP acceptance test:
//! Node A puts a blob -> Node B fetches it -> bytes match.

use iroh::address_lookup::MemoryLookup;
use iroh::protocol::Router;
use iroh::{Endpoint, RelayMode};
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::BlobsProtocol;

/// Spawn a node with in-memory store and address lookup.
async fn spawn_node() -> (Router, Endpoint, MemStore, MemoryLookup) {
    let lookup = MemoryLookup::new();
    let endpoint = Endpoint::empty_builder(RelayMode::Default)
        .address_lookup(lookup.clone())
        .alpns(vec![iroh_blobs::ALPN.to_vec()])
        .bind()
        .await
        .expect("failed to bind endpoint");

    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, None);

    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    (router, endpoint, store, lookup)
}

#[tokio::test]
async fn two_node_blob_transfer() {
    // Spawn two nodes
    let (router_a, endpoint_a, store_a, _lookup_a) = spawn_node().await;
    let (_router_b, endpoint_b, store_b, lookup_b) = spawn_node().await;

    // Tell node B how to reach node A
    lookup_b.add_endpoint_info(endpoint_a.addr());

    // Node A stores a blob
    let data = b"Hello from the bolo mesh! This is the Phase 0a MVP test.";
    let tag = store_a
        .blobs()
        .add_bytes(data.to_vec())
        .await
        .expect("failed to add blob");

    let blob_hash = tag.hash;

    // Verify Node A has it
    let bytes_a = store_a
        .get_bytes(blob_hash)
        .await
        .expect("node A should have the blob");
    assert_eq!(bytes_a.as_ref(), data.as_slice());

    // Verify Node B does NOT have it yet
    assert!(store_b.get_bytes(blob_hash).await.is_err());

    // Node B downloads the blob from Node A
    let downloader = store_b.downloader(&endpoint_b);
    downloader
        .download(blob_hash, vec![endpoint_a.id()])
        .await
        .expect("failed to download blob from node A");

    // Verify Node B now has the exact same bytes
    let bytes_b = store_b
        .get_bytes(blob_hash)
        .await
        .expect("node B should have the blob after download");
    assert_eq!(bytes_b.as_ref(), data.as_slice());
    assert_eq!(bytes_a, bytes_b);

    // Clean shutdown
    router_a.shutdown().await.ok();
}

#[tokio::test]
async fn transfer_larger_blob() {
    let (router_a, endpoint_a, store_a, _lookup_a) = spawn_node().await;
    let (_router_b, endpoint_b, store_b, lookup_b) = spawn_node().await;

    // Tell node B how to reach node A
    lookup_b.add_endpoint_info(endpoint_a.addr());

    // Create a 100KB blob (multiple chunks)
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let tag = store_a
        .blobs()
        .add_bytes(data.clone())
        .await
        .expect("failed to add large blob");

    let blob_hash = tag.hash;

    // Node B downloads
    let downloader = store_b.downloader(&endpoint_b);
    downloader
        .download(blob_hash, vec![endpoint_a.id()])
        .await
        .expect("failed to download large blob");

    let bytes_b = store_b
        .get_bytes(blob_hash)
        .await
        .expect("node B should have the large blob");
    assert_eq!(bytes_b.len(), data.len());
    assert_eq!(bytes_b.as_ref(), data.as_slice());

    router_a.shutdown().await.ok();
}
