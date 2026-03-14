//! Integration test: 3-node concurrent CRDT document editing via gossip.
//!
//! Phase 1 acceptance test:
//! Three nodes subscribe to the same document topic. Each edits the document.
//! All three converge to the same content after syncing updates via gossip.

use bolo_docs::sync::{doc_topic_id, DocSyncMessage};
use bolo_docs::DocStore;
use futures_lite::StreamExt;
use iroh::address_lookup::MemoryLookup;
use iroh::protocol::Router;
use iroh::{Endpoint, RelayMode};
use iroh_gossip::net::GOSSIP_ALPN;
use iroh_gossip::Gossip;
use loro::LoroDoc;

struct TestNode {
    _router: Router,
    endpoint: Endpoint,
    gossip: Gossip,
    store: DocStore,
    node_id: String,
    lookup: MemoryLookup,
}

/// Spawn a test node with gossip and a temp doc store.
async fn spawn_node(tmp_dir: &std::path::Path) -> TestNode {
    let lookup = MemoryLookup::new();
    let endpoint = Endpoint::builder()
        .relay_mode(RelayMode::Disabled)
        .address_lookup(lookup.clone())
        .alpns(vec![GOSSIP_ALPN.to_vec()])
        .bind()
        .await
        .expect("failed to bind endpoint");

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let store = DocStore::open(tmp_dir).expect("failed to open store");
    let node_id = endpoint.id().to_string();

    TestNode {
        _router: router,
        endpoint,
        gossip,
        store,
        node_id,
        lookup,
    }
}

/// Connect two nodes so they can discover each other.
fn connect_nodes(a: &TestNode, b: &TestNode) {
    b.lookup.add_endpoint_info(a.endpoint.addr());
    a.lookup.add_endpoint_info(b.endpoint.addr());
}

/// Receive one doc sync message within a timeout, applying it to the store.
async fn recv_and_apply(receiver: &mut iroh_gossip::api::GossipReceiver, store: &DocStore) -> bool {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match tokio::time::timeout_at(deadline, receiver.try_next()).await {
            Ok(Ok(Some(iroh_gossip::api::Event::Received(msg)))) => {
                let sync_msg = DocSyncMessage::from_bytes(&msg.content).unwrap();
                bolo_docs::apply_sync_message(store, &sync_msg).unwrap();
                return true;
            }
            Ok(Ok(Some(_))) => continue, // NeighborUp/Down/Lagged — keep waiting
            Ok(Ok(None)) => return false, // stream ended
            Ok(Err(e)) => panic!("gossip error: {e}"),
            Err(_) => return false, // timeout
        }
    }
}

/// Three nodes sync a document over gossip.
///
/// Run separately with: `cargo test -p bolo-docs --test sync -- three_node_doc_sync`
/// Ignored in workspace-wide test runs due to port conflicts with blob transfer tests.
#[tokio::test]
#[ignore]
async fn three_node_doc_sync() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let tmp_c = tempfile::tempdir().unwrap();

    let node_a = spawn_node(tmp_a.path()).await;
    let node_b = spawn_node(tmp_b.path()).await;
    let node_c = spawn_node(tmp_c.path()).await;

    // All nodes can discover each other
    connect_nodes(&node_a, &node_b);
    connect_nodes(&node_a, &node_c);
    connect_nodes(&node_b, &node_c);

    let doc_path = "specs/test-doc";
    let topic_id = doc_topic_id(doc_path);

    // Node A creates the document
    let doc_a = node_a.store.create(doc_path).unwrap();
    let text_a = doc_a.get_text("content");
    text_a.insert(0, "Hello from A\n").unwrap();
    doc_a.commit();
    node_a.store.save(doc_path, &doc_a).unwrap();

    // All three join the topic using subscribe_and_join with timeout.
    // Launch all three joins concurrently so they can find each other.
    let (topic_a, topic_b, topic_c) = tokio::join!(
        async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                node_a
                    .gossip
                    .subscribe_and_join(topic_id, vec![node_b.endpoint.id()]),
            )
            .await
            .expect("Node A join timeout")
            .expect("Node A join failed")
        },
        async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                node_b
                    .gossip
                    .subscribe_and_join(topic_id, vec![node_a.endpoint.id(), node_c.endpoint.id()]),
            )
            .await
            .expect("Node B join timeout")
            .expect("Node B join failed")
        },
        async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                node_c
                    .gossip
                    .subscribe_and_join(topic_id, vec![node_b.endpoint.id()]),
            )
            .await
            .expect("Node C join timeout")
            .expect("Node C join failed")
        },
    );

    let (sender_a, mut receiver_a) = topic_a.split();
    let (sender_b, mut receiver_b) = topic_b.split();
    let (_sender_c, mut receiver_c) = topic_c.split();

    // Brief pause for mesh to stabilize
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Node A broadcasts its initial content as a snapshot
    let snapshot_bytes = doc_a.export(loro::ExportMode::Snapshot).unwrap();
    let msg = DocSyncMessage::Snapshot {
        path: doc_path.to_string(),
        data: snapshot_bytes,
        author: node_a.node_id.clone(),
        timestamp: 1000,
        nonce: 0,
    };
    sender_a
        .broadcast(msg.to_bytes().unwrap().into())
        .await
        .unwrap();

    // Node B and C receive and apply the snapshot
    assert!(
        recv_and_apply(&mut receiver_b, &node_b.store).await,
        "Node B should receive snapshot"
    );
    assert!(
        recv_and_apply(&mut receiver_c, &node_c.store).await,
        "Node C should receive snapshot"
    );

    // Verify B and C have the initial content
    let doc_b = node_b.store.load(doc_path).unwrap();
    assert_eq!(doc_b.get_text("content").to_string(), "Hello from A\n");

    let doc_c = node_c.store.load(doc_path).unwrap();
    assert_eq!(doc_c.get_text("content").to_string(), "Hello from A\n");

    // Node B makes an edit and broadcasts it as a snapshot
    let text_b = doc_b.get_text("content");
    text_b
        .insert(text_b.len_unicode(), "Hello from B\n")
        .unwrap();
    doc_b.commit();
    node_b.store.save(doc_path, &doc_b).unwrap();

    let snapshot_bytes = doc_b.export(loro::ExportMode::Snapshot).unwrap();
    let msg = DocSyncMessage::Snapshot {
        path: doc_path.to_string(),
        data: snapshot_bytes,
        author: node_b.node_id.clone(),
        timestamp: 2000,
        nonce: 0,
    };
    sender_b
        .broadcast(msg.to_bytes().unwrap().into())
        .await
        .unwrap();

    // Node A and C receive B's update
    assert!(
        recv_and_apply(&mut receiver_a, &node_a.store).await,
        "Node A should receive B's update"
    );
    assert!(
        recv_and_apply(&mut receiver_c, &node_c.store).await,
        "Node C should receive B's update"
    );

    // Verify all three nodes have converged
    let final_a = node_a.store.load(doc_path).unwrap();
    let final_b = node_b.store.load(doc_path).unwrap();
    let final_c = node_c.store.load(doc_path).unwrap();

    let content_a = final_a.get_text("content").to_string();
    let content_b = final_b.get_text("content").to_string();
    let content_c = final_c.get_text("content").to_string();

    assert_eq!(content_a, content_b, "A and B should converge");
    assert_eq!(content_b, content_c, "B and C should converge");
    assert!(
        content_a.contains("Hello from A"),
        "Should contain A's text"
    );
    assert!(
        content_a.contains("Hello from B"),
        "Should contain B's text"
    );
}

#[tokio::test]
async fn concurrent_edits_converge() {
    // Two nodes edit simultaneously, verify CRDT merge without gossip
    let doc_a = LoroDoc::new();
    let doc_b = LoroDoc::new();

    // Both nodes make edits to the same text container
    let text_a = doc_a.get_text("content");
    text_a.insert(0, "AAA").unwrap();
    doc_a.commit();

    let text_b = doc_b.get_text("content");
    text_b.insert(0, "BBB").unwrap();
    doc_b.commit();

    // Export both as snapshots
    let snap_a = doc_a.export(loro::ExportMode::Snapshot).unwrap();
    let snap_b = doc_b.export(loro::ExportMode::Snapshot).unwrap();

    // Cross-import: A imports B's snapshot, B imports A's snapshot
    doc_a.import(&snap_b).unwrap();
    doc_a.commit();

    doc_b.import(&snap_a).unwrap();
    doc_b.commit();

    // Both should have the same content (CRDT merge)
    let content_a = doc_a.get_text("content").to_string();
    let content_b = doc_b.get_text("content").to_string();

    assert_eq!(
        content_a, content_b,
        "Concurrent edits should converge to same content"
    );
    assert!(content_a.contains("AAA"), "Should contain A's edit");
    assert!(content_a.contains("BBB"), "Should contain B's edit");
}
