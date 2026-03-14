#![deny(unsafe_code)]

//! Pub/sub messaging operations for bolo.

pub mod channel;

pub use bolo_core::{BoloError, TopicId};
pub use channel::{create_gossip, Channel};
pub use iroh_gossip;
pub use iroh_gossip::Gossip;

#[cfg(test)]
mod tests {
    use super::*;

    // ── TopicId derivation ──────────────────────────────────────────

    #[test]
    fn topic_id_deterministic() {
        let a = TopicId::from_name("my-topic");
        let b = TopicId::from_name("my-topic");
        assert_eq!(a, b, "same name must produce identical TopicId");
    }

    #[test]
    fn topic_id_different_names_differ() {
        let a = TopicId::from_name("alpha");
        let b = TopicId::from_name("beta");
        assert_ne!(a, b, "different names must produce different TopicIds");
    }

    #[test]
    fn topic_id_empty_name_is_valid() {
        // Empty string is still a valid blake3 input; just verify no panic.
        let t = TopicId::from_name("");
        assert_eq!(t.0.len(), 32);
    }

    #[test]
    fn topic_id_display_is_hex() {
        let t = TopicId::from_name("display-test");
        let display = format!("{t}");
        assert_eq!(display.len(), 64, "hex-encoded 32 bytes = 64 chars");
        assert!(
            display.chars().all(|c| c.is_ascii_hexdigit()),
            "display must be lowercase hex"
        );
    }

    // ── TopicId serialization roundtrip ─────────────────────────────

    #[test]
    fn topic_id_serde_roundtrip() {
        let original = TopicId::from_name("serde-test");
        let json = serde_json::to_string(&original).expect("serialize");
        let restored: TopicId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    // ── BoloError re-export ─────────────────────────────────────────

    #[test]
    fn bolo_error_reexported() {
        // Verify the re-export compiles and the variant is accessible.
        let err = BoloError::ConfigError("test".into());
        let msg = format!("{err}");
        assert!(msg.contains("test"));
    }

    // ── Public API surface smoke test ───────────────────────────────

    #[test]
    fn public_api_types_accessible() {
        // Ensure key re-exports resolve at the bolo_pub level.
        let _topic: TopicId = TopicId::from_name("smoke");
        fn _assert_create_gossip_exists(_e: iroh::Endpoint) {
            let _g: Gossip = create_gossip(_e);
        }
    }
}
