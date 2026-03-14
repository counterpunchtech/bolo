#![deny(unsafe_code)]

//! Relay server operations for bolo.

#[cfg(test)]
mod tests {
    /// Smoke test: verify that the iroh-relay dependency is accessible and that
    /// core relay types can be named / constructed without binding a port.
    #[test]
    fn iroh_relay_types_accessible() {
        // `iroh_relay::RelayMap` is a core public type; confirm we can construct
        // an empty one, proving the dependency links correctly.
        let map = iroh_relay::RelayMap::empty();
        assert!(map.is_empty());
    }

    /// Verify that `RelayMap` default-constructs as empty and reports len 0.
    #[test]
    fn relay_map_default_is_empty() {
        let map = iroh_relay::RelayMap::empty();
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
    }
}
