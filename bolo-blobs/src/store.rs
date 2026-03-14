//! Blob store operations wrapping iroh-blobs FsStore.

use std::path::Path;

use bolo_core::error::BoloError;
use iroh_blobs::store::fs::FsStore;

/// Open (or create) the blob store in the given data directory.
pub async fn open_store(data_dir: &Path) -> Result<FsStore, BoloError> {
    let blobs_dir = data_dir.join("blobs");
    std::fs::create_dir_all(&blobs_dir)?;
    FsStore::load(&blobs_dir)
        .await
        .map_err(|e| BoloError::ConfigError(format!("failed to open blob store: {e}")))
}
