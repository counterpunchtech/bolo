//! Identity command handlers.

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use bolo_core::identity::{hex_decode, hex_encode};
use bolo_core::{BoloConfig, Identity};

use super::daemon::resolve_config_dir;

/// Load the identity from the config directory.
fn load_identity(config_flag: Option<&str>) -> Result<(Identity, PathBuf)> {
    let config_dir = resolve_config_dir(config_flag)?;
    let identity = Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;
    Ok((identity, config_dir))
}

/// `bolo id show` — print node ID.
pub fn show(config_flag: Option<&str>, json: bool) -> Result<()> {
    let (identity, _) = load_identity(config_flag)?;
    let node_id = identity.node_id().to_string();

    if json {
        let out = serde_json::json!({ "node_id": node_id });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("{node_id}");
    }
    Ok(())
}

/// `bolo id export` — print secret key to stdout.
pub fn export(config_flag: Option<&str>, json: bool) -> Result<()> {
    let (identity, _) = load_identity(config_flag)?;
    let secret_hex = hex_encode(&identity.secret_key().to_bytes());
    let node_id = identity.node_id().to_string();

    if json {
        let out = serde_json::json!({
            "node_id": node_id,
            "secret_key": secret_hex,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        eprintln!("# Node ID: {node_id}");
        eprintln!("# Keep this secret key safe — it IS your identity.");
        println!("{secret_hex}");
    }
    Ok(())
}

/// `bolo id import <file>` — import keypair from file.
pub fn import(file: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config_path = config_dir.join("config.toml");
    let config = if config_path.exists() {
        BoloConfig::load(Some(&config_path))?
    } else {
        BoloConfig::default()
    };

    let key_path = config_dir.join(&config.identity.key_file);
    if key_path.exists() {
        bail!(
            "Identity already exists at {}. Remove it first to import.",
            key_path.display()
        );
    }

    // Load the identity from the provided file to validate it
    let source = std::path::Path::new(file);
    let identity =
        Identity::load(source).context("failed to load identity from the provided file")?;

    // Save to the config directory
    std::fs::create_dir_all(&config_dir)?;
    identity.save(&key_path)?;

    if !config_path.exists() {
        config.save(Some(&config_path))?;
    }

    let node_id = identity.node_id().to_string();

    if json {
        let out = serde_json::json!({
            "node_id": node_id,
            "imported_from": file,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Imported identity: {node_id}");
    }
    Ok(())
}

/// `bolo id sign <data>` — sign arbitrary data.
pub fn sign(data: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let (identity, _) = load_identity(config_flag)?;
    let signature = identity.sign(data.as_bytes());
    let sig_hex = hex_encode(&signature.to_bytes());

    if json {
        let out = serde_json::json!({
            "data": data,
            "signature": sig_hex,
            "node_id": identity.node_id().to_string(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("{sig_hex}");
    }
    Ok(())
}

/// `bolo id verify <data> <sig> <peer>` — verify a signature.
pub fn verify(data: &str, sig_hex: &str, peer: &str, json: bool) -> Result<()> {
    let sig_bytes = hex_decode(sig_hex).context("invalid signature hex")?;
    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("signature must be exactly 64 bytes (128 hex chars)"))?;
    let signature = iroh::Signature::from_bytes(&sig_array);

    let public_key = iroh::PublicKey::from_str(peer).context("invalid peer node ID")?;

    let valid = Identity::verify(&public_key, data.as_bytes(), &signature).is_ok();

    if json {
        let out = serde_json::json!({
            "valid": valid,
            "data": data,
            "peer": peer,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if valid {
        println!("Signature is valid.");
    } else {
        bail!("Signature verification failed.");
    }
    Ok(())
}
