//! Relay command handlers.

use anyhow::{bail, Context, Result};
use bolo_core::BoloConfig;

use super::daemon::resolve_config_dir;

/// `bolo relay start` — run relay server (requires relay.serve = true in config).
pub fn start(_config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Relay server not yet available.\n\
         Configure external relays with `bolo relay add <url>` instead."
    );
}

/// `bolo relay stop` — stop relay server.
pub fn stop(_config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!("No relay server is running.");
}

/// `bolo relay status` — show relay server status.
pub fn status(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config_path = config_dir.join("config.toml");
    let config = BoloConfig::load(Some(&config_path))?;

    if json {
        let out = serde_json::json!({
            "serve": config.relay.serve,
            "port": config.relay.port,
            "relay_count": config.relay.urls.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Relay server: {}",
            if config.relay.serve {
                "enabled"
            } else {
                "disabled"
            }
        );
        println!("  Port:   {}", config.relay.port);
        println!("  Relays: {} configured", config.relay.urls.len());
    }

    Ok(())
}

/// `bolo relay ping` — measure latency to configured relays (requires daemon).
pub fn ping(_config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Relay ping requires a running daemon to measure relay latency.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo relay discover` — find community relays.
pub fn discover(_config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Relay discovery is not yet available.\n\
         Add relays manually with `bolo relay add <url>`."
    );
}

/// `bolo relay ls` — list configured relay URLs.
pub fn ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config_path = config_dir.join("config.toml");
    let config = BoloConfig::load(Some(&config_path))?;

    if json {
        let out = serde_json::json!({ "relays": config.relay.urls });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if config.relay.urls.is_empty() {
        println!("No relays configured.");
    } else {
        for url in &config.relay.urls {
            println!("{url}");
        }
    }

    Ok(())
}

/// `bolo relay add <url>` — add a relay URL to config.
pub fn add(url: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config_path = config_dir.join("config.toml");
    let mut config = BoloConfig::load(Some(&config_path))?;

    if config.relay.urls.iter().any(|u| u == url) {
        if json {
            let out = serde_json::json!({ "added": false, "reason": "already configured" });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("Relay already configured: {url}");
        }
        return Ok(());
    }

    config.relay.urls.push(url.to_string());
    config
        .save(Some(&config_path))
        .context("failed to save config")?;

    if json {
        let out = serde_json::json!({ "added": true, "url": url });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added relay: {url}");
    }

    Ok(())
}

/// `bolo relay rm <url>` — remove a relay URL from config.
pub fn rm(url: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config_path = config_dir.join("config.toml");
    let mut config = BoloConfig::load(Some(&config_path))?;

    let before = config.relay.urls.len();
    config.relay.urls.retain(|u| u != url);
    let removed = config.relay.urls.len() < before;

    if removed {
        config
            .save(Some(&config_path))
            .context("failed to save config")?;
    }

    if json {
        let out = serde_json::json!({ "removed": removed, "url": url });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if removed {
        println!("Removed relay: {url}");
    } else {
        println!("Relay not found: {url}");
    }

    Ok(())
}
