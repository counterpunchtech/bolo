//! Mesh command handlers — unified mesh capability discovery.

use anyhow::{Context, Result};
use bolo_core::capabilities::{format_bytes, MeshCapabilities};

use super::daemon::resolve_config_dir;

/// `bolo mesh status` — show aggregate mesh capabilities.
pub async fn status(timeout_secs: u64, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot query mesh status: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    if !json {
        println!(
            "Discovering mesh capabilities ({}s timeout)...",
            timeout_secs
        );
    }

    let result = client
        .call(
            "mesh.status",
            serde_json::json!({ "timeout_secs": timeout_secs }),
        )
        .await
        .context("mesh.status failed")?;

    let mesh: MeshCapabilities =
        serde_json::from_value(result).context("failed to parse mesh capabilities")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&mesh)?);
    } else {
        print_mesh_status(&mesh);
    }

    Ok(())
}

fn print_mesh_status(mesh: &MeshCapabilities) {
    println!(
        "\nMesh Status ({}/{} nodes responding)",
        mesh.responding_count, mesh.peer_count
    );
    println!("{}", "\u{2550}".repeat(34));

    for node in &mesh.nodes {
        println!();
        println!("  {} ({})", node.hostname, node.platform());
        if !node.version.is_empty() {
            println!("    Version: {}", node.version);
        }
        println!(
            "    CPU:     {} ({} cores)",
            node.cpu_brand, node.cores_logical
        );
        println!("    RAM:     {}", format_bytes(node.ram_bytes));
        println!(
            "    Storage: {} ({} free)",
            format_bytes(node.storage_total_bytes),
            format_bytes(node.storage_free_bytes)
        );
        for gpu in &node.gpus {
            if let Some(vram) = gpu.vram_bytes {
                println!("    GPU:     {} ({})", gpu.name, format_bytes(vram));
            } else {
                println!("    GPU:     {}", gpu.name);
            }
        }
        for cam in &node.cameras {
            println!("    Cameras: {cam}");
        }
        for sensor in &node.sensors {
            println!(
                "    Sensors: {} {:.1}\u{00B0}C",
                sensor.label, sensor.temp_celsius
            );
        }
    }

    println!();
    println!("Totals:");
    println!("  Cores:   {}", mesh.total_cores);
    println!("  RAM:     {}", format_bytes(mesh.total_ram_bytes));
    println!("  Storage: {}", format_bytes(mesh.total_storage_bytes));
    if mesh.total_gpus > 0 {
        println!("  GPUs:    {}", mesh.total_gpus);
    }
}
