//! Hardware capability discovery and mesh aggregation.
//!
//! `NodeCapabilities::discover()` detects the local machine's CPU, RAM, storage,
//! GPUs, cameras, and thermal sensors. `MeshCapabilities::aggregate()` combines
//! multiple nodes into a unified view of the mesh.

use serde::{Deserialize, Serialize};

/// Hardware capabilities of a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub node_id: String,
    pub hostname: String,
    #[serde(default)]
    pub version: String,
    pub os: String,
    pub arch: String,
    pub cpu_brand: String,
    pub cores_physical: usize,
    pub cores_logical: usize,
    pub ram_bytes: u64,
    pub storage_total_bytes: u64,
    pub storage_free_bytes: u64,
    pub gpus: Vec<GpuInfo>,
    pub cameras: Vec<String>,
    pub sensors: Vec<SensorInfo>,
    pub uptime_secs: u64,
}

/// GPU information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuInfo {
    pub name: String,
    pub vram_bytes: Option<u64>,
}

/// Thermal sensor reading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorInfo {
    pub label: String,
    pub temp_celsius: f32,
}

impl NodeCapabilities {
    /// Detect the capabilities of the local machine.
    pub fn discover(node_id: &str, version: &str) -> Self {
        use sysinfo::System;

        let mut sys = System::new_all();
        sys.refresh_all();

        let hostname = System::host_name().unwrap_or_else(|| "unknown".into());
        let os = System::long_os_version().unwrap_or_else(|| std::env::consts::OS.into());
        let arch = std::env::consts::ARCH.to_string();

        // CPU info
        let cpu_brand = sys
            .cpus()
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "unknown".into());
        let cores_physical = System::physical_core_count().unwrap_or(0);
        let cores_logical = sys.cpus().len();

        // RAM
        let ram_bytes = sys.total_memory();

        // Storage — sum all unique mount points
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let mut total_bytes: u64 = 0;
        let mut free_bytes: u64 = 0;
        let mut seen_devices = std::collections::HashSet::new();
        for disk in disks.list() {
            let device = disk.name().to_string_lossy().to_string();
            if seen_devices.insert(device) {
                total_bytes += disk.total_space();
                free_bytes += disk.available_space();
            }
        }

        // Thermal sensors
        let components = sysinfo::Components::new_with_refreshed_list();
        let sensors: Vec<SensorInfo> = components
            .list()
            .iter()
            .filter(|c| c.temperature().unwrap_or(0.0) > 0.0)
            .map(|c| SensorInfo {
                label: c.label().to_string(),
                temp_celsius: c.temperature().unwrap_or(0.0),
            })
            .collect();

        // GPUs — platform-specific detection
        let gpus = detect_gpus();

        // Cameras — platform-specific detection
        let cameras = detect_cameras();

        // Uptime
        let uptime_secs = System::uptime();

        NodeCapabilities {
            node_id: node_id.to_string(),
            hostname,
            version: version.to_string(),
            os,
            arch,
            cpu_brand,
            cores_physical,
            cores_logical,
            ram_bytes,
            storage_total_bytes: total_bytes,
            storage_free_bytes: free_bytes,
            gpus,
            cameras,
            sensors,
            uptime_secs,
        }
    }

    /// Short platform string like "macos/aarch64".
    pub fn platform(&self) -> String {
        let os_short = if self.os.to_lowercase().contains("mac") {
            "macos"
        } else if self.os.to_lowercase().contains("linux") {
            "linux"
        } else {
            &self.os
        };
        format!("{}/{}", os_short, self.arch)
    }
}

/// Aggregated capabilities across the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshCapabilities {
    pub nodes: Vec<NodeCapabilities>,
    pub responding_count: usize,
    pub peer_count: usize,
    pub total_cores: usize,
    pub total_ram_bytes: u64,
    pub total_storage_bytes: u64,
    pub total_gpus: usize,
}

impl MeshCapabilities {
    /// Aggregate capabilities from multiple nodes.
    pub fn aggregate(nodes: Vec<NodeCapabilities>, peer_count: usize) -> Self {
        let responding_count = nodes.len();
        let total_cores: usize = nodes.iter().map(|n| n.cores_logical).sum();
        let total_ram_bytes: u64 = nodes.iter().map(|n| n.ram_bytes).sum();
        let total_storage_bytes: u64 = nodes.iter().map(|n| n.storage_total_bytes).sum();
        let total_gpus: usize = nodes.iter().map(|n| n.gpus.len()).sum();

        MeshCapabilities {
            nodes,
            responding_count,
            peer_count,
            total_cores,
            total_ram_bytes,
            total_storage_bytes,
            total_gpus,
        }
    }
}

/// Detect GPUs via platform-specific methods.
fn detect_gpus() -> Vec<GpuInfo> {
    let mut gpus = Vec::new();

    #[cfg(target_os = "macos")]
    {
        // Try system_profiler for GPU info
        if let Ok(output) = std::process::Command::new("system_profiler")
            .args(["SPDisplaysDataType", "-json"])
            .output()
        {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(displays) = json.get("SPDisplaysDataType").and_then(|v| v.as_array()) {
                    for display in displays {
                        let name = display
                            .get("sppci_model")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown GPU")
                            .to_string();
                        let vram = display
                            .get("spdisplays_vram")
                            .or_else(|| display.get("spdisplays_vram_shared"))
                            .and_then(|v| v.as_str())
                            .and_then(parse_size_string);
                        gpus.push(GpuInfo {
                            name,
                            vram_bytes: vram,
                        });
                    }
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Check for NVIDIA GPUs via /proc/driver/nvidia/gpus/
        let nvidia_path = std::path::Path::new("/proc/driver/nvidia/gpus");
        if nvidia_path.exists() {
            if let Ok(entries) = std::fs::read_dir(nvidia_path) {
                for entry in entries.flatten() {
                    let info_path = entry.path().join("information");
                    if let Ok(content) = std::fs::read_to_string(&info_path) {
                        let name = content
                            .lines()
                            .find(|l| l.starts_with("Model:"))
                            .map(|l| l.trim_start_matches("Model:").trim().to_string())
                            .unwrap_or_else(|| "NVIDIA GPU".into());
                        gpus.push(GpuInfo {
                            name,
                            vram_bytes: None,
                        });
                    }
                }
            }
        }

        // Check for Jetson (tegra) via /sys/devices/gpu.0
        if gpus.is_empty() {
            let tegra_path = std::path::Path::new("/sys/devices/gpu.0");
            if tegra_path.exists() {
                // Try to get GPU name from device tree
                let name = std::fs::read_to_string("/sys/firmware/devicetree/base/model")
                    .ok()
                    .map(|s| {
                        let trimmed = s.trim_end_matches('\0').trim();
                        format!(
                            "NVIDIA {}",
                            trimmed.split_whitespace().last().unwrap_or("Orin")
                        )
                    })
                    .unwrap_or_else(|| "NVIDIA Tegra GPU".into());
                // Try to read VRAM from iGPU memory info
                let vram = std::fs::read_to_string("/sys/kernel/debug/nvmap/iovmm/size")
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok());
                gpus.push(GpuInfo {
                    name,
                    vram_bytes: vram,
                });
            }
        }
    }

    gpus
}

/// Detect cameras via platform-specific methods.
fn detect_cameras() -> Vec<String> {
    let mut cameras = Vec::new();

    #[cfg(target_os = "linux")]
    {
        // Check /dev/video* devices
        if let Ok(entries) = std::fs::read_dir("/dev") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("video") {
                    cameras.push(format!("/dev/{name}"));
                }
            }
        }
        cameras.sort();
    }

    #[cfg(target_os = "macos")]
    {
        // Try system_profiler for camera info
        if let Ok(output) = std::process::Command::new("system_profiler")
            .args(["SPCameraDataType", "-json"])
            .output()
        {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(cams) = json.get("SPCameraDataType").and_then(|v| v.as_array()) {
                    for cam in cams {
                        if let Some(name) = cam.get("_name").and_then(|v| v.as_str()) {
                            cameras.push(name.to_string());
                        }
                    }
                }
            }
        }
    }

    cameras
}

/// Parse a size string like "8 GB" or "16384 MB" into bytes.
#[allow(dead_code)]
fn parse_size_string(s: &str) -> Option<u64> {
    let s = s.trim();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let num: f64 = parts[0].parse().ok()?;
    let unit = parts[1].to_uppercase();
    match unit.as_str() {
        "GB" => Some((num * 1_073_741_824.0) as u64),
        "MB" => Some((num * 1_048_576.0) as u64),
        "KB" => Some((num * 1024.0) as u64),
        _ => None,
    }
}

/// Format bytes as a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_099_511_627_776 {
        format!("{:.1} TB", bytes as f64 / 1_099_511_627_776.0)
    } else if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    }
}
