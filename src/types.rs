use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ConsoleType {
    Java,
    Html5,
    Sol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvmCookie {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvmConsoleInfo {
    pub console_type: ConsoleType,
    pub jnlp_content: Option<String>,
    pub html5_path: Option<String>,
    pub cookies: Vec<KvmCookie>,
    pub csrf_header: Option<String>,
    /// BMC viewer 需要的 sessionStorage 键值对（features、token JSON、privilege 等）
    #[serde(default)]
    pub session_storage: Vec<(String, String)>,
    /// BMC viewer 需要的 localStorage 键值对（华为 iBMC H5 KVM 需要）
    #[serde(default)]
    pub local_storage: Vec<(String, String)>,
    /// 额外需要代理的 BMC 端口（TCP 代理或 WS 端口映射）
    #[serde(default)]
    pub bmc_extra_ports: Vec<(u16, String)>,
}

#[derive(Debug, Clone)]
pub struct BmcCreds {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
    pub base_path: String,
}

impl BmcCreds {
    pub fn base_url(&self) -> String {
        let scheme = if self.use_tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    pub fn redfish_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url(), path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub bios_version: Option<String>,
    #[serde(default)]
    pub bmc_version: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub power_state: Option<String>,
    #[serde(default)]
    pub total_cpu_count: Option<u32>,
    #[serde(default)]
    pub total_memory_gib: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorInfo {
    pub id: String,
    #[serde(default)]
    pub socket: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub total_cores: Option<u32>,
    #[serde(default)]
    pub total_threads: Option<u32>,
    #[serde(default)]
    pub max_speed_mhz: Option<u32>,
    #[serde(default)]
    pub temperature_celsius: Option<f64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub architecture: Option<String>,
    #[serde(default)]
    pub frequency_mhz: Option<u32>,
    #[serde(default)]
    pub l1_cache_kib: Option<u32>,
    #[serde(default)]
    pub l2_cache_kib: Option<u32>,
    #[serde(default)]
    pub l3_cache_kib: Option<u32>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub part_number: Option<String>,
    #[serde(default)]
    pub instruction_set: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    pub id: String,
    #[serde(default)]
    pub capacity_gib: Option<f64>,
    #[serde(default)]
    pub memory_type: Option<String>,
    #[serde(default)]
    pub speed_mhz: Option<u32>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub slot_index: Option<u32>,
    #[serde(default)]
    pub temperature_celsius: Option<f64>,
    #[serde(default = "default_true")]
    pub populated: bool,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub part_number: Option<String>,
    #[serde(default)]
    pub rank_count: Option<u32>,
    #[serde(default)]
    pub module_type: Option<String>,
    #[serde(default)]
    pub data_width_bits: Option<u32>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub capacity_gib: Option<f64>,
    #[serde(default)]
    pub media_type: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub firmware_version: Option<String>,
    #[serde(default)]
    pub rotation_speed_rpm: Option<u32>,
    #[serde(default)]
    pub capable_speed_gbps: Option<f64>,
    #[serde(default)]
    pub negotiated_speed_gbps: Option<f64>,
    #[serde(default)]
    pub failure_predicted: Option<bool>,
    #[serde(default)]
    pub predicted_media_life_left_percent: Option<u32>,
    #[serde(default)]
    pub hotspare_type: Option<String>,
    #[serde(default)]
    pub temperature_celsius: Option<f64>,
    #[serde(default)]
    pub hours_powered_on: Option<f64>,
    #[serde(default)]
    pub slot_number: Option<u32>,
    #[serde(default)]
    pub form_factor: Option<String>,
    #[serde(default)]
    pub firmware_status: Option<String>,
    #[serde(default)]
    pub raid_level: Option<String>,
    #[serde(default)]
    pub controller_name: Option<String>,
    #[serde(default)]
    pub rebuild_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageControllerInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub firmware_version: Option<String>,
    #[serde(default)]
    pub speed_gbps: Option<f64>,
    #[serde(default)]
    pub supported_raid_types: Vec<String>,
    #[serde(default)]
    pub cache_size_mib: Option<u32>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub drive_count: Option<u32>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterfaceInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mac_address: Option<String>,
    #[serde(default)]
    pub speed_mbps: Option<u32>,
    #[serde(default)]
    pub speed_gbps: Option<f64>,
    #[serde(default)]
    pub port_max_speed: Option<String>,
    #[serde(default)]
    pub link_status: Option<String>,
    #[serde(default)]
    pub ipv4_address: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub associated_resource: Option<String>,
    #[serde(default)]
    pub bdf: Option<String>,
    #[serde(default)]
    pub position: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThermalInfo {
    pub temperatures: Vec<TemperatureReading>,
    pub fans: Vec<FanReading>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemperatureReading {
    pub name: String,
    #[serde(default)]
    pub reading_celsius: Option<f64>,
    #[serde(default)]
    pub upper_threshold: Option<f64>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanReading {
    pub name: String,
    #[serde(default)]
    pub reading_rpm: Option<u32>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerInfo {
    #[serde(default)]
    pub power_consumed_watts: Option<f64>,
    #[serde(default)]
    pub power_capacity_watts: Option<f64>,
    #[serde(default)]
    pub current_cpu_power_watts: Option<f64>,
    #[serde(default)]
    pub current_memory_power_watts: Option<f64>,
    #[serde(default)]
    pub redundancy_mode: Option<String>,
    #[serde(default)]
    pub redundancy_health: Option<String>,
    pub power_supplies: Vec<PowerSupplyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerSupplyInfo {
    pub id: String,
    #[serde(default)]
    pub input_watts: Option<f64>,
    #[serde(default)]
    pub output_watts: Option<f64>,
    #[serde(default)]
    pub capacity_watts: Option<f64>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub firmware_version: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PCIeDeviceInfo {
    pub id: String,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub device_class: Option<String>,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub vendor_id: Option<String>,
    #[serde(default)]
    pub subsystem_id: Option<String>,
    #[serde(default)]
    pub subsystem_vendor_id: Option<String>,
    #[serde(default)]
    pub associated_resource: Option<String>,
    #[serde(default)]
    pub position: Option<String>,
    #[serde(default)]
    pub source_type: Option<String>,
    #[serde(default)]
    pub serial_number: Option<String>,
    #[serde(default)]
    pub firmware_version: Option<String>,
    #[serde(default)]
    pub link_width: Option<String>,
    #[serde(default)]
    pub link_speed: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub populated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogEntry {
    pub id: String,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub entry_type: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub suggestion: Option<String>,
    #[serde(default)]
    pub event_code: Option<String>,
    #[serde(default)]
    pub alert_status: Option<String>,
}
