use crate::error::{BmcError, BmcResult};
use crate::generic_redfish::GenericRedfishProvider;
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

pub struct InspurProvider;

#[derive(Clone)]
struct InspurSession {
    client: Client,
    base_url: String,
    csrf_token: String,
    q_session_id: String,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<InspurSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 240;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<InspurSession>>> {
    let mut pool = SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

fn make_client() -> BmcResult<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .gzip(true)
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| BmcError::internal(format!("HTTP client error: {}", e)))
}

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<InspurSession> {
    let client = make_client()?;
    let url = format!("{}/api/session", base_url);
    debug!("Inspur login POST {}", url);

    let resp = client
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[
            ("username", username),
            ("password", password),
            ("encrypt_flag", "0"),
            ("login_tag", "pxcloud"),
        ])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("Inspur login failed: {}", e)))?;

    let q_session_id = resp
        .cookies()
        .find(|c| c.name() == "QSESSIONID")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    let csrf_token = body
        .get("CSRFToken")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if q_session_id.is_empty() {
        return Err(BmcError::internal("Inspur login: no QSESSIONID cookie"));
    }

    Ok(InspurSession {
        client,
        base_url: base_url.to_string(),
        csrf_token,
        q_session_id,
        created_at: std::time::Instant::now(),
    })
}

async fn do_logout(session: &InspurSession) {
    let url = format!("{}/api/session", session.base_url);
    let _ = session
        .client
        .delete(&url)
        .header("X-CSRFTOKEN", &session.csrf_token)
        .header("Cookie", format!("QSESSIONID={}", session.q_session_id))
        .send()
        .await;
}

async fn get_session(creds: &BmcCreds) -> BmcResult<InspurSession> {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref session) = *guard {
        if session.created_at.elapsed().as_secs() < SESSION_MAX_AGE_SECS {
            return Ok(session.clone());
        }
        do_logout(session).await;
    }

    let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
    *guard = Some(session.clone());
    info!("Inspur: new session created for {}", key);
    Ok(session)
}

async fn invalidate_session(creds: &BmcCreds) {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    if let Some(ref session) = *guard {
        do_logout(session).await;
    }
    *guard = None;
}

async fn with_session<F, Fut, T>(creds: &BmcCreds, f: F) -> BmcResult<T>
where
    F: Fn(InspurSession) -> Fut + Send,
    Fut: std::future::Future<Output = BmcResult<T>> + Send,
{
    let session = get_session(creds).await?;
    match f(session).await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("401") || msg.contains("session") || msg.contains("unauthorized") {
                invalidate_session(creds).await;
                let session = get_session(creds).await?;
                f(session).await
            } else {
                Err(e)
            }
        }
    }
}

impl InspurSession {
    async fn get_json(&self, path: &str) -> BmcResult<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .header("X-CSRFTOKEN", &self.csrf_token)
            .header("X-Requested-With", "XMLHttpRequest")
            .header("Cookie", format!("QSESSIONID={}", self.q_session_id))
            .send()
            .await
            .map_err(|e| BmcError::internal(format!("Inspur GET {} failed: {}", path, e)))?;
        if !resp.status().is_success() {
            return Err(BmcError::internal(format!(
                "Inspur GET {} returned {}",
                path,
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| BmcError::internal(format!("Inspur parse {} failed: {}", path, e)))
    }
}

fn str_val(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        if x.is_string() {
            x.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
        } else if x.is_number() {
            Some(x.to_string())
        } else {
            None
        }
    })
}

fn f64_val(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

fn u32_val(v: &serde_json::Value, key: &str) -> Option<u32> {
    v.get(key).and_then(|x| x.as_u64()).map(|v| v as u32)
}

fn bool_num(v: &serde_json::Value, key: &str) -> bool {
    v.get(key).and_then(|x| x.as_i64()).unwrap_or(0) != 0
}

fn ts_to_iso(ts: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
}

fn inspur_sensor_type_name(sensor_type: i64) -> &'static str {
    match sensor_type {
        1 => "温度",
        2 => "电压",
        3 => "电流",
        4 => "风扇",
        5 => "物理安全",
        _ => "未知",
    }
}

fn inspur_event_code_desc(event_data1: i64) -> &'static str {
    match event_data1 {
        87 => "非关键性较高-变高",
        _ => "状态变更",
    }
}

fn inspur_event_dir_desc(event_dir_type: i64) -> &'static str {
    match event_dir_type {
        1 => "触发",
        129 => "解除",
        _ => "未知",
    }
}

fn inspur_severity_label(value: i64) -> &'static str {
    match value {
        2 => "critical",
        1 => "warning",
        _ => "info",
    }
}

#[async_trait]
impl BmcProvider for InspurProvider {
    fn name(&self) -> &str {
        "Inspur BMC"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let _ = get_session(creds).await?;
        Ok(true)
    }

    // User requirement: power operations use standard Redfish.
    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        GenericRedfishProvider.get_power_state(creds).await
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        GenericRedfishProvider.power_action(creds, action).await
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        with_session(creds, |s| async move {
            let fru = s.get_json("/api/fru").await.unwrap_or_default();
            let board = fru
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("board"));
            let product = fru
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("product"));

            let mem = s
                .get_json("/api/status/memory_info")
                .await
                .unwrap_or_default();
            let total_memory_gib = mem
                .get("mem_modules")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter(|m| bool_num(m, "mem_mod_status"))
                        .map(|m| f64_val(m, "mem_mod_size").unwrap_or(0.0))
                        .sum::<f64>()
                })
                .filter(|v| *v > 0.0);

            let cpu = s.get_json("/api/status/cpu_info").await.unwrap_or_default();
            let total_cpu_count = cpu
                .get("processors")
                .and_then(|v| v.as_array())
                .map(|arr| arr.len() as u32)
                .filter(|v| *v > 0);

            Ok(SystemInfo {
                manufacturer: board
                    .and_then(|b| str_val(b, "manufacturer"))
                    .or_else(|| product.and_then(|p| str_val(p, "manufacturer"))),
                model: board
                    .and_then(|b| str_val(b, "product_name"))
                    .or_else(|| product.and_then(|p| str_val(p, "product_name"))),
                serial_number: product.and_then(|p| str_val(p, "serial_number")),
                bios_version: None,
                bmc_version: None,
                hostname: None,
                power_state: None,
                total_cpu_count,
                total_memory_gib,
            })
        })
        .await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |s| async move {
            let cpu = s.get_json("/api/status/cpu_info").await?;
            let processors = cpu
                .get("processors")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            Ok(processors
                .iter()
                .map(|p| ProcessorInfo {
                    id: str_val(p, "proc_id").unwrap_or_else(|| "cpu".to_string()),
                    socket: str_val(p, "proc_socket").or_else(|| str_val(p, "proc_id")),
                    model: str_val(p, "proc_name"),
                    manufacturer: str_val(p, "proc_vendor"),
                    total_cores: u32_val(p, "proc_core_count"),
                    total_threads: u32_val(p, "proc_thread_count"),
                    max_speed_mhz: u32_val(p, "proc_speed"),
                    temperature_celsius: None,
                    status: str_val(p, "status"),
                    architecture: str_val(p, "proc_arch"),
                    frequency_mhz: u32_val(p, "proc_speed"),
                    l1_cache_kib: u32_val(p, "proc_l1cache_size"),
                    l2_cache_kib: u32_val(p, "proc_l2cache_size"),
                    l3_cache_kib: u32_val(p, "proc_l3cache_size"),
                    serial_number: str_val(p, "proc_SN"),
                    part_number: None,
                    instruction_set: None,
                })
                .collect())
        })
        .await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |s| async move {
            let mem = s.get_json("/api/status/memory_info").await?;
            let modules = mem
                .get("mem_modules")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            Ok(modules
                .iter()
                .map(|m| {
                    let populated = bool_num(m, "mem_mod_status");
                    MemoryInfo {
                        id: str_val(m, "mem_mod_id").unwrap_or_default(),
                        capacity_gib: f64_val(m, "mem_mod_size").filter(|v| *v > 0.0),
                        memory_type: str_val(m, "mem_mod_type"),
                        speed_mhz: u32_val(m, "mem_mod_frequency"),
                        manufacturer: str_val(m, "mem_mod_vendor"),
                        serial_number: str_val(m, "mem_mod_serial_num"),
                        slot: str_val(m, "mem_device_locator")
                            .or_else(|| str_val(m, "mem_mod_slot")),
                        channel: str_val(m, "mem_channel"),
                        slot_index: u32_val(m, "mem_slot"),
                        temperature_celsius: None,
                        populated,
                        status: str_val(m, "status"),
                        part_number: str_val(m, "mem_mod_part_num"),
                        rank_count: u32_val(m, "mem_mod_ranks"),
                        module_type: str_val(m, "mem_base_type"),
                        data_width_bits: u32_val(m, "mem_mod_data_width"),
                    }
                })
                .collect())
        })
        .await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        with_session(creds, |s| async move {
            let sata = s
                .get_json("/api/status/SATA_HDDinfo")
                .await
                .unwrap_or_default();
            let arr = sata.as_array().cloned().unwrap_or_default();
            Ok(arr
                .iter()
                .filter(|d| bool_num(d, "present"))
                .map(|d| StorageInfo {
                    id: str_val(d, "id").unwrap_or_default(),
                    name: str_val(d, "model"),
                    capacity_gib: f64_val(d, "capacity"),
                    media_type: None,
                    protocol: Some("SATA".to_string()),
                    manufacturer: None,
                    model: str_val(d, "model"),
                    serial_number: str_val(d, "SN"),
                    status: Some("Present".to_string()),
                    firmware_version: None,
                    rotation_speed_rpm: None,
                    capable_speed_gbps: None,
                    negotiated_speed_gbps: None,
                    failure_predicted: None,
                    predicted_media_life_left_percent: None,
                    hotspare_type: None,
                    temperature_celsius: None,
                    hours_powered_on: None,
                    slot_number: None,
                    form_factor: None,
                    firmware_status: None,
                    raid_level: None,
                    controller_name: None,
                    rebuild_state: None,
                })
                .collect())
        })
        .await
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        with_session(creds, |s| async move {
            let nets = s
                .get_json("/api/settings/network")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let arr = nets.as_array().cloned().unwrap_or_default();
            Ok(arr
                .iter()
                .map(|n| NetworkInterfaceInfo {
                    id: str_val(n, "id").unwrap_or_else(|| "nic".to_string()),
                    name: str_val(n, "interface_name"),
                    mac_address: str_val(n, "mac_address"),
                    speed_mbps: None,
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: Some(
                        if bool_num(n, "lan_enable") {
                            "Up"
                        } else {
                            "Down"
                        }
                        .to_string(),
                    ),
                    ipv4_address: str_val(n, "ipv4_address"),
                    manufacturer: None,
                    model: None,
                    slot: None,
                    associated_resource: None,
                    bdf: None,
                    position: None,
                })
                .collect())
        })
        .await
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |s| async move {
            let fan_info = s.get_json("/api/status/fan_info").await.unwrap_or_default();
            let fans = fan_info
                .get("fans")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|f| FanReading {
                    name: str_val(f, "fan_name").unwrap_or_default(),
                    reading_rpm: u32_val(f, "speed_rpm"),
                    status: str_val(f, "status_str"),
                })
                .collect::<Vec<_>>();

            let sensors = s
                .get_json("/api/sensors")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let temperatures = sensors
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|x| {
                    str_val(x, "type")
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains("temperature")
                })
                .map(|x| TemperatureReading {
                    name: str_val(x, "name").unwrap_or_default(),
                    reading_celsius: f64_val(x, "reading").or_else(|| f64_val(x, "raw_reading")),
                    upper_threshold: f64_val(x, "higher_critical_threshold")
                        .or_else(|| f64_val(x, "higher_non_critical_threshold")),
                    status: str_val(x, "sensor_state"),
                })
                .collect::<Vec<_>>();

            Ok(ThermalInfo { temperatures, fans })
        })
        .await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |s| async move {
            let psu = s.get_json("/api/status/psu_info").await?;
            let consumed = f64_val(&psu, "present_power_reading");
            let capacity = f64_val(&psu, "rated_power");
            let redundancy_mode = if bool_num(&psu, "power_supplies_redundant") {
                Some("redundant".to_string())
            } else {
                Some("non_redundant".to_string())
            };

            let mut power_supplies: Vec<PowerSupplyInfo> = psu
                .get("power_supplies")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|x| bool_num(x, "present"))
                .map(|x| PowerSupplyInfo {
                    id: str_val(x, "id").unwrap_or_default(),
                    input_watts: f64_val(x, "ps_in_power"),
                    output_watts: f64_val(x, "ps_out_power"),
                    capacity_watts: f64_val(x, "rated_power")
                        .or_else(|| f64_val(x, "ps_out_power_max")),
                    serial_number: str_val(x, "serial_num"),
                    firmware_version: str_val(x, "fw_ver"),
                    manufacturer: str_val(x, "vendor_id"),
                    model: str_val(x, "model"),
                    status: str_val(x, "status"),
                })
                .collect();

            power_supplies.sort_by(|a, b| a.id.cmp(&b.id));

            let redundancy_health = if power_supplies.is_empty() {
                None
            } else if power_supplies.iter().all(|p| {
                p.status
                    .as_deref()
                    .map(|s| s.eq_ignore_ascii_case("ok"))
                    .unwrap_or(false)
            }) {
                Some("ok".to_string())
            } else {
                Some("warning".to_string())
            };

            Ok(PowerInfo {
                power_consumed_watts: consumed,
                power_capacity_watts: capacity,
                current_cpu_power_watts: None,
                current_memory_power_watts: None,
                redundancy_mode,
                redundancy_health,
                power_supplies,
            })
        })
        .await
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        with_session(creds, |s| async move {
            let inv = s.get_json("/api/status/device_inventory").await?;
            let arr = inv.as_array().cloned().unwrap_or_default();
            Ok(arr
                .iter()
                .map(|d| PCIeDeviceInfo {
                    id: str_val(d, "id").unwrap_or_default(),
                    slot: str_val(d, "pcie_slot_name").or_else(|| str_val(d, "slot")),
                    name: str_val(d, "device_name"),
                    description: None,
                    manufacturer: str_val(d, "vendor_name"),
                    model: str_val(d, "device_name"),
                    device_class: str_val(d, "dev_type"),
                    device_id: None,
                    vendor_id: None,
                    subsystem_id: str_val(d, "subsystem_id"),
                    subsystem_vendor_id: str_val(d, "SubSysVendorId"),
                    associated_resource: None,
                    position: str_val(d, "location"),
                    source_type: Some("device_inventory".to_string()),
                    serial_number: str_val(d, "serial_num"),
                    firmware_version: str_val(d, "fw_ver"),
                    link_width: str_val(d, "current_link_width"),
                    link_speed: str_val(d, "current_link_speed"),
                    status: str_val(d, "health"),
                    populated: bool_num(d, "present"),
                })
                .collect())
        })
        .await
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        with_session(creds, |s| async move {
            let event_logs = s
                .get_json("/api/logs/event")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let idl_logs = s
                .get_json("/api/logs/idl")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let audit_logs = s
                .get_json("/api/logs/audit")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));

            let mut merged: Vec<(i64, EventLogEntry)> = Vec::new();

            for item in event_logs.as_array().cloned().unwrap_or_default() {
                let ts = item.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let sensor_type = item
                    .get("sensor_type")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let event_dir_type = item
                    .get("event_dir_type")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let event_data1 = item
                    .get("event_data1")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let sensor_name =
                    str_val(&item, "sensor_name").unwrap_or_else(|| "unknown".to_string());
                let message = format!(
                    "{} | {} | {} - {}",
                    sensor_name,
                    inspur_sensor_type_name(sensor_type),
                    inspur_event_code_desc(event_data1),
                    inspur_event_dir_desc(event_dir_type)
                );
                merged.push((
                    ts,
                    EventLogEntry {
                        id: format!("event:{}", str_val(&item, "id").unwrap_or_default()),
                        severity: Some(
                            inspur_severity_label(
                                item.get("severity").and_then(|v| v.as_i64()).unwrap_or(0),
                            )
                            .to_string(),
                        ),
                        message: Some(message),
                        created: ts_to_iso(ts),
                        entry_type: Some("event".to_string()),
                        subject: Some(inspur_sensor_type_name(sensor_type).to_string()),
                        suggestion: None,
                        event_code: Some(event_data1.to_string()),
                        alert_status: None,
                    },
                ));
            }

            for item in idl_logs.as_array().cloned().unwrap_or_default() {
                let ts = item.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let dec = str_val(&item, "dec").unwrap_or_else(|| "IDL".to_string());
                let advice = str_val(&item, "advice").unwrap_or_default();
                let message = if advice.is_empty() {
                    dec
                } else {
                    format!("{} | 建议: {}", dec, advice)
                };
                merged.push((
                    ts,
                    EventLogEntry {
                        id: format!("idl:{}", str_val(&item, "id").unwrap_or_default()),
                        severity: Some(
                            inspur_severity_label(
                                item.get("severity").and_then(|v| v.as_i64()).unwrap_or(0),
                            )
                            .to_string(),
                        ),
                        message: Some(message),
                        created: ts_to_iso(ts),
                        entry_type: Some("idl".to_string()),
                        subject: str_val(&item, "type"),
                        suggestion: str_val(&item, "advice"),
                        event_code: str_val(&item, "errorCode"),
                        alert_status: str_val(&item, "status"),
                    },
                ));
            }

            for item in audit_logs.as_array().cloned().unwrap_or_default() {
                let ts = item.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                merged.push((
                    ts,
                    EventLogEntry {
                        id: format!("audit:{}", str_val(&item, "id").unwrap_or_default()),
                        severity: Some("info".to_string()),
                        message: str_val(&item, "message"),
                        created: ts_to_iso(ts),
                        entry_type: Some("audit".to_string()),
                        subject: str_val(&item, "hostname"),
                        suggestion: None,
                        event_code: None,
                        alert_status: None,
                    },
                ));
            }

            merged.sort_by(|a, b| b.0.cmp(&a.0));
            let mut out: Vec<EventLogEntry> = merged.into_iter().map(|(_, e)| e).collect();
            if let Some(lim) = limit {
                out.truncate(lim as usize);
            }
            Ok(out)
        })
        .await
    }

    async fn clear_event_logs(&self, _creds: &BmcCreds) -> BmcResult<()> {
        warn!("Inspur clear_event_logs not implemented");
        Ok(())
    }
}
