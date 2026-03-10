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
use tracing::{info, warn};

pub struct GuoxinProvider;

#[derive(Clone)]
struct GuoxinSession {
    client: Client,
    base_url: String,
    csrf_token: String,
    q_session_id: String,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<GuoxinSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 240;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<GuoxinSession>>> {
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

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<GuoxinSession> {
    let client = make_client()?;
    let url = format!("{}/api/session", base_url);

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
        .map_err(|e| BmcError::internal(format!("Guoxin login failed: {}", e)))?;

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
        return Err(BmcError::internal("Guoxin login: no QSESSIONID cookie"));
    }

    Ok(GuoxinSession {
        client,
        base_url: base_url.to_string(),
        csrf_token,
        q_session_id,
        created_at: std::time::Instant::now(),
    })
}

async fn do_logout(session: &GuoxinSession) {
    let url = format!("{}/api/session", session.base_url);
    let _ = session
        .client
        .delete(&url)
        .header("X-CSRFTOKEN", &session.csrf_token)
        .header("Cookie", format!("QSESSIONID={}", session.q_session_id))
        .send()
        .await;
}

async fn get_session(creds: &BmcCreds) -> BmcResult<GuoxinSession> {
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
    info!("Guoxin: new session created for {}", key);
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
    F: Fn(GuoxinSession) -> Fut + Send,
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

impl GuoxinSession {
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
            .map_err(|e| BmcError::internal(format!("Guoxin GET {} failed: {}", path, e)))?;
        if !resp.status().is_success() {
            return Err(BmcError::internal(format!(
                "Guoxin GET {} returned {}",
                path,
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| BmcError::internal(format!("Guoxin parse {} failed: {}", path, e)))
    }
}

fn str_val(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        if x.is_string() {
            x.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .and_then(|s| {
                    if s.eq_ignore_ascii_case("n/a") {
                        None
                    } else {
                        Some(s.to_string())
                    }
                })
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
    v.get(key).and_then(|x| x.as_u64()).map(|x| x as u32)
}

fn i64_val(v: &serde_json::Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

fn bool_num(v: &serde_json::Value, key: &str) -> bool {
    v.get(key).and_then(|x| x.as_i64()).unwrap_or(0) != 0
}

fn ts_to_iso(ts: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
}

#[async_trait]
impl BmcProvider for GuoxinProvider {
    fn name(&self) -> &str {
        "Guoxin BMC"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let _ = get_session(creds).await?;
        Ok(true)
    }

    // Prefer standard Redfish for power operations; fallback to chassis-status for read only.
    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        match GenericRedfishProvider.get_power_state(creds).await {
            Ok(v) => Ok(v),
            Err(_) => {
                with_session(creds, |s| async move {
                    let ch = s.get_json("/api/chassis-status").await?;
                    let state = match ch.get("power_status").and_then(|v| v.as_i64()) {
                        Some(1) => "on",
                        Some(0) => "off",
                        _ => "unknown",
                    };
                    Ok(state.to_string())
                })
                .await
            }
        }
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

            let cpus = s
                .get_json("/api/GooxiSysInfo/CpuInfo")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let mems = s
                .get_json("/api/GooxiSysInfo/MemInfo")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));

            let total_cpu_count = cpus.as_array().map(|arr| arr.len() as u32).filter(|v| *v > 0);
            let total_memory_gib = mems
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter(|m| bool_num(m, "status_flag"))
                        .map(|m| f64_val(m, "capacity").unwrap_or(0.0))
                        .sum::<f64>()
                })
                .filter(|v| *v > 0.0);

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
            let cpu = s.get_json("/api/GooxiSysInfo/CpuInfo").await?;
            let processors = cpu.as_array().cloned().unwrap_or_default();
            Ok(processors
                .iter()
                .map(|p| ProcessorInfo {
                    id: str_val(p, "cpu_index").unwrap_or_default(),
                    socket: str_val(p, "socket_id"),
                    model: str_val(p, "model"),
                    manufacturer: None,
                    total_cores: u32_val(p, "core_num"),
                    total_threads: None,
                    max_speed_mhz: u32_val(p, "main_freq"),
                    temperature_celsius: None,
                    status: Some("OK".to_string()),
                    architecture: None,
                    frequency_mhz: u32_val(p, "main_freq"),
                    l1_cache_kib: u32_val(p, "l1_cache"),
                    l2_cache_kib: u32_val(p, "l2_cache"),
                    l3_cache_kib: u32_val(p, "l3_cache"),
                    serial_number: None,
                    part_number: None,
                    instruction_set: None,
                })
                .collect())
        })
        .await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |s| async move {
            let mem = s.get_json("/api/GooxiSysInfo/MemInfo").await?;
            let modules = mem.as_array().cloned().unwrap_or_default();

            Ok(modules
                .iter()
                .map(|m| {
                    let populated = bool_num(m, "status_flag");
                    let socket = str_val(m, "loc_socket_id").unwrap_or_else(|| "?".to_string());
                    let channel = str_val(m, "loc_chnl_num").unwrap_or_else(|| "?".to_string());
                    let dimm = str_val(m, "loc_dimm_slot_id").unwrap_or_else(|| "?".to_string());
                    MemoryInfo {
                        id: str_val(m, "mem_index").unwrap_or_default(),
                        capacity_gib: f64_val(m, "capacity").filter(|v| *v > 0.0),
                        memory_type: str_val(m, "type"),
                        speed_mhz: u32_val(m, "freq"),
                        manufacturer: str_val(m, "vendor_id"),
                        serial_number: None,
                        slot: Some(format!("CPU{}-CH{}-DIMM{}", socket, channel, dimm)),
                        channel: Some(channel),
                        slot_index: u32_val(m, "mem_index"),
                        temperature_celsius: None,
                        populated,
                        status: if populated {
                            Some("OK".to_string())
                        } else {
                            Some("Absent".to_string())
                        },
                        part_number: None,
                        rank_count: None,
                        module_type: None,
                        data_width_bits: None,
                    }
                })
                .collect())
        })
        .await
    }

    async fn get_storage(&self, _creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        // Placeholder: no stable storage endpoint in current Guoxin API document.
        Ok(vec![])
    }

    async fn get_network_interfaces(
        &self,
        _creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        // Placeholder: no stable NIC endpoint in current Guoxin API document.
        Ok(vec![])
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |s| async move {
            let sensors = s.get_json("/api/sensors").await?;
            let arr = sensors.as_array().cloned().unwrap_or_default();

            let temperatures = arr
                .iter()
                .filter(|x| str_val(x, "type").unwrap_or_default().to_lowercase() == "temperature")
                .map(|x| TemperatureReading {
                    name: str_val(x, "name").unwrap_or_default(),
                    reading_celsius: f64_val(x, "reading").or_else(|| f64_val(x, "raw_reading")),
                    upper_threshold: f64_val(x, "higher_critical_threshold")
                        .or_else(|| f64_val(x, "higher_non_critical_threshold")),
                    status: Some(if i64_val(x, "sensor_state").unwrap_or(0) == 1 {
                        "ok".to_string()
                    } else {
                        "warning".to_string()
                    }),
                })
                .collect::<Vec<_>>();

            let fans = arr
                .iter()
                .filter(|x| str_val(x, "type").unwrap_or_default().to_lowercase() == "fan")
                .map(|x| FanReading {
                    name: str_val(x, "name").unwrap_or_default(),
                    reading_rpm: u32_val(x, "reading").or_else(|| u32_val(x, "raw_reading")),
                    status: Some(if i64_val(x, "sensor_state").unwrap_or(0) == 1 {
                        "ok".to_string()
                    } else {
                        "warning".to_string()
                    }),
                })
                .collect::<Vec<_>>();

            Ok(ThermalInfo { temperatures, fans })
        })
        .await
    }

    async fn get_power(&self, _creds: &BmcCreds) -> BmcResult<PowerInfo> {
        // Placeholder: detailed PSU/power endpoint not confirmed in current Guoxin API doc.
        Ok(PowerInfo {
            power_consumed_watts: None,
            power_capacity_watts: None,
            current_cpu_power_watts: None,
            current_memory_power_watts: None,
            redundancy_mode: None,
            redundancy_health: None,
            power_supplies: vec![],
        })
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        with_session(creds, |s| async move {
            // Prefer GooxiSysInfo endpoint. Fallback to status/device_inventory.
            let pcie = match s.get_json("/api/GooxiSysInfo/PcieInfo").await {
                Ok(v) => v,
                Err(_) => s.get_json("/api/status/device_inventory").await?,
            };
            let arr = pcie.as_array().cloned().unwrap_or_default();

            Ok(arr
                .iter()
                .map(|d| {
                    let populated = bool_num(d, "status_flag") || bool_num(d, "present");
                    PCIeDeviceInfo {
                        id: str_val(d, "pcie_index")
                            .or_else(|| str_val(d, "id"))
                            .unwrap_or_default(),
                        slot: str_val(d, "slot_name").or_else(|| str_val(d, "pcie_slot_name")),
                        name: str_val(d, "device_name").or_else(|| str_val(d, "vendor_id")),
                        description: None,
                        manufacturer: str_val(d, "vendor_id").or_else(|| str_val(d, "vendor_name")),
                        model: str_val(d, "device_name"),
                        device_class: str_val(d, "class_code").or_else(|| str_val(d, "dev_type")),
                        device_id: str_val(d, "dev_id"),
                        vendor_id: None,
                        subsystem_id: str_val(d, "subsystem_id"),
                        subsystem_vendor_id: str_val(d, "SubSysVendorId"),
                        associated_resource: None,
                        position: str_val(d, "socket_id").or_else(|| str_val(d, "location")),
                        source_type: Some("gooxi_pcie".to_string()),
                        serial_number: str_val(d, "serial_num"),
                        firmware_version: str_val(d, "fw_ver"),
                        link_width: str_val(d, "cur_bandwidth")
                            .or_else(|| str_val(d, "current_link_width")),
                        link_speed: str_val(d, "cur_speed")
                            .or_else(|| str_val(d, "current_link_speed")),
                        status: str_val(d, "health").or_else(|| {
                            if populated {
                                Some("OK".to_string())
                            } else {
                                Some("Absent".to_string())
                            }
                        }),
                        populated,
                    }
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
                .get_json("/api/logs/eventlog")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));
            let audit_logs = s
                .get_json("/api/logs/audit?level=")
                .await
                .unwrap_or_else(|_| serde_json::json!([]));

            let mut merged: Vec<(i64, EventLogEntry)> = Vec::new();

            for item in event_logs.as_array().cloned().unwrap_or_default() {
                let ts = item.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let sensor_name =
                    str_val(&item, "sensor_name").unwrap_or_else(|| "unknown".to_string());
                let event_desc =
                    str_val(&item, "event_description").unwrap_or_else(|| "event".to_string());
                let direction = str_val(&item, "event_direction").unwrap_or_default();
                let advanced = str_val(&item, "advanced_event_description").unwrap_or_default();
                let message = if advanced.is_empty() || advanced.eq_ignore_ascii_case("unknown") {
                    format!("{} | {} | {}", sensor_name, event_desc, direction)
                } else {
                    format!("{} | {} | {} | {}", sensor_name, event_desc, direction, advanced)
                };
                let severity = if direction.eq_ignore_ascii_case("asserted") {
                    "warning"
                } else {
                    "info"
                };
                merged.push((
                    ts,
                    EventLogEntry {
                        id: format!("event:{}", str_val(&item, "id").unwrap_or_default()),
                        severity: Some(severity.to_string()),
                        message: Some(message),
                        created: ts_to_iso(ts),
                        entry_type: Some("event".to_string()),
                        subject: str_val(&item, "sensor_type"),
                        suggestion: None,
                        event_code: str_val(&item, "offset"),
                        alert_status: Some(direction),
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
        // Placeholder: no confirmed clear logs endpoint.
        warn!("Guoxin clear_event_logs not implemented");
        Ok(())
    }
}
