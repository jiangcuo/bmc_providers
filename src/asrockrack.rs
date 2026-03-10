use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

pub struct AsrockrackProvider;

// ─── Global session pool (per-host async mutex) ─────────────────────────

#[derive(Clone)]
struct AsrockrackSession {
    client: Client,
    base_url: String,
    csrf_token: String,
    q_session_id: String,
    login_response: serde_json::Value,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<AsrockrackSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 240;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<AsrockrackSession>>> {
    let mut pool = SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

async fn get_session(creds: &BmcCreds) -> BmcResult<AsrockrackSession> {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref session) = *guard {
        if session.created_at.elapsed().as_secs() < SESSION_MAX_AGE_SECS {
            debug!(
                "ASRockRack: reusing cached session for {} (age={}s)",
                key,
                session.created_at.elapsed().as_secs()
            );
            return Ok(session.clone());
        }
        debug!(
            "ASRockRack: session expired for {}, logging out and re-login",
            key
        );
        do_logout(session).await;
    }

    let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
    *guard = Some(session.clone());
    info!("ASRockRack: new session created for {}", key);
    Ok(session)
}

async fn invalidate_session(creds: &BmcCreds) {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    if let Some(ref session) = *guard {
        do_logout(session).await;
        debug!("ASRockRack: invalidated session for {}", key);
    }
    *guard = None;
}

async fn do_logout(session: &AsrockrackSession) {
    let url = format!("{}/api/session", session.base_url);
    let _ = session
        .client
        .delete(&url)
        .header("X-CSRFTOKEN", &session.csrf_token)
        .header("Cookie", format!("QSESSIONID={}", session.q_session_id))
        .send()
        .await;
    debug!("ASRockRack: session logged out for {}", session.base_url);
}

async fn with_session<F, Fut, T>(creds: &BmcCreds, f: F) -> BmcResult<T>
where
    F: Fn(AsrockrackSession) -> Fut + Send,
    Fut: std::future::Future<Output = BmcResult<T>> + Send,
{
    let session = get_session(creds).await?;
    match f(session).await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("401") || msg.contains("session") || msg.contains("unauthorized") {
                warn!("ASRockRack session error, invalidating and retrying: {}", msg);
                invalidate_session(creds).await;
                let session = get_session(creds).await?;
                f(session).await
            } else {
                Err(e)
            }
        }
    }
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

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<AsrockrackSession> {
    let client = make_client()?;
    let url = format!("{}/api/session", base_url);
    debug!("ASRockRack login POST {}", url);

    let resp = client
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("X-CSRFTOKEN", "null")
        .body(format!(
            "username={}&password={}&certlogin=0",
            username, password
        ))
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("ASRockRack login failed: {}", e)))?;

    let q_session_id = resp
        .cookies()
        .find(|c| c.name() == "QSESSIONID")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| BmcError::internal(format!("ASRockRack login parse error: {}", e)))?;

    let csrf_token = body
        .get("CSRFToken")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if q_session_id.is_empty() {
        return Err(BmcError::internal("ASRockRack login: no QSESSIONID cookie"));
    }

    debug!(
        "ASRockRack login success: session={}, csrf={}",
        &q_session_id[..q_session_id.len().min(10)],
        csrf_token
    );
    Ok(AsrockrackSession {
        client,
        base_url: base_url.to_string(),
        csrf_token,
        q_session_id,
        login_response: body,
        created_at: std::time::Instant::now(),
    })
}

impl AsrockrackSession {
    async fn get_json(&self, path: &str) -> BmcResult<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        debug!("ASRockRack GET {}", url);

        let resp = self
            .client
            .get(&url)
            .header("X-CSRFTOKEN", &self.csrf_token)
            .header("Cookie", format!("QSESSIONID={}", self.q_session_id))
            .send()
            .await
            .map_err(|e| BmcError::internal(format!("ASRockRack GET {} failed: {}", path, e)))?;

        if !resp.status().is_success() {
            return Err(BmcError::internal(format!(
                "ASRockRack GET {} returned {}",
                path,
                resp.status()
            )));
        }

        resp.json()
            .await
            .map_err(|e| BmcError::internal(format!("ASRockRack parse {} failed: {}", path, e)))
    }

    async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> BmcResult<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        debug!("ASRockRack POST {}", url);

        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-CSRFTOKEN", &self.csrf_token)
            .header("Cookie", format!("QSESSIONID={}", self.q_session_id))
            .json(body)
            .send()
            .await
            .map_err(|e| BmcError::internal(format!("ASRockRack POST {} failed: {}", path, e)))?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            warn!(
                "ASRockRack POST {} returned HTTP {}: {}",
                path,
                status,
                &resp_body[..resp_body.len().min(500)]
            );
            return Err(BmcError::internal(format!(
                "ASRockRack POST {} returned HTTP {}",
                path, status
            )));
        }

        let json: serde_json::Value =
            serde_json::from_str(&resp_body).unwrap_or(serde_json::json!({"status": "ok"}));
        debug!("ASRockRack POST {} => {}", path, status);
        Ok(json)
    }
}

/// BMC 有时返回未经模板引擎处理的原始 JNLP（包含 <% JAVA_RC_PARAMS(...) %> 标签），
/// 这里手动替换这些标签为真实值。
fn process_jnlp_template(
    template: &str,
    host: &str,
    port: u16,
    use_tls: bool,
    session_id: &str,
) -> String {
    let scheme = if use_tls { "https" } else { "http" };
    let base_url = format!("{}://{}:{}", scheme, host, port);

    let mut result = template.to_string();

    result = result.replace(
        r#"<% JAVA_RC_PARAMS("$IP_KEY$"); %>"#,
        &base_url,
    );

    if result.contains(r#"JAVA_RC_PARAMS("$ALL_ARGUMENTS$")"#) {
        let arguments = format!(
            concat!(
                "<argument>-hostname</argument>\n",
                "        <argument>{host}</argument>\n",
                "        <argument>-kvmport</argument>\n",
                "        <argument>{port}</argument>\n",
                "        <argument>-webport</argument>\n",
                "        <argument>{port}</argument>\n",
                "        <argument>-kvmtoken</argument>\n",
                "        <argument>{token}</argument>\n",
                "        <argument>encryption</argument>\n",
                "        <argument>1</argument>\n",
                "        <argument>-title</argument>\n",
                "        <argument>{host}</argument>",
            ),
            host = host,
            port = port,
            token = session_id,
        );
        result = result.replace(
            r#"<% JAVA_RC_PARAMS("$ALL_ARGUMENTS$"); %>"#,
            &arguments,
        );
    }

    while let Some(start) = result.find("<%") {
        if let Some(end_offset) = result[start..].find("%>") {
            result.replace_range(start..start + end_offset + 2, "");
        } else {
            break;
        }
    }

    result
}

fn str_val(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(|x| {
            if x.is_string() {
                x.as_str().map(String::from)
            } else {
                Some(x.to_string())
            }
        })
        .filter(|s| !s.is_empty() && s != "N/A" && s != "N\\/A")
}

fn f64_val(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

fn parse_memory_extra(extra: &str) -> (Option<u32>, Option<f64>) {
    let parts: Vec<&str> = extra.split_whitespace().collect();
    let mut speed_mhz = None;
    let mut capacity_gib = None;
    for (i, part) in parts.iter().enumerate() {
        if *part == "MT/s" || *part == "MHz" {
            if i > 0 {
                speed_mhz = parts[i - 1].parse::<u32>().ok();
            }
        }
        if part.ends_with("GB") {
            capacity_gib = part.trim_end_matches("GB").parse::<f64>().ok();
        }
        if part.ends_with("TB") {
            capacity_gib = part
                .trim_end_matches("TB")
                .parse::<f64>()
                .ok()
                .map(|v| v * 1024.0);
        }
    }
    (speed_mhz, capacity_gib)
}

#[async_trait]
impl BmcProvider for AsrockrackProvider {
    fn name(&self) -> &str {
        "ASRockRack"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let _session = get_session(creds).await?;
        Ok(true)
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        with_session(creds, |session| async move {
            let data = match session.get_json("/api/chassis").await {
                Ok(d) => d,
                Err(_) => session.get_json("/api/chassis-status").await?,
            };
            let status = data
                .get("power_status")
                .and_then(|v| v.as_i64())
                .unwrap_or(-1);
            Ok(match status {
                1 => "On",
                0 => "Off",
                _ => "Unknown",
            }
            .to_string())
        })
        .await
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        let cmd = match action {
            "on" => 1,
            "off" | "graceful_shutdown" => 0,
            "reset" => 3,
            _ => {
                return Err(BmcError::bad_request(format!(
                    "Unknown power action: {}",
                    action
                )))
            }
        };
        with_session(creds, |session| async move {
            session
                .post_json(
                    "/api/actions/power",
                    &serde_json::json!({"power_command": cmd}),
                )
                .await?;
            Ok(format!("Power action '{}' executed", action))
        })
        .await
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        with_session(creds, |session| async move {
            let fw = session
                .get_json("/api/asrr/fw-info")
                .await
                .unwrap_or_default();
            let bios_model = session
                .get_json("/api/asrr/bios-model-name")
                .await
                .unwrap_or_default();
            let inventory: Vec<serde_json::Value> = session
                .get_json("/api/asrr/inventory_info")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let fru_list: Vec<serde_json::Value> = session
                .get_json("/api/fru")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();
            let fru = fru_list.first();
            let board = fru.and_then(|f| f.get("board"));
            let fru_serial = board.and_then(|b| str_val(b, "serial_number"));
            let fru_manufacturer = board.and_then(|b| str_val(b, "manufacturer"));
            let fru_model = board.and_then(|b| str_val(b, "product_name"));

            let cpu = inventory
                .iter()
                .find(|d| str_val(d, "device_type").as_deref() == Some("CPU"));
            let mut total_mem = 0.0f64;
            for d in &inventory {
                if str_val(d, "device_type").as_deref() == Some("Memory") {
                    if let Some(extra) = str_val(d, "product_extra") {
                        let (_, cap) = parse_memory_extra(&extra);
                        if let Some(gb) = cap {
                            total_mem += gb;
                        }
                    }
                }
            }

            Ok(SystemInfo {
                manufacturer: fru_manufacturer
                    .or_else(|| cpu.and_then(|c| str_val(c, "product_manufacturer_name"))),
                model: str_val(&bios_model, "bios_mb_name").or(fru_model),
                serial_number: fru_serial,
                bios_version: str_val(&fw, "BIOS_fw_version"),
                bmc_version: str_val(&fw, "BMC_fw_version"),
                hostname: None,
                power_state: None,
                total_cpu_count: Some(
                    inventory
                        .iter()
                        .filter(|d| str_val(d, "device_type").as_deref() == Some("CPU"))
                        .count() as u32,
                ),
                total_memory_gib: if total_mem > 0.0 {
                    Some(total_mem)
                } else {
                    None
                },
            })
        })
        .await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |session| async move {
            let inventory: Vec<serde_json::Value> = session
                .get_json("/api/asrr/inventory_info")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();
            let sensors: Vec<serde_json::Value> = session
                .get_json("/api/sensors")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let mut cpus: Vec<ProcessorInfo> = Vec::new();
            for d in &inventory {
                if str_val(d, "device_type").as_deref() != Some("CPU") {
                    continue;
                }
                let name = str_val(d, "device_name").unwrap_or_default();
                let temp = sensors
                    .iter()
                    .find(|s| {
                        let sn = str_val(s, "name").unwrap_or_default();
                        sn.to_lowercase().contains(&name.to_lowercase())
                            && str_val(s, "type").as_deref() == Some("temperature")
                    })
                    .and_then(|s| f64_val(s, "reading"));

                cpus.push(ProcessorInfo {
                    id: name.clone(),
                    socket: Some(name),
                    model: str_val(d, "product_name"),
                    manufacturer: str_val(d, "product_manufacturer_name"),
                    total_cores: None,
                    total_threads: None,
                    max_speed_mhz: None,
                    temperature_celsius: temp,
                    status: Some("Present".to_string()),
                    architecture: None,
                    frequency_mhz: None,
                    l1_cache_kib: None,
                    l2_cache_kib: None,
                    l3_cache_kib: None,
                    serial_number: None,
                    part_number: None,
                    instruction_set: None,
                });
            }
            Ok(cpus)
        })
        .await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |session| async move {
            let inventory: Vec<serde_json::Value> = session
                .get_json("/api/asrr/inventory_info")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();
            let sensors: Vec<serde_json::Value> = session
                .get_json("/api/sensors")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let mut dimms: Vec<MemoryInfo> = Vec::new();
            for d in &inventory {
                if str_val(d, "device_type").as_deref() != Some("Memory") {
                    continue;
                }
                let slot_name = str_val(d, "device_name").unwrap_or_default();
                let extra = str_val(d, "product_extra").unwrap_or_default();
                let (speed, capacity) = parse_memory_extra(&extra);

                let channel = slot_name
                    .chars()
                    .find(|c| c.is_ascii_uppercase() && *c >= 'A' && *c <= 'H')
                    .map(|c| c.to_string());
                let slot_idx = slot_name.chars().last().and_then(|c| c.to_digit(10));

                let temp = sensors
                    .iter()
                    .find(|s| {
                        let sn = str_val(s, "name").unwrap_or_default().to_lowercase();
                        let sl = slot_name.to_lowercase();
                        (sn.contains("mem")
                            && sn.contains(&channel.clone().unwrap_or_default().to_lowercase()))
                            || sn.contains(&sl)
                    })
                    .and_then(|s| f64_val(s, "reading"));

                dimms.push(MemoryInfo {
                    id: slot_name.clone(),
                    capacity_gib: capacity,
                    memory_type: str_val(d, "product_name"),
                    speed_mhz: speed,
                    manufacturer: str_val(d, "product_manufacturer_name"),
                    serial_number: str_val(d, "product_serial_number"),
                    slot: Some(slot_name),
                    channel,
                    slot_index: slot_idx,
                    temperature_celsius: temp,
                    populated: true,
                    status: Some("Present".to_string()),
                    part_number: None,
                    rank_count: None,
                    module_type: None,
                    data_width_bits: None,
                });
            }
            Ok(dimms)
        })
        .await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        with_session(creds, |session| async move {
            let inventory: Vec<serde_json::Value> = session
                .get_json("/api/asrr/inventory_info")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let mut drives: Vec<StorageInfo> = Vec::new();
            for d in &inventory {
                let dt = str_val(d, "device_type").unwrap_or_default();
                if !dt.contains("Storage") {
                    continue;
                }
                drives.push(StorageInfo {
                    id: str_val(d, "device_name").unwrap_or_default(),
                    name: str_val(d, "device_name"),
                    capacity_gib: None,
                    media_type: None,
                    protocol: None,
                    manufacturer: str_val(d, "product_manufacturer_name"),
                    model: str_val(d, "product_part_number"),
                    serial_number: str_val(d, "product_serial_number"),
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
                });
            }
            Ok(drives)
        })
        .await
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        with_session(creds, |session| async move {
            let nets: Vec<serde_json::Value> = session
                .get_json("/api/settings/network")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            Ok(nets
                .iter()
                .map(|n| NetworkInterfaceInfo {
                    id: str_val(n, "interface_name").unwrap_or_else(|| {
                        format!("eth{}", n.get("id").and_then(|v| v.as_i64()).unwrap_or(0))
                    }),
                    name: str_val(n, "interface_name"),
                    mac_address: str_val(n, "mac_address"),
                    speed_mbps: None,
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: Some(
                        if n.get("lan_enable").and_then(|v| v.as_i64()).unwrap_or(0) == 1 {
                            "Up".to_string()
                        } else {
                            "Down".to_string()
                        },
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
        with_session(creds, |session| async move {
            let sensors: Vec<serde_json::Value> = session
                .get_json("/api/sensors")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let temps: Vec<TemperatureReading> = sensors
                .iter()
                .filter(|s| {
                    str_val(s, "type").as_deref() == Some("temperature")
                        && s.get("accessible").and_then(|v| v.as_i64()).unwrap_or(999) == 0
                })
                .map(|s| TemperatureReading {
                    name: str_val(s, "name").unwrap_or_default(),
                    reading_celsius: f64_val(s, "reading"),
                    upper_threshold: f64_val(s, "higher_critical_threshold")
                        .or_else(|| f64_val(s, "higher_non_critical_threshold")),
                    status: Some(
                        if s.get("sensor_state").and_then(|v| v.as_i64()).unwrap_or(0) == 1 {
                            "ok".to_string()
                        } else {
                            "ns".to_string()
                        },
                    ),
                })
                .collect();

            let fans: Vec<FanReading> = sensors
                .iter()
                .filter(|s| str_val(s, "type").as_deref() == Some("fan"))
                .map(|s| FanReading {
                    name: str_val(s, "name").unwrap_or_default(),
                    reading_rpm: f64_val(s, "reading").map(|v| v as u32),
                    status: Some(
                        if s.get("sensor_state").and_then(|v| v.as_i64()).unwrap_or(0) == 1 {
                            "ok".to_string()
                        } else {
                            "ns".to_string()
                        },
                    ),
                })
                .collect();

            Ok(ThermalInfo {
                temperatures: temps,
                fans,
            })
        })
        .await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |session| async move {
            let sensors: Vec<serde_json::Value> = session
                .get_json("/api/sensors")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let mut total_watts: Option<f64> = None;
            let mut supplies: Vec<PowerSupplyInfo> = Vec::new();

            for s in &sensors {
                let name = str_val(s, "name").unwrap_or_default();
                let tp = str_val(s, "type").unwrap_or_default();
                let unit = str_val(s, "unit").unwrap_or_default();
                let reading = f64_val(s, "reading");

                if (tp == "power_supply" || unit == "W") && reading.is_some() {
                    let name_lower = name.to_lowercase();
                    if name_lower.contains("pin") || name_lower.contains("total") {
                        if total_watts.is_none() {
                            total_watts = reading;
                        }
                    } else if name_lower.contains("pout") || name_lower.contains("psu") {
                        supplies.push(PowerSupplyInfo {
                            id: name,
                            input_watts: None,
                            output_watts: reading,
                            capacity_watts: None,
                            serial_number: None,
                            firmware_version: None,
                            manufacturer: None,
                            model: None,
                            status: Some(
                                if s.get("sensor_state").and_then(|v| v.as_i64()).unwrap_or(0) == 1
                                {
                                    "ok".to_string()
                                } else {
                                    "ns".to_string()
                                },
                            ),
                        });
                    }
                }
            }

            Ok(PowerInfo {
                power_consumed_watts: total_watts,
                power_capacity_watts: None,
                current_cpu_power_watts: None,
                current_memory_power_watts: None,
                redundancy_mode: None,
                redundancy_health: None,
                power_supplies: supplies,
            })
        })
        .await
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        with_session(creds, |session| async move {
            let inventory: Vec<serde_json::Value> = session
                .get_json("/api/asrr/inventory_info")
                .await
                .and_then(|v| {
                    serde_json::from_value(v).map_err(|e| BmcError::internal(e.to_string()))
                })
                .unwrap_or_default();

            let mut devices: Vec<PCIeDeviceInfo> = Vec::new();
            for d in &inventory {
                let dt = str_val(d, "device_type").unwrap_or_default();
                if !dt.contains("PCIe") && !dt.contains("OCP") {
                    continue;
                }

                let product_name = str_val(d, "product_name").unwrap_or_default();
                let device_class = if product_name.contains("Ethernet") {
                    Some("NIC".to_string())
                } else if product_name.contains("SAS") || product_name.contains("RAID") {
                    Some("HBA".to_string())
                } else if product_name.contains("VGA") || product_name.contains("GPU") {
                    Some("GPU".to_string())
                } else if product_name.contains("NVMe") {
                    Some("NVMe".to_string())
                } else {
                    Some(product_name.clone())
                };

                devices.push(PCIeDeviceInfo {
                    id: str_val(d, "device_name").unwrap_or_default(),
                    slot: str_val(d, "product_asset_tag"),
                    name: str_val(d, "device_name"),
                    description: str_val(d, "description"),
                    manufacturer: str_val(d, "product_manufacturer_name"),
                    model: str_val(d, "product_name"),
                    device_class,
                    device_id: None,
                    vendor_id: None,
                    subsystem_id: None,
                    subsystem_vendor_id: None,
                    associated_resource: None,
                    position: None,
                    source_type: Some("inventory".to_string()),
                    serial_number: str_val(d, "product_serial_number"),
                    firmware_version: None,
                    link_width: None,
                    link_speed: None,
                    status: Some("Present".to_string()),
                    populated: true,
                });
            }
            Ok(devices)
        })
        .await
    }

    async fn get_event_logs(
        &self,
        _creds: &BmcCreds,
        _limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        Ok(vec![])
    }

    async fn clear_event_logs(&self, _creds: &BmcCreds) -> BmcResult<()> {
        warn!("ASRockRack SP-X: clear_event_logs not implemented");
        Ok(())
    }

    fn console_types(&self) -> Vec<ConsoleType> {
        vec![ConsoleType::Java, ConsoleType::Html5]
    }

    async fn get_kvm_console(
        &self,
        creds: &BmcCreds,
        console_type: &ConsoleType,
    ) -> BmcResult<KvmConsoleInfo> {
        // KVM 需要独立的 BMC 会话，不走共享池，防止被 bmc_collect 等后台任务 logout
        let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
        info!(
            "ASRockRack: dedicated KVM session created for {}:{}",
            creds.host, creds.port
        );

        let cookie_hdr = format!(
            "QSESSIONID={}; CSRFTOKEN={}",
            session.q_session_id, session.csrf_token
        );

        let cookies = vec![
            KvmCookie {
                name: "QSESSIONID".into(),
                value: session.q_session_id.clone(),
            },
            KvmCookie {
                name: "CSRFTOKEN".into(),
                value: session.csrf_token.clone(),
            },
        ];
        let csrf_header = Some("X-CSRFTOKEN".into());

        // 获取 BMC features (GET /api/configuration/project)
        let features_str = match session
            .client
            .get(format!("{}/api/configuration/project", session.base_url))
            .header("Cookie", &cookie_hdr)
            .header("X-CSRFTOKEN", &session.csrf_token)
            .send()
            .await
        {
            Ok(resp) => resp.text().await.unwrap_or_default(),
            Err(e) => {
                warn!("ASRockRack: failed to fetch features: {}", e);
                "[]".into()
            }
        };

        // 获取 KVM token (GET /api/kvm/token)
        // viewer JS 用这个做 WebSocket 连接认证
        let kvm_token_str = match session
            .client
            .get(format!("{}/api/kvm/token", session.base_url))
            .header("Cookie", &cookie_hdr)
            .header("X-CSRFTOKEN", &session.csrf_token)
            .send()
            .await
        {
            Ok(resp) => resp.text().await.unwrap_or_default(),
            Err(e) => {
                warn!("ASRockRack: failed to fetch kvm token: {}", e);
                String::new()
            }
        };
        debug!("ASRockRack: kvm token response: {}", kvm_token_str);

        // 获取 media redirection 设置
        // 从登录响应提取会话信息
        let login = &session.login_response;
        let privilege_id = login.get("privilege").and_then(|v| v.as_i64()).unwrap_or(4);
        let extended_priv = login.get("extendedpriv").and_then(|v| v.as_i64()).unwrap_or(259);
        let session_id = login.get("racsession_id").and_then(|v| v.as_i64()).unwrap_or(0);

        let privilege_name = match privilege_id {
            4 => "Administrator",
            3 => "Operator",
            2 => "User",
            _ => "Administrator",
        };

        let mut session_storage = vec![
            ("features".into(), features_str),
            ("garc".into(), session.csrf_token.clone()),
            ("privilege".into(), privilege_name.into()),
            ("privilege_id".into(), privilege_id.to_string()),
            ("extended_privilege".into(), extended_priv.to_string()),
            ("kvm_access".into(), "1".into()),
            ("vmedia_access".into(), "1".into()),
            ("username".into(), creds.username.clone()),
            ("server_addr".into(), creds.host.clone()),
            ("session_id".into(), session_id.to_string()),
            ("id".into(), login.get("racsession_id").and_then(|v| v.as_i64()).unwrap_or(0).to_string()),
        ];

        // token: viewer 从 /api/kvm/token 获取的 JSON（包含 client_ip, token, session）
        if !kvm_token_str.is_empty() && kvm_token_str.starts_with('{') {
            session_storage.push(("token".into(), kvm_token_str));
        } else {
            let fallback = serde_json::json!({
                "client_ip": "",
                "token": session.csrf_token,
                "session": session.q_session_id,
            })
            .to_string();
            session_storage.push(("token".into(), fallback));
        }

        match console_type {
            ConsoleType::Java => {
                let url = format!(
                    "{}/api/asrr/java-console",
                    session.base_url
                );
                debug!("ASRockRack: fetching JNLP from {}", url);
                let resp = session
                    .client
                    .get(&url)
                    .header("Cookie", &cookie_hdr)
                    .header("X-CSRFTOKEN", &session.csrf_token)
                    .send()
                    .await
                    .map_err(|e| {
                        BmcError::internal(format!("ASRockRack JNLP fetch failed: {}", e))
                    })?;

                let mut jnlp = resp.text().await.map_err(|e| {
                    BmcError::internal(format!("ASRockRack JNLP read failed: {}", e))
                })?;
                // #endregion

                if jnlp.contains("<%") {
                    warn!("ASRockRack: JNLP contains unprocessed template tags, processing manually");

                    jnlp = process_jnlp_template(
                        &jnlp,
                        &creds.host,
                        creds.port,
                        creds.use_tls,
                        &session.q_session_id,
                    );

                }
                Ok(KvmConsoleInfo {
                    console_type: ConsoleType::Java,
                    jnlp_content: Some(jnlp),
                    html5_path: None,
                    cookies,
                    csrf_header,
                    session_storage,
                    local_storage: vec![],
                    bmc_extra_ports: vec![],
                })
            }
            ConsoleType::Html5 => Ok(KvmConsoleInfo {
                console_type: ConsoleType::Html5,
                jnlp_content: None,
                html5_path: Some("/viewer.html".into()),
                cookies,
                csrf_header,
                session_storage,
                local_storage: vec![],
                bmc_extra_ports: vec![],
            }),
            _ => Err(BmcError::Unsupported(
                "ASRockRack: unsupported console type".into(),
            )),
        }
    }

    fn rewrite_jnlp_for_proxy(
        &self,
        jnlp: &str,
        proxy_host: &str,
        proxy_port: u16,
        codebase_url: &str,
        _port_map: &std::collections::HashMap<u16, u16>,
    ) -> String {
        let mut result = jnlp.to_string();

        if let Some(start) = result.find("codebase=\"") {
            let attr_start = start + "codebase=\"".len();
            if let Some(end_offset) = result[attr_start..].find('"') {
                let old_codebase = result[attr_start..attr_start + end_offset].to_string();
                result = result.replacen(
                    &format!("codebase=\"{}\"", old_codebase),
                    &format!("codebase=\"{}\"", codebase_url),
                    1,
                );
            }
        }

        result = set_argument(&result, "-hostname", proxy_host);
        result = set_argument(&result, "-kvmport", &proxy_port.to_string());
        result = set_argument(&result, "-webport", &proxy_port.to_string());

        result
    }
}

fn set_argument(xml: &str, arg_name: &str, new_val: &str) -> String {
    let name_tag = format!("<argument>{}</argument>", arg_name);

    let Some(name_pos) = xml.find(&name_tag) else {
        return xml.to_string();
    };

    let after_name = name_pos + name_tag.len();
    let rest = &xml[after_name..];

    let Some(next_arg_offset) = rest.find("<argument>") else {
        return xml.to_string();
    };

    let val_start = after_name + next_arg_offset + "<argument>".len();
    let Some(val_len) = xml[val_start..].find("</argument>") else {
        return xml.to_string();
    };

    let mut out = String::with_capacity(xml.len());
    out.push_str(&xml[..val_start]);
    out.push_str(new_val);
    out.push_str(&xml[val_start + val_len..]);
    out
}
