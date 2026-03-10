// Sugon (曙光) BMC Web API Provider — AMI MegaRAC legacy firmware
//
// CRITICAL: AMI BMC only allows ~3 concurrent sessions total.
// We use a global session pool keyed by host, so all provider methods
// share ONE session per BMC host. Session is reused until it expires,
// then automatically re-created.

use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};

pub struct SugonProvider;

// ─── Global session pool (per-host async mutex) ─────────────────────────
//
// Outer StdMutex: quick HashMap lookup/insert (never held across .await)
// Inner AsyncMutex per host: serializes login so only ONE session is
// created at a time per BMC host, preventing session pool exhaustion.

#[derive(Clone)]
struct SugonSession {
    client: Client,
    base_url: String,
    cookie: String,
    csrf_token: String,
    username: String,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<SugonSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 180;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<SugonSession>>> {
    let mut pool = SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

async fn get_session(creds: &BmcCreds) -> BmcResult<SugonSession> {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref session) = *guard {
        if session.created_at.elapsed().as_secs() < SESSION_MAX_AGE_SECS {
            debug!(
                "Sugon: reusing cached session for {} (age={}s)",
                key,
                session.created_at.elapsed().as_secs()
            );
            return Ok(session.clone());
        }
        debug!(
            "Sugon: session expired for {} (age={}s), logging out and re-login",
            key,
            session.created_at.elapsed().as_secs()
        );
        do_logout(session).await;
    }

    let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
    *guard = Some(session.clone());
    info!("Sugon: new session created for {}", key);
    Ok(session)
}

async fn invalidate_session(creds: &BmcCreds) {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    if let Some(ref session) = *guard {
        do_logout(session).await;
        debug!("Sugon: invalidated session for {}", key);
    }
    *guard = None;
}

// ─── Login ──────────────────────────────────────────────────────────────

fn make_client() -> BmcResult<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .gzip(true)
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| BmcError::internal(format!("Failed to create HTTP client: {}", e)))
}

async fn do_logout(session: &SugonSession) {
    let url = format!("{}/rpc/WEBSES/logout.asp", session.base_url);
    match session
        .client
        .post(&url)
        .header(
            "Cookie",
            format!(
                "SessionCookie={}; Username={}; CSRFTOKEN={}",
                session.cookie, session.username, session.csrf_token
            ),
        )
        .header("CSRFTOKEN", &session.csrf_token)
        .send()
        .await
    {
        Ok(_) => debug!("Sugon: session logged out for {}", session.base_url),
        Err(e) => debug!("Sugon: logout request failed (non-critical): {}", e),
    }
}

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<SugonSession> {
    let client = make_client()?;
    let url = format!("{}/rpc/WEBSES/create.asp", base_url);
    debug!("Sugon login POST {}", url);

    let resp = client
        .post(&url)
        .form(&[("WEBVAR_USERNAME", username), ("WEBVAR_PASSWORD", password)])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("Sugon login request failed: {}", e)))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| BmcError::internal(format!("Sugon login read body: {}", e)))?;

    debug!("Sugon login status={} body_len={}", status, body.len());

    let cookie = extract_field(&body, "SESSION_COOKIE")
        .filter(|v| v != "Failure_Session_Creation" && !v.is_empty())
        .ok_or_else(|| {
            error!(
                "Sugon login failed (session pool full?). body_len={}",
                body.len()
            );
            BmcError::internal(
                "Sugon BMC login failed: session pool exhausted or credentials invalid",
            )
        })?;

    let csrf_token = extract_field(&body, "CSRFTOKEN").unwrap_or_default();
    debug!(
        "Sugon login success, cookie={:.10}..., csrf={}",
        cookie, csrf_token
    );

    Ok(SugonSession {
        client,
        base_url: base_url.to_string(),
        cookie,
        csrf_token,
        username: username.to_string(),
        created_at: std::time::Instant::now(),
    })
}

fn extract_field(body: &str, field: &str) -> Option<String> {
    let pos = body.find(field)?;
    let after = &body[pos + field.len()..];
    let after = after.trim_start_matches(|c: char| c == '\'' || c == '"' || c == ' ' || c == ':');
    let end = after.find(|c: char| c == '\'' || c == '"' || c == ',' || c == '}')?;
    let val = after[..end].trim();
    if val.is_empty() {
        None
    } else {
        Some(val.to_string())
    }
}

// ─── Session request helpers ────────────────────────────────────────────

impl SugonSession {
    fn is_session_error(body: &str) -> bool {
        let has_data = body.contains("HAPI_STATUS") || body.contains("WEBVAR_");
        !has_data && (body.contains("session_expired") || body.contains("login.html"))
    }

    async fn get(&self, path: &str) -> BmcResult<String> {
        let url = format!("{}{}", self.base_url, path);

        let resp = self
            .client
            .get(&url)
            .header(
                "Cookie",
                format!(
                    "SessionCookie={}; Username={}; CSRFTOKEN={}; test=1; PNO=4",
                    self.cookie, self.username, self.csrf_token
                ),
            )
            .header("CSRFTOKEN", &self.csrf_token)
            .header("X-CSRFTOKEN", &self.csrf_token)
            .send()
            .await
            .map_err(|e| BmcError::internal(format!("Sugon GET {} failed: {}", path, e)))?;

        let body = resp
            .text()
            .await
            .map_err(|e| BmcError::internal(format!("Sugon read body {}: {}", path, e)))?;

        if Self::is_session_error(&body) {
            return Err(BmcError::internal(format!(
                "Sugon session expired on {}",
                path
            )));
        }
        Ok(body)
    }

    async fn get_json(&self, path: &str) -> BmcResult<Vec<serde_json::Value>> {
        let body = self.get(path).await?;
        parse_ami_js_response(&body, path)
    }

    async fn fetch_dmi_info(&self) -> serde_json::Value {
        match self.get_json("/rpc/getalldmiinfo.asp").await {
            Ok(items) => items.into_iter().next().unwrap_or(serde_json::Value::Null),
            Err(e) => {
                warn!("Sugon getalldmiinfo failed: {}", e);
                serde_json::Value::Null
            }
        }
    }

    async fn fetch_cpu_info(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/sugon_get_cpu_info.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon cpu_info failed: {}", e);
                vec![]
            })
    }

    async fn fetch_mem_info(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/sugon_get_mem_info.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon mem_info failed: {}", e);
                vec![]
            })
    }

    async fn fetch_pci_info(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/sugon_get_pci_info.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon pci_info failed: {}", e);
                vec![]
            })
    }

    async fn fetch_psu_info(&self) -> serde_json::Value {
        match self.get_json("/rpc/sugon_get_psu_info.asp").await {
            Ok(items) => items.into_iter().next().unwrap_or(serde_json::Value::Null),
            Err(e) => {
                warn!("Sugon psu_info failed: {}", e);
                serde_json::Value::Null
            }
        }
    }

    async fn fetch_fan_info(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/sugon_get_fan_info.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon fan_info failed: {}", e);
                vec![]
            })
    }

    async fn fetch_nic_info(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/sugon_get_nic_info.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon nic_info failed: {}", e);
                vec![]
            })
    }

    async fn fetch_sensors(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/getallsensors.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon sensors failed: {}", e);
                vec![]
            })
    }

    async fn fetch_lan_cfg(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/getalllancfg.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon lan_cfg failed: {}", e);
                vec![]
            })
    }

    async fn fetch_sel_entries(&self) -> Vec<serde_json::Value> {
        self.get_json("/rpc/getallselentries.asp")
            .await
            .unwrap_or_else(|e| {
                warn!("Sugon sel_entries failed: {}", e);
                vec![]
            })
    }

    fn parse_power_state(sensors: &[serde_json::Value]) -> String {
        for s in sensors {
            if sensor_name(s) == "PWR_State" {
                let reading = s.get("SensorReading").and_then(|x| x.as_u64()).unwrap_or(0);
                let on = (reading / 1000) & 0x0001 != 0;
                return if on {
                    "on".to_string()
                } else {
                    "off".to_string()
                };
            }
        }
        if !sensors.is_empty() {
            "on".to_string()
        } else {
            "unknown".to_string()
        }
    }
}

async fn with_session<F, Fut, T>(creds: &BmcCreds, f: F) -> BmcResult<T>
where
    F: Fn(SugonSession) -> Fut + Send,
    Fut: std::future::Future<Output = BmcResult<T>> + Send,
{
    let session = get_session(creds).await?;
    match f(session).await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("session expired") || msg.contains("session_expired") {
                warn!("Sugon session error, invalidating and retrying: {}", msg);
                invalidate_session(creds).await;
                let session = get_session(creds).await?;
                f(session).await
            } else {
                Err(e)
            }
        }
    }
}

// ─── Parsing helpers ────────────────────────────────────────────────────

fn parse_ami_js_response(body: &str, path: &str) -> BmcResult<Vec<serde_json::Value>> {
    if let Some(start) = body.find('[') {
        if let Some(end) = body.rfind(']') {
            if end > start {
                let json_str = body[start..=end].replace('\'', "\"");
                match serde_json::from_str::<Vec<serde_json::Value>>(&json_str) {
                    Ok(arr) => {
                        return Ok(arr
                            .into_iter()
                            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty())
                            .collect());
                    }
                    Err(e) => {
                        error!("Sugon JSON parse error for {}: {}", path, e);
                        return Err(BmcError::internal(format!(
                            "Sugon JSON parse error for {}: {}",
                            path, e
                        )));
                    }
                }
            }
        }
    }
    warn!("Sugon: no JSON array found in response for {}", path);
    Ok(vec![])
}

fn jv_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        if x.is_string() {
            x.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
        } else {
            None
        }
    })
}
fn jv_u64(v: &serde_json::Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|x| x.as_u64())
}
fn jv_bool(v: &serde_json::Value, key: &str) -> bool {
    v.get(key).and_then(|x| x.as_u64()).unwrap_or(0) != 0
}

fn mem_freq_to_mhz(freq: u64) -> Option<u32> {
    match freq {
        1 => Some(800),
        2 => Some(1066),
        3 => Some(1333),
        4 => Some(1600),
        5 => Some(1866),
        6 => Some(2133),
        7 => Some(2400),
        8 => Some(2666),
        9 => Some(2933),
        10 => Some(3200),
        _ => None,
    }
}
fn mem_type_str(t: u64) -> Option<&'static str> {
    match t {
        3 => Some("DDR"),
        4 => Some("DDR4"),
        5 => Some("LPDDR4"),
        6 => Some("HBM"),
        7 => Some("DDR5"),
        _ => None,
    }
}

fn pci_vendor_name(id0: u64, id1: u64) -> Option<String> {
    let vid = (id1 << 8) | id0;
    match vid {
        0x8086 => Some("Intel".into()),
        0x1022 => Some("AMD".into()),
        0x10de => Some("NVIDIA".into()),
        0x1000 => Some("Broadcom/LSI".into()),
        0x14e4 => Some("Broadcom".into()),
        0x15b3 => Some("Mellanox".into()),
        _ if vid > 0 => Some(format!("0x{:04x}", vid)),
        _ => None,
    }
}

fn pci_base_class(base: u64, sub: u64) -> Option<String> {
    match base {
        1 => match sub {
            4 => Some("RAID Controller".into()),
            6 => Some("SATA Controller".into()),
            8 => Some("NVMe Controller".into()),
            _ => Some("Storage Controller".into()),
        },
        2 => Some("Ethernet Controller".into()),
        3 => Some("Display Controller".into()),
        12 => match sub {
            3 => Some("USB Controller".into()),
            _ => Some("Serial Bus Controller".into()),
        },
        _ if base > 0 => Some(format!("Class 0x{:02x}", base)),
        _ => None,
    }
}

fn sensor_unit2(v: &serde_json::Value) -> u32 {
    v.get("SensorUnit2").and_then(|x| x.as_u64()).unwrap_or(0) as u32
}
fn sensor_accessible(v: &serde_json::Value) -> bool {
    v.get("SensorAccessibleFlags")
        .and_then(|x| x.as_u64())
        .unwrap_or(0)
        != 213
}
fn sensor_reading_temp(v: &serde_json::Value) -> Option<f64> {
    let raw = v.get("RawReading").and_then(|x| x.as_f64()).unwrap_or(0.0);
    if raw > 0.0 {
        Some(raw)
    } else {
        v.get("SensorReading")
            .and_then(|x| x.as_f64())
            .map(|r| r / 1000.0)
            .filter(|&v| v > 0.0)
    }
}
fn sensor_reading_generic(v: &serde_json::Value) -> Option<f64> {
    match sensor_unit2(v) {
        1 => sensor_reading_temp(v),
        _ => v
            .get("SensorReading")
            .and_then(|x| x.as_f64())
            .map(|r| r / 1000.0)
            .filter(|&v| v > 0.0),
    }
}
fn sensor_name(v: &serde_json::Value) -> String {
    v.get("SensorName")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

// ─── BmcProvider trait impl ─────────────────────────────────────────────

#[async_trait]
impl BmcProvider for SugonProvider {
    fn name(&self) -> &str {
        "Sugon AMI (Legacy)"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let session = get_session(creds).await?;
        let dmi = session.fetch_dmi_info().await;
        Ok(!dmi.is_null())
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        with_session(creds, |s| async move {
            let sensors = s.fetch_sensors().await;
            Ok(SugonSession::parse_power_state(&sensors))
        })
        .await
    }

    async fn power_action(&self, _creds: &BmcCreds, _action: &str) -> BmcResult<String> {
        Err(BmcError::internal(
            "Sugon provider does not support power actions via Web API",
        ))
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        with_session(creds, |s| async move {
            let dmi = s.fetch_dmi_info().await;
            let cpus = s.fetch_cpu_info().await;
            let mems = s.fetch_mem_info().await;
            let sensors = s.fetch_sensors().await;

            let bios_version = jv_str(&dmi, "BIOSVERSION");
            let model = jv_str(&dmi, "SYSNAME").or_else(|| jv_str(&dmi, "MBNAME"));
            let serial_number = jv_str(&dmi, "SYSSN").or_else(|| jv_str(&dmi, "MBSN"));
            let manufacturer = jv_str(&dmi, "SYSVENDER")
                .filter(|s| !s.trim().is_empty())
                .or_else(|| jv_str(&dmi, "MBVENDER"));
            let cpu_count = cpus.iter().filter(|c| jv_bool(c, "cpuPresent")).count() as u32;
            let total_mem_gib: f64 = mems
                .iter()
                .filter(|m| jv_bool(m, "memPresent"))
                .map(|m| jv_u64(m, "memSize").unwrap_or(0) as f64)
                .sum();
            let power_state = Some(SugonSession::parse_power_state(&sensors));

            Ok(SystemInfo {
                manufacturer,
                model,
                serial_number,
                bios_version,
                bmc_version: None,
                hostname: None,
                power_state,
                total_cpu_count: if cpu_count > 0 { Some(cpu_count) } else { None },
                total_memory_gib: if total_mem_gib > 0.0 {
                    Some(total_mem_gib)
                } else {
                    None
                },
            })
        })
        .await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |s| async move {
            let cpus = s.fetch_cpu_info().await;
            let sensors = s.fetch_sensors().await;

            let mut cpu_temps: HashMap<String, f64> = HashMap::new();
            for sen in &sensors {
                let name = sensor_name(sen);
                if name.ends_with("_Temp") && name.starts_with("CPU") && !name.contains("DIMM") {
                    if let Some(key) = name.split('_').next() {
                        if let Some(t) = sensor_reading_temp(sen) {
                            if t > 0.0 {
                                cpu_temps.insert(key.to_string(), t);
                            }
                        }
                    }
                }
            }

            Ok(cpus
                .iter()
                .map(|cpu| {
                    let no = jv_u64(cpu, "CpuNo").unwrap_or(0);
                    let present = jv_bool(cpu, "cpuPresent");
                    let socket = format!("CPU{}", no);
                    ProcessorInfo {
                        id: socket.clone(),
                        socket: Some(socket.clone()),
                        model: jv_str(cpu, "cpuBrandName"),
                        manufacturer: None,
                        total_cores: None,
                        total_threads: None,
                        max_speed_mhz: None,
                        temperature_celsius: cpu_temps.get(&socket).copied(),
                        status: Some(if present {
                            "Present".into()
                        } else {
                            "Absent".into()
                        }),
                        architecture: None,
                        frequency_mhz: None,
                        l1_cache_kib: None,
                        l2_cache_kib: None,
                        l3_cache_kib: None,
                        serial_number: None,
                        part_number: None,
                        instruction_set: None,
                    }
                })
                .collect())
        })
        .await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |s| async move {
            let mems = s.fetch_mem_info().await;
            let sensors = s.fetch_sensors().await;

            let mut dimm_temps: HashMap<String, f64> = HashMap::new();
            for sen in &sensors {
                let name = sensor_name(sen);
                if name.contains("DIMM") && name.ends_with("_Temp") {
                    if let Some(t) = sensor_reading_temp(sen) {
                        if t > 0.0 {
                            dimm_temps.insert(name.replace("_Temp", ""), t);
                        }
                    }
                }
            }

            let mut result: Vec<MemoryInfo> = mems
                .iter()
                .enumerate()
                .map(|(idx, mem)| {
                    let node = jv_u64(mem, "nodeNo").unwrap_or(0);
                    let ch = jv_u64(mem, "channelNo").unwrap_or(0);
                    let dimm_no = jv_u64(mem, "dimmNo").unwrap_or(0);
                    let present = jv_bool(mem, "memPresent");
                    let ch_letter = (b'A' + (node * 4 + ch) as u8) as char;
                    let slot_name = format!("CPU{}_DIMM{}{}", node, ch_letter, dimm_no);
                    MemoryInfo {
                        id: format!("dimm_{}", idx),
                        capacity_gib: jv_u64(mem, "memSize").filter(|&v| v > 0).map(|v| v as f64),
                        memory_type: jv_u64(mem, "memType")
                            .and_then(mem_type_str)
                            .map(String::from),
                        speed_mhz: jv_u64(mem, "memFreq").and_then(mem_freq_to_mhz),
                        manufacturer: jv_str(mem, "memManufact"),
                        serial_number: jv_str(mem, "memPN"),
                        slot: Some(slot_name.clone()),
                        channel: Some(format!("{}", ch_letter)),
                        slot_index: Some(dimm_no as u32),
                        temperature_celsius: dimm_temps.get(&slot_name).copied(),
                        populated: present,
                        status: Some(if present {
                            "Present".into()
                        } else {
                            "Empty".into()
                        }),
                        part_number: None,
                        rank_count: None,
                        module_type: None,
                        data_width_bits: None,
                    }
                })
                .collect();
            result.sort_by(|a, b| a.slot.cmp(&b.slot));
            Ok(result)
        })
        .await
    }

    async fn get_storage(&self, _creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        Ok(vec![])
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        with_session(creds, |s| async move {
            let nics = s.fetch_nic_info().await;
            let lan_cfg = s.fetch_lan_cfg().await;
            let mut result = Vec::new();

            for nic in &nics {
                let no = jv_u64(nic, "onBNicNo").unwrap_or(0);
                if !jv_bool(nic, "onBNicStatus") && !jv_bool(nic, "biosSetFlags") {
                    continue;
                }
                let mac_bytes = [
                    jv_u64(nic, "onBNicMac0").unwrap_or(0),
                    jv_u64(nic, "onBNicMac1").unwrap_or(0),
                    jv_u64(nic, "onBNicMac2").unwrap_or(0),
                    jv_u64(nic, "onBNicMac3").unwrap_or(0),
                    jv_u64(nic, "onBNicMac4").unwrap_or(0),
                    jv_u64(nic, "onBNicMac5").unwrap_or(0),
                ];
                let mac = if mac_bytes.iter().any(|&b| b != 0) {
                    Some(format!(
                        "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                        mac_bytes[0],
                        mac_bytes[1],
                        mac_bytes[2],
                        mac_bytes[3],
                        mac_bytes[4],
                        mac_bytes[5]
                    ))
                } else {
                    None
                };
                result.push(NetworkInterfaceInfo {
                    id: format!("nic{}", no),
                    name: Some(format!("NIC{}", no)),
                    mac_address: mac,
                    speed_mbps: match jv_u64(nic, "onBNicLinkSpeed").unwrap_or(0) {
                        1 => Some(100),
                        2 => Some(1000),
                        3 => Some(10000),
                        4 => Some(25000),
                        _ => None,
                    },
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: Some(if jv_bool(nic, "onBNicLinkStatus") {
                        "Up".into()
                    } else {
                        "Down".into()
                    }),
                    ipv4_address: None,
                    manufacturer: None,
                    model: None,
                    slot: None,
                    associated_resource: None,
                    bdf: None,
                    position: None,
                });
            }
            for (i, lan) in lan_cfg.iter().enumerate() {
                let mac = jv_str(lan, "macAddress");
                let ip = jv_str(lan, "v4IPAddr").filter(|s| s != "0.0.0.0");
                if mac.is_some() || ip.is_some() {
                    result.push(NetworkInterfaceInfo {
                        id: format!("bmc_lan{}", i),
                        name: Some(format!("BMC LAN{}", i)),
                        mac_address: mac,
                        speed_mbps: None,
                        speed_gbps: None,
                        port_max_speed: None,
                        link_status: Some(if jv_u64(lan, "lanEnable").unwrap_or(0) != 0 {
                            "Up".into()
                        } else {
                            "Down".into()
                        }),
                        ipv4_address: ip,
                        manufacturer: None,
                        model: None,
                        slot: None,
                        associated_resource: None,
                        bdf: None,
                        position: None,
                    });
                }
            }
            Ok(result)
        })
        .await
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |s| async move {
            let sensors = s.fetch_sensors().await;
            let fans_raw = s.fetch_fan_info().await;
            let mut temperatures = Vec::new();
            let mut fans = Vec::new();

            for sen in &sensors {
                if !sensor_accessible(sen) {
                    continue;
                }
                let u2 = sensor_unit2(sen);
                if u2 == 1 {
                    temperatures.push(TemperatureReading {
                        name: sensor_name(sen),
                        reading_celsius: sensor_reading_temp(sen),
                        upper_threshold: sen
                            .get("HighCTThresh")
                            .and_then(|x| x.as_f64())
                            .map(|v| v / 1000.0)
                            .filter(|&v| v > 0.0),
                        status: Some("ok".into()),
                    });
                }
                if u2 == 18 {
                    fans.push(FanReading {
                        name: sensor_name(sen),
                        reading_rpm: sensor_reading_generic(sen).map(|v| v as u32),
                        status: Some("ok".into()),
                    });
                }
            }
            if fans.is_empty() {
                for (i, fan) in fans_raw.iter().enumerate() {
                    let speed = jv_u64(fan, "FanSpeed").unwrap_or(0);
                    fans.push(FanReading {
                        name: format!("FAN{}", i + 1),
                        reading_rpm: if speed > 0 {
                            Some((speed * 80) as u32)
                        } else {
                            None
                        },
                        status: Some("ok".into()),
                    });
                }
            }
            Ok(ThermalInfo { temperatures, fans })
        })
        .await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |s| async move {
            let psu = s.fetch_psu_info().await;
            let sensors = s.fetch_sensors().await;
            let mut total_watts: Option<f64> = None;
            let mut supplies: Vec<PowerSupplyInfo> = Vec::new();

            for sen in &sensors {
                if !sensor_accessible(sen) || sensor_unit2(sen) != 6 {
                    continue;
                }
                let name = sensor_name(sen);
                let reading = sensor_reading_generic(sen);
                let nl = name.to_lowercase();
                if nl.contains("total") {
                    total_watts = reading;
                } else if nl.starts_with("psu") && nl.contains("out") {
                    let id = name.split('_').next().unwrap_or(&name).to_string();
                    if let Some(e) = supplies.iter_mut().find(|p| p.id == id) {
                        e.output_watts = reading;
                    } else {
                        supplies.push(PowerSupplyInfo {
                            id,
                            input_watts: None,
                            output_watts: reading,
                            capacity_watts: None,
                            serial_number: None,
                            firmware_version: None,
                            manufacturer: None,
                            model: None,
                            status: Some("ok".into()),
                        });
                    }
                }
            }
            if supplies.is_empty() {
                for i in 1..=2u32 {
                    if jv_u64(&psu, &format!("PSU{}PRE", i)).unwrap_or(0) == 0 {
                        continue;
                    }
                    let vout = jv_u64(&psu, &format!("PSU{}RVOut", i)).map(|v| v as f64 / 10.0);
                    let iin = jv_u64(&psu, &format!("PSU{}RPIn", i)).map(|v| v as f64);
                    supplies.push(PowerSupplyInfo {
                        id: format!("PSU{}", i),
                        input_watts: None,
                        output_watts: vout.zip(iin).map(|(v, i)| v * i),
                        capacity_watts: None,
                        serial_number: None,
                        firmware_version: None,
                        manufacturer: None,
                        model: None,
                        status: Some("ok".into()),
                    });
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
        with_session(creds, |s| async move {
            let pci_list = s.fetch_pci_info().await;
            Ok(pci_list
                .iter()
                .filter_map(|pci| {
                    let present = jv_bool(pci, "pcieDevPresent");
                    if !present && !jv_bool(pci, "biosSetFlags") {
                        return None;
                    }
                    let no = jv_u64(pci, "pcieNo").unwrap_or(0);
                    let v0 = jv_u64(pci, "pcieVendorID0").unwrap_or(0);
                    let v1 = jv_u64(pci, "pcieVendorID1").unwrap_or(0);
                    let bc = jv_u64(pci, "pcieBaseClass").unwrap_or(0);
                    let sc = jv_u64(pci, "pcieSubClass").unwrap_or(0);
                    let bus = jv_u64(pci, "pcieBusNo").unwrap_or(0);
                    let dev = jv_u64(pci, "pcieDevNo").unwrap_or(0);
                    let func = jv_u64(pci, "pcieFunNo").unwrap_or(0);
                    let dc = pci_base_class(bc, sc);
                    Some(PCIeDeviceInfo {
                        id: format!("pcie{}", no),
                        slot: Some(format!(
                            "PCIe{} (CPU{})",
                            no,
                            jv_u64(pci, "cpuNo").unwrap_or(0)
                        )),
                        name: dc
                            .clone()
                            .map(|c| format!("{} @ {:02x}:{:02x}.{}", c, bus, dev, func)),
                        description: None,
                        manufacturer: pci_vendor_name(v0, v1),
                        model: None,
                        device_class: dc,
                        device_id: None,
                        vendor_id: None,
                        subsystem_id: None,
                        subsystem_vendor_id: None,
                        associated_resource: None,
                        position: None,
                        source_type: Some("pcie".into()),
                        serial_number: None,
                        firmware_version: None,
                        link_width: None,
                        link_speed: None,
                        status: Some(if present { "ok".into() } else { "empty".into() }),
                        populated: present,
                    })
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
            let entries = s.fetch_sel_entries().await;
            let max = limit.unwrap_or(50) as usize;
            Ok(entries
                .iter()
                .take(max)
                .map(|entry| {
                    let ts = jv_u64(entry, "TimeStamp").unwrap_or(0);
                    EventLogEntry {
                        id: format!("{}", jv_u64(entry, "RecordID").unwrap_or(0)),
                        severity: Some("Informational".into()),
                        message: Some(format!(
                            "{} (SensorType={})",
                            jv_str(entry, "SensorName").unwrap_or_else(|| "Unknown".into()),
                            jv_u64(entry, "SensorType").unwrap_or(0)
                        )),
                        created: if ts > 0 {
                            Some(
                                chrono::DateTime::from_timestamp(ts as i64, 0)
                                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                                    .unwrap_or_else(|| format!("{}", ts)),
                            )
                        } else {
                            None
                        },
                        entry_type: Some("SEL".into()),
                        subject: None,
                        suggestion: None,
                        event_code: None,
                        alert_status: None,
                    }
                })
                .collect())
        })
        .await
    }

    async fn clear_event_logs(&self, _creds: &BmcCreds) -> BmcResult<()> {
        warn!("Sugon provider: clear event logs not implemented");
        Ok(())
    }
}
