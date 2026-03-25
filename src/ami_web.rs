use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};

pub struct AmiWebProvider;

// ─── Global session pool (per-host async mutex) ─────────────────────────

#[derive(Clone)]
struct AmiSession {
    client: Client,
    base_url: String,
    cookie: String,
    csrf_token: String,
    username: String,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<AmiSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 180;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<AmiSession>>> {
    let mut pool = SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

async fn get_session(creds: &BmcCreds) -> BmcResult<AmiSession> {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref session) = *guard {
        if session.created_at.elapsed().as_secs() < SESSION_MAX_AGE_SECS {
            debug!(
                "AMI: reusing cached session for {} (age={}s)",
                key,
                session.created_at.elapsed().as_secs()
            );
            return Ok(session.clone());
        }
        debug!("AMI: session expired for {}, logging out and re-login", key);
        do_logout(session).await;
    }

    let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
    *guard = Some(session.clone());
    info!("AMI: new session created for {}", key);
    Ok(session)
}

async fn invalidate_session(creds: &BmcCreds) {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    if let Some(ref session) = *guard {
        do_logout(session).await;
        debug!("AMI: invalidated session for {}", key);
    }
    *guard = None;
}

async fn do_logout(session: &AmiSession) {
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
        Ok(_) => debug!("AMI: session logged out for {}", session.base_url),
        Err(e) => debug!("AMI: logout request failed (non-critical): {}", e),
    }
}

fn make_session_client() -> BmcResult<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .gzip(true)
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| BmcError::internal(format!("Failed to create HTTP client: {}", e)))
}

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<AmiSession> {
    let client = make_session_client()?;
    let url = format!("{}/rpc/WEBSES/create.asp", base_url);
    debug!("AMI Web login POST {}", url);

    let resp = client
        .post(&url)
        .form(&[("WEBVAR_USERNAME", username), ("WEBVAR_PASSWORD", password)])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("AMI login failed: {}", e)))?;

    debug!(
        "AMI login status={}, headers={:?}",
        resp.status(),
        resp.headers().clone()
    );

    let body = resp
        .text()
        .await
        .map_err(|e| BmcError::internal(format!("AMI login read body failed: {}", e)))?;

    debug!("AMI login response ({} bytes):\n{}", body.len(), body);

    let cookie = extract_field(&body, "SESSION_COOKIE")
        .filter(|v| v != "Failure_Session_Creation")
        .ok_or_else(|| {
            error!(
                "AMI login: failed to extract SessionCookie. Full response:\n{}",
                body
            );
            BmcError::internal("AMI login: failed to extract SessionCookie from response")
        })?;

    let csrf_token = extract_field(&body, "CSRFTOKEN").unwrap_or_default();

    debug!(
        "AMI Web login success, cookie={}, csrf={}",
        cookie, csrf_token
    );
    Ok(AmiSession {
        client,
        base_url: base_url.to_string(),
        cookie,
        csrf_token,
        username: username.to_string(),
        created_at: std::time::Instant::now(),
    })
}

async fn with_session<F, Fut, T>(creds: &BmcCreds, f: F) -> BmcResult<T>
where
    F: Fn(AmiSession) -> Fut + Send,
    Fut: std::future::Future<Output = BmcResult<T>> + Send,
{
    let session = get_session(creds).await?;
    match f(session).await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("session expired") || msg.contains("session_expired") {
                warn!("AMI session error, invalidating and retrying: {}", msg);
                invalidate_session(creds).await;
                let session = get_session(creds).await?;
                f(session).await
            } else {
                Err(e)
            }
        }
    }
}

fn extract_field(body: &str, field: &str) -> Option<String> {
    if let Some(pos) = body.find(field) {
        let after = &body[pos + field.len()..];
        let after =
            after.trim_start_matches(|c: char| c == '\'' || c == '"' || c == ' ' || c == ':');
        if let Some(end) = after.find(|c: char| c == '\'' || c == '"' || c == ',' || c == '}') {
            let val = after[..end].trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

impl AmiSession {
    async fn get(&self, path: &str) -> BmcResult<String> {
        let url = format!("{}{}", self.base_url, path);
        debug!(
            "AMI session GET {} (cookie={}, csrf={})",
            url,
            &self.cookie[..self.cookie.len().min(10)],
            self.csrf_token
        );

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
            .map_err(|e| BmcError::internal(format!("AMI GET {} failed: {}", path, e)))?;

        let body = resp
            .text()
            .await
            .map_err(|e| BmcError::internal(format!("AMI read body failed for {}: {}", path, e)))?;

        let truncated: String = body.chars().take(2000).collect();
        debug!(
            "AMI response for {} ({} bytes):\n{}",
            path,
            body.len(),
            truncated
        );

        if body.contains("session_expired") || body.contains("Session has been expired") {
            return Err(BmcError::internal("AMI BMC session expired"));
        }
        Ok(body)
    }

    async fn get_all_sensors(&self) -> BmcResult<Vec<serde_json::Value>> {
        let body = self.get("/rpc/getallsensors.asp").await?;
        parse_ami_json_response(&body)
    }

    async fn get_lan_cfg(&self) -> BmcResult<Vec<serde_json::Value>> {
        let body = self.get("/rpc/getalllancfg.asp").await?;
        parse_ami_json_response(&body)
    }
}

fn parse_ami_json_response(body: &str) -> BmcResult<Vec<serde_json::Value>> {
    // AMI responses wrap data in: WEBVAR_JSONVAR_GET_XXX = { WEBVAR_STRUCTNAME_XXX : [ {data}, {} ], HAPI_STATUS:0 };
    // Find the JSON array between [ and ]
    if let Some(start) = body.find('[') {
        if let Some(end) = body.rfind(']') {
            let json_str = &body[start..=end];
            // AMI uses single quotes for strings, convert to double quotes
            let json_str = json_str.replace('\'', "\"");
            debug!(
                "AMI JSON to parse ({} bytes): {}",
                json_str.len(),
                &json_str[..json_str.len().min(500)]
            );
            let arr: Vec<serde_json::Value> = serde_json::from_str(&json_str).map_err(|e| {
                error!(
                    "AMI JSON parse error: {}. Raw JSON (first 1000 chars): {}",
                    e,
                    &json_str[..json_str.len().min(1000)]
                );
                BmcError::internal(format!("AMI JSON parse error: {}", e))
            })?;
            let arr: Vec<serde_json::Value> = arr
                .into_iter()
                .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty())
                .collect();
            return Ok(arr);
        }
    }

    // Check if this is a session expired / login page
    if body.contains("session_expired") || body.contains("login") {
        error!("AMI session expired or not authenticated. Response contains login/session_expired page.");
        return Err(BmcError::internal(
            "AMI BMC session expired, authentication failed",
        ));
    }

    error!(
        "AMI response: no JSON array found. Body length={}. Last 500 chars: {}",
        body.len(),
        &body[body.len().saturating_sub(500)..]
    );
    Err(BmcError::internal("AMI response: no JSON array found"))
}

fn sensor_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(|x| {
            if x.is_string() {
                x.as_str().map(String::from)
            } else {
                Some(x.to_string())
            }
        })
        .filter(|s| !s.is_empty())
}

fn sensor_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| {
        if let Some(f) = x.as_f64() {
            Some(f)
        } else {
            x.as_str().and_then(|s| s.parse().ok())
        }
    })
}

fn sensor_name(v: &serde_json::Value) -> String {
    sensor_str(v, "SensorName")
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn sensor_unit2(v: &serde_json::Value) -> u32 {
    v.get("SensorUnit2").and_then(|x| x.as_u64()).unwrap_or(0) as u32
}

fn sensor_reading(v: &serde_json::Value) -> Option<f64> {
    // AMI: SensorReading = RawReading * 1000 for analog sensors
    // For fan RPM: SensorReading/1000 gives RPM (NOT RawReading)
    // For temp: RawReading is the direct °C value
    // For voltage/power/current: SensorReading/1000 is the actual value
    let unit2 = sensor_unit2(v);
    match unit2 {
        1 => {
            // Temperature in °C: use RawReading directly if non-zero, else SensorReading/1000
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
        18 => {
            // Fan RPM: SensorReading/1000 is the RPM
            v.get("SensorReading")
                .and_then(|x| x.as_f64())
                .map(|r| r / 1000.0)
        }
        _ => {
            // Voltage, current, power, etc: SensorReading/1000
            v.get("SensorReading")
                .and_then(|x| x.as_f64())
                .map(|r| r / 1000.0)
                .filter(|&v| v > 0.0)
        }
    }
}

fn sensor_status_ok(v: &serde_json::Value) -> bool {
    let accessible = v
        .get("SensorAccessibleFlags")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    // SensorAccessibleFlags=213 means "not present/not accessible"
    if accessible == 213 {
        return false;
    }
    let state = v.get("SensorState").and_then(|x| x.as_u64()).unwrap_or(0);
    state != 0
}

#[async_trait]
impl BmcProvider for AmiWebProvider {
    fn name(&self) -> &str {
        "AMI Web"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let session = get_session(creds).await?;
        let _ = session.get_all_sensors().await?;
        Ok(true)
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;
            for s in &sensors {
                let name = sensor_name(s);
                if name == "PWR_State" {
                    let discrete = s.get("DiscreteState").and_then(|x| x.as_u64()).unwrap_or(0);
                    let reading = s.get("SensorReading").and_then(|x| x.as_u64()).unwrap_or(0);
                    let bit = reading / 1000;
                    let on = (bit & 0x0001) != 0 || discrete == 111;
                    return Ok(if on {
                        "on".to_string()
                    } else {
                        "off".to_string()
                    });
                }
            }
            if !sensors.is_empty() {
                Ok("on".to_string())
            } else {
                Ok("unknown".to_string())
            }
        })
        .await
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        let _ = (creds, action);
        Err(BmcError::internal(
            "AMI Web provider does not support power actions. Use IPMI for power control.",
        ))
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;

            let mut cpu_count = 0u32;
            for s in &sensors {
                let name = sensor_name(s);
                if name.contains("_Stat") && name.starts_with("CPU") && sensor_status_ok(s) {
                    cpu_count += 1;
                }
            }

            let power_state = {
                let mut state = "Unknown".to_string();
                for s in &sensors {
                    if sensor_name(s) == "PWR_State" {
                        let reading = s.get("SensorReading").and_then(|x| x.as_u64()).unwrap_or(0);
                        let bit = reading / 1000;
                        state = if (bit & 0x0001) != 0 {
                            "on".to_string()
                        } else {
                            "off".to_string()
                        };
                        break;
                    }
                }
                state
            };

            Ok(SystemInfo {
                manufacturer: None,
                model: None,
                serial_number: None,
                bios_version: None,
                bmc_version: None,
                hostname: None,
                power_state: Some(power_state),
                total_cpu_count: if cpu_count > 0 { Some(cpu_count) } else { None },
                total_memory_gib: None,
            })
        })
        .await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;

            let mut cpus: std::collections::BTreeMap<String, ProcessorInfo> =
                std::collections::BTreeMap::new();

            for s in &sensors {
                let name = sensor_name(s);
                if name.starts_with("CPU") && name.contains("_Stat") {
                    let cpu_id = name.split('_').next().unwrap_or(&name).to_string();
                    let present = sensor_status_ok(s);
                    cpus.entry(cpu_id.clone()).or_insert_with(|| ProcessorInfo {
                        id: cpu_id.clone(),
                        socket: Some(cpu_id),
                        model: None,
                        manufacturer: None,
                        total_cores: None,
                        total_threads: None,
                        max_speed_mhz: None,
                        temperature_celsius: None,
                        status: Some(if present {
                            "Present".to_string()
                        } else {
                            "Absent".to_string()
                        }),
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
                if name.starts_with("CPU") && name.contains("_Temp") {
                    let cpu_id = name.split('_').next().unwrap_or(&name).to_string();
                    let temp = sensor_reading(s);
                    if let Some(entry) = cpus.get_mut(&cpu_id) {
                        entry.temperature_celsius = temp;
                    } else {
                        cpus.insert(
                            cpu_id.clone(),
                            ProcessorInfo {
                                id: cpu_id.clone(),
                                socket: Some(cpu_id),
                                model: None,
                                manufacturer: None,
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
                            },
                        );
                    }
                }
            }

            Ok(cpus.into_values().collect())
        })
        .await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;

            let mut dimms: Vec<MemoryInfo> = Vec::new();
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

            // Pass 1: _Stat sensors determine populated status (authoritative)
            for s in &sensors {
                let name = sensor_name(s);
                if !name.contains("DIMM") || !name.contains("_Stat") {
                    continue;
                }
                let slot_name = name.replace("_Stat", "");
                if seen.contains(&slot_name) {
                    continue;
                }

                let discrete = s.get("DiscreteState").and_then(|x| x.as_u64()).unwrap_or(0);
                let populated = discrete == 0x4080 || discrete > 0x0080;
                let (channel, slot_index) = parse_dimm_location(&slot_name);

                seen.insert(slot_name.clone());
                dimms.push(MemoryInfo {
                    id: slot_name.clone(),
                    capacity_gib: None,
                    memory_type: None,
                    speed_mhz: None,
                    manufacturer: None,
                    serial_number: None,
                    slot: Some(slot_name),
                    channel,
                    slot_index,
                    temperature_celsius: None,
                    populated,
                    status: Some(if populated {
                        "Present".to_string()
                    } else {
                        "Empty".to_string()
                    }),
                    part_number: None,
                    rank_count: None,
                    module_type: None,
                    data_width_bits: None,
                });
            }

            // Pass 2: _Temp sensors — update temperature if slot exists, create entry only if not seen
            for s in &sensors {
                let name = sensor_name(s);
                if !name.contains("DIMM") || !name.contains("_Temp") {
                    continue;
                }
                let slot_name = name.replace("_Temp", "");

                if let Some(dimm) = dimms
                    .iter_mut()
                    .find(|d| d.slot.as_deref() == Some(&slot_name))
                {
                    dimm.temperature_celsius = sensor_reading(s);
                } else if !seen.contains(&slot_name) {
                    let (channel, slot_index) = parse_dimm_location(&slot_name);
                    seen.insert(slot_name.clone());
                    dimms.push(MemoryInfo {
                        id: slot_name.clone(),
                        capacity_gib: None,
                        memory_type: None,
                        speed_mhz: None,
                        manufacturer: None,
                        serial_number: None,
                        slot: Some(slot_name),
                        channel,
                        slot_index,
                        temperature_celsius: sensor_reading(s),
                        populated: true,
                        status: Some("Present".to_string()),
                        part_number: None,
                        rank_count: None,
                        module_type: None,
                        data_width_bits: None,
                    });
                }
            }

            dimms.sort_by(|a, b| a.id.cmp(&b.id));
            Ok(dimms)
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
        with_session(creds, |session| async move {
            let lan_entries = session.get_lan_cfg().await.unwrap_or_default();

            let mut ifaces = Vec::new();
            for entry in &lan_entries {
                let mac = sensor_str(entry, "macAddress").unwrap_or_default();
                let ipv4 = sensor_str(entry, "v4IPAddr").unwrap_or_default();
                let channel = entry
                    .get("channelNum")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0);
                let enabled = entry.get("lanEnable").and_then(|x| x.as_u64()).unwrap_or(0) != 0;

                if !mac.is_empty() {
                    ifaces.push(NetworkInterfaceInfo {
                        id: format!("eth{}", channel),
                        name: Some(format!("BMC LAN{}", channel)),
                        mac_address: Some(mac),
                        ipv4_address: if !ipv4.is_empty() && ipv4 != "0.0.0.0" {
                            Some(ipv4)
                        } else {
                            None
                        },
                        link_status: Some(if enabled {
                            "Up".to_string()
                        } else {
                            "Down".to_string()
                        }),
                        speed_mbps: None,
                        speed_gbps: None,
                        port_max_speed: None,
                        manufacturer: None,
                        model: None,
                        slot: None,
                        associated_resource: None,
                        bdf: None,
                        position: None,
                    });
                }
            }
            Ok(ifaces)
        })
        .await
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;

            let mut temperatures = Vec::new();
            let mut fans = Vec::new();

            for s in &sensors {
                let name = sensor_name(s);
                let unit2 = sensor_unit2(s);

                if unit2 == 1 {
                    // Temperature sensor (°C)
                    let reading = sensor_reading(s);
                    // Only include sensors with valid readings or known accessible
                    let accessible = s
                        .get("SensorAccessibleFlags")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    if accessible == 213 {
                        continue;
                    } // not populated
                    let upper = s
                        .get("HighCTThresh")
                        .and_then(|x| x.as_f64())
                        .map(|v| v / 1000.0)
                        .filter(|&v| v > 0.0);
                    let status_str = if sensor_status_ok(s) { "ok" } else { "ns" };
                    temperatures.push(TemperatureReading {
                        name: name.clone(),
                        reading_celsius: reading,
                        upper_threshold: upper,
                        status: Some(status_str.to_string()),
                    });
                }
                if unit2 == 18 {
                    // Fan sensor (RPM = SensorReading/1000)
                    let reading = sensor_reading(s).map(|v| v as u32);
                    fans.push(FanReading {
                        name: name.clone(),
                        reading_rpm: reading,
                        status: Some(if sensor_status_ok(s) {
                            "ok".to_string()
                        } else {
                            "ns".to_string()
                        }),
                    });
                }
            }

            Ok(ThermalInfo { temperatures, fans })
        })
        .await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |session| async move {
            let sensors = session.get_all_sensors().await?;

            let mut total_watts: Option<f64> = None;
            let mut supplies: Vec<PowerSupplyInfo> = Vec::new();

            for s in &sensors {
                let name = sensor_name(s);
                let unit2 = sensor_unit2(s);
                let name_lower = name.to_lowercase();

                // unit2=6 → Watts
                if unit2 == 6 {
                    let reading = sensor_reading(s);
                    if name_lower.contains("total") {
                        total_watts = reading;
                    } else if name_lower.contains("psu") || name_lower.starts_with("ps") {
                        let psu_id = name.split('_').next().unwrap_or(&name).to_string();
                        if let Some(existing) = supplies.iter_mut().find(|p| p.id == psu_id) {
                            if name_lower.contains("out") {
                                existing.output_watts = reading;
                            }
                        } else if name_lower.contains("out") {
                            supplies.push(PowerSupplyInfo {
                                id: psu_id,
                                input_watts: None,
                                output_watts: reading,
                                capacity_watts: None,
                                serial_number: None,
                                firmware_version: None,
                                manufacturer: None,
                                model: None,
                                status: Some(if sensor_status_ok(s) {
                                    "ok".to_string()
                                } else {
                                    "ns".to_string()
                                }),
                            });
                        }
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
            let sensors = session.get_all_sensors().await?;

            let mut pcie_slots: Vec<PCIeDeviceInfo> = Vec::new();
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

            for s in &sensors {
                let name = sensor_name(s);
                let name_lower = name.to_lowercase();
                if !(name_lower.contains("pci")
                    || name_lower.starts_with("j1")
                    || name_lower.starts_with("j2"))
                {
                    continue;
                }
                if !name_lower.contains("temp") && !name_lower.contains("amb") {
                    continue;
                }

                let slot_name = name.split('_').next().unwrap_or(&name).to_string();
                if seen.contains(&slot_name) {
                    continue;
                }
                seen.insert(slot_name.clone());

                let has_reading = sensor_reading(s).is_some() && sensor_status_ok(s);
                pcie_slots.push(PCIeDeviceInfo {
                    id: slot_name.clone(),
                    slot: Some(slot_name),
                    name: None,
                    description: None,
                    manufacturer: None,
                    model: None,
                    device_class: None,
                    device_id: None,
                    vendor_id: None,
                    subsystem_id: None,
                    subsystem_vendor_id: None,
                    associated_resource: None,
                    position: None,
                    source_type: Some("sensor".to_string()),
                    serial_number: None,
                    firmware_version: None,
                    link_width: None,
                    link_speed: None,
                    status: Some(if has_reading {
                        "ok".to_string()
                    } else {
                        "ns".to_string()
                    }),
                    populated: has_reading,
                });
            }

            Ok(pcie_slots)
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
        warn!("AMI Web provider does not support clearing event logs");
        Ok(())
    }
}

fn parse_dimm_location(slot_name: &str) -> (Option<String>, Option<u32>) {
    // Parse patterns like "CPU0_DIMMA0", "CPU1_DIMMB2", etc.
    if let Some(dimm_part) = slot_name.split("DIMM").nth(1) {
        let channel = dimm_part
            .chars()
            .next()
            .filter(|c| c.is_ascii_uppercase())
            .map(|c| c.to_string());
        let index = dimm_part.chars().nth(1).and_then(|c| c.to_digit(10));
        return (channel, index);
    }
    (None, None)
}
