use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

pub struct HuananProvider;

#[derive(Clone)]
struct HuananSession {
    client: Client,
    base_url: String,
    csrf_token: String,
    q_session_id: String,
    login_response: serde_json::Value,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<HuananSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 240;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<HuananSession>>> {
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

async fn do_login(base_url: &str, username: &str, password: &str) -> BmcResult<HuananSession> {
    let client = make_client()?;
    let url = format!("{}/api/session", base_url);

    let resp = client
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[("username", username), ("password", password)])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("Huanan login failed: {}", e)))?;

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
        return Err(BmcError::internal("Huanan login: no QSESSIONID cookie"));
    }

    Ok(HuananSession {
        client,
        base_url: base_url.to_string(),
        csrf_token,
        q_session_id,
        login_response: body,
        created_at: std::time::Instant::now(),
    })
}

async fn do_logout(session: &HuananSession) {
    let url = format!("{}/api/session", session.base_url);
    let _ = session
        .client
        .delete(&url)
        .header("X-CSRFTOKEN", &session.csrf_token)
        .header("Cookie", format!("QSESSIONID={}", session.q_session_id))
        .send()
        .await;
}

async fn get_session(creds: &BmcCreds) -> BmcResult<HuananSession> {
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
    info!("Huanan: new session created for {}", key);
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
    F: Fn(HuananSession) -> Fut + Send,
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

impl HuananSession {
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
            .map_err(|e| BmcError::internal(format!("Huanan GET {} failed: {}", path, e)))?;
        if !resp.status().is_success() {
            return Err(BmcError::internal(format!(
                "Huanan GET {} returned {}",
                path,
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| BmcError::internal(format!("Huanan parse {} failed: {}", path, e)))
    }
}

fn str_val(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        if x.is_string() {
            x.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .and_then(|s| {
                    if s.eq_ignore_ascii_case("n/a") || s.chars().all(|c| c == '0') {
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

fn ts_to_iso(ts: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
}

fn threshold_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| {
        if x.is_string() && x.as_str() == Some("NA") {
            None
        } else {
            x.as_f64()
        }
    })
}

fn sensor_accessible(v: &serde_json::Value) -> bool {
    v.get("accessible").and_then(|x| x.as_u64()).unwrap_or(0) == 0
}

#[async_trait]
impl BmcProvider for HuananProvider {
    fn name(&self) -> &str {
        "Huanan BMC"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let session = get_session(creds).await?;
        let _ = session.get_json("/api/chassis-status").await?;
        Ok(true)
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
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

    async fn power_action(&self, _creds: &BmcCreds, _action: &str) -> BmcResult<String> {
        Err(BmcError::internal(
            "Huanan BMC does not support power actions via web API. Use IPMI for power control.",
        ))
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

            let ch = s
                .get_json("/api/chassis-status")
                .await
                .unwrap_or_default();
            let power_state = match ch.get("power_status").and_then(|v| v.as_i64()) {
                Some(1) => Some("on".to_string()),
                Some(0) => Some("off".to_string()),
                _ => None,
            };

            let sensors = s.get_json("/api/sensors").await.unwrap_or_default();
            let arr = sensors.as_array().cloned().unwrap_or_default();
            let cpu_count = arr
                .iter()
                .filter(|x| {
                    let name = str_val(x, "name").unwrap_or_default().to_lowercase();
                    let stype = str_val(x, "type").unwrap_or_default().to_lowercase();
                    stype == "processor"
                        && name.contains("presence")
                        && sensor_accessible(x)
                })
                .count() as u32;

            Ok(SystemInfo {
                manufacturer: board
                    .and_then(|b| str_val(b, "manufacturer"))
                    .or_else(|| product.and_then(|p| str_val(p, "manufacturer"))),
                model: board
                    .and_then(|b| str_val(b, "product_name"))
                    .or_else(|| product.and_then(|p| str_val(p, "product_name"))),
                serial_number: product
                    .and_then(|p| str_val(p, "serial_number"))
                    .or_else(|| board.and_then(|b| str_val(b, "serial_number"))),
                bios_version: None,
                bmc_version: product.and_then(|p| str_val(p, "product_version")),
                hostname: None,
                power_state,
                total_cpu_count: if cpu_count > 0 { Some(cpu_count) } else { None },
                total_memory_gib: None,
            })
        })
        .await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |s| async move {
            let sensors = s.get_json("/api/sensors").await?;
            let arr = sensors.as_array().cloned().unwrap_or_default();

            let mut cpus: std::collections::BTreeMap<String, ProcessorInfo> =
                std::collections::BTreeMap::new();

            for item in &arr {
                let name = str_val(item, "name").unwrap_or_default();
                let stype = str_val(item, "type").unwrap_or_default().to_lowercase();
                let name_lower = name.to_lowercase();

                if stype == "temperature" && name_lower.starts_with("cpu") && name_lower.contains("temp") {
                    let cpu_id = name.split('_').next().unwrap_or(&name).to_string();
                    let reading = f64_val(item, "reading").or_else(|| f64_val(item, "raw_reading"));
                    let temp = reading.filter(|&v| v > 0.0 && sensor_accessible(item));
                    cpus.entry(cpu_id.clone())
                        .and_modify(|e| e.temperature_celsius = temp)
                        .or_insert_with(|| ProcessorInfo {
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
                        });
                }
            }

            if cpus.is_empty() {
                let presence_count = arr
                    .iter()
                    .filter(|x| {
                        let stype = str_val(x, "type").unwrap_or_default().to_lowercase();
                        let name = str_val(x, "name").unwrap_or_default().to_lowercase();
                        stype == "processor" && name.contains("presence") && sensor_accessible(x)
                    })
                    .count();
                for i in 0..presence_count {
                    let cpu_id = format!("CPU{}", i);
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
                            temperature_celsius: None,
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

            Ok(cpus.into_values().collect())
        })
        .await
    }

    async fn get_memory(&self, _creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        Ok(vec![])
    }

    async fn get_storage(&self, _creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        Ok(vec![])
    }

    async fn get_network_interfaces(
        &self,
        _creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        Ok(vec![])
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |s| async move {
            let sensors = s.get_json("/api/sensors").await?;
            let arr = sensors.as_array().cloned().unwrap_or_default();

            let temperatures = arr
                .iter()
                .filter(|x| {
                    str_val(x, "type").unwrap_or_default().to_lowercase() == "temperature"
                        && sensor_accessible(x)
                })
                .map(|x| TemperatureReading {
                    name: str_val(x, "name").unwrap_or_default(),
                    reading_celsius: f64_val(x, "reading")
                        .or_else(|| f64_val(x, "raw_reading"))
                        .filter(|&v| v > 0.0),
                    upper_threshold: threshold_f64(x, "higher_critical_threshold")
                        .or_else(|| threshold_f64(x, "higher_non_critical_threshold")),
                    status: Some(
                        if i64_val(x, "sensor_state").unwrap_or(0) == 1 {
                            "ok".to_string()
                        } else {
                            "warning".to_string()
                        },
                    ),
                })
                .collect::<Vec<_>>();

            let fans = arr
                .iter()
                .filter(|x| {
                    let stype = str_val(x, "type").unwrap_or_default().to_lowercase();
                    let unit = str_val(x, "unit").unwrap_or_default().to_lowercase();
                    stype == "fan" && unit == "rpm" && sensor_accessible(x)
                })
                .map(|x| FanReading {
                    name: str_val(x, "name").unwrap_or_default(),
                    reading_rpm: u32_val(x, "reading").or_else(|| u32_val(x, "raw_reading")),
                    status: Some(
                        if i64_val(x, "sensor_state").unwrap_or(0) == 1 {
                            "ok".to_string()
                        } else {
                            "warning".to_string()
                        },
                    ),
                })
                .collect::<Vec<_>>();

            Ok(ThermalInfo { temperatures, fans })
        })
        .await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |s| async move {
            let sensors = s.get_json("/api/sensors").await?;
            let arr = sensors.as_array().cloned().unwrap_or_default();

            let mut supplies: Vec<PowerSupplyInfo> = Vec::new();

            for item in &arr {
                let stype = str_val(item, "type").unwrap_or_default().to_lowercase();
                let name = str_val(item, "name").unwrap_or_default();
                let unit = str_val(item, "unit").unwrap_or_default().to_lowercase();

                if stype == "power_supply" && unit == "watts" {
                    let reading = f64_val(item, "reading").or_else(|| f64_val(item, "raw_reading"));
                    let psu_id = name.split('_').take(2).collect::<Vec<_>>().join("_");
                    if let Some(existing) = supplies.iter_mut().find(|p| p.id == psu_id) {
                        if existing.input_watts.is_none() {
                            existing.input_watts = reading;
                        }
                    } else {
                        supplies.push(PowerSupplyInfo {
                            id: psu_id,
                            input_watts: reading,
                            output_watts: None,
                            capacity_watts: None,
                            serial_number: None,
                            firmware_version: None,
                            manufacturer: None,
                            model: None,
                            status: Some(
                                if i64_val(item, "sensor_state").unwrap_or(0) == 1 {
                                    "ok".to_string()
                                } else {
                                    "warning".to_string()
                                },
                            ),
                        });
                    }
                }
            }

            let total_watts = supplies
                .iter()
                .filter_map(|p| p.input_watts)
                .reduce(|a, b| a + b);

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

    async fn get_pcie_devices(&self, _creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        Ok(vec![])
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
                    format!(
                        "{} | {} | {} | {}",
                        sensor_name, event_desc, direction, advanced
                    )
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
        warn!("Huanan clear_event_logs not implemented");
        Ok(())
    }

    fn console_types(&self) -> Vec<ConsoleType> {
        vec![ConsoleType::Html5]
    }

    async fn get_kvm_console(
        &self,
        creds: &BmcCreds,
        _console_type: &ConsoleType,
    ) -> BmcResult<KvmConsoleInfo> {
        let session = do_login(&creds.base_url(), &creds.username, &creds.password).await?;
        info!(
            "Huanan: dedicated KVM session created for {}:{}",
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
                warn!("Huanan: failed to fetch features: {}", e);
                "[]".into()
            }
        };

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
                warn!("Huanan: failed to fetch kvm token: {}", e);
                String::new()
            }
        };
        debug!("Huanan: kvm token response: {}", kvm_token_str);

        let login = &session.login_response;
        let privilege_id = login.get("privilege").and_then(|v| v.as_i64()).unwrap_or(4);
        let extended_priv = login
            .get("extendedpriv")
            .and_then(|v| v.as_i64())
            .unwrap_or(259);
        let session_id = login
            .get("racsession_id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

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
            ("id".into(), session_id.to_string()),
        ];

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

        Ok(KvmConsoleInfo {
            console_type: ConsoleType::Html5,
            jnlp_content: None,
            html5_path: Some("/viewer.html".into()),
            cookies,
            csrf_header,
            session_storage,
            local_storage: vec![],
            bmc_extra_ports: vec![],
        })
    }
}
