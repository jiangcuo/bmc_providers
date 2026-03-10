use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};

pub struct GenericRedfishProvider;

fn make_client() -> BmcResult<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .gzip(true)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| BmcError::internal(format!("Failed to create HTTP client: {}", e)))
}

// ─── Global Redfish session pool (per-host async mutex) ─────────────────

#[derive(Clone)]
pub(crate) struct CachedRedfishSession {
    token: Option<String>,
    session_uri: Option<String>,
    system_path: String,
    chassis_path: String,
    manager_path: String,
    storage_path: Option<String>,
    creds_base_url: String,
    created_at: std::time::Instant,
}

static REDFISH_SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<CachedRedfishSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const REDFISH_SESSION_MAX_AGE_SECS: u64 = 300;

fn redfish_pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<CachedRedfishSession>>> {
    let mut pool = REDFISH_SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

pub(crate) async fn get_redfish_session(creds: &BmcCreds) -> BmcResult<RedfishSession> {
    let key = redfish_pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref cached) = *guard {
        if cached.created_at.elapsed().as_secs() < REDFISH_SESSION_MAX_AGE_SECS {
            debug!(
                "Redfish: reusing cached session for {} (age={}s)",
                key,
                cached.created_at.elapsed().as_secs()
            );
            return Ok(RedfishSession {
                token: cached.token.clone(),
                session_uri: cached.session_uri.clone(),
                system_path: cached.system_path.clone(),
                chassis_path: cached.chassis_path.clone(),
                manager_path: cached.manager_path.clone(),
                storage_path: cached.storage_path.clone(),
            });
        }
        debug!(
            "Redfish: session expired for {}, deleting old and re-creating",
            key
        );
        delete_redfish_session(
            cached.creds_base_url.clone(),
            cached.token.clone(),
            cached.session_uri.clone(),
        )
        .await;
    }

    let session = create_redfish_session_internal(creds).await?;

    *guard = Some(CachedRedfishSession {
        token: session.token.clone(),
        session_uri: session.session_uri.clone(),
        system_path: session.system_path.clone(),
        chassis_path: session.chassis_path.clone(),
        manager_path: session.manager_path.clone(),
        storage_path: session.storage_path.clone(),
        creds_base_url: creds.base_url(),
        created_at: std::time::Instant::now(),
    });
    info!("Redfish: new session created for {}", key);
    Ok(session)
}

#[allow(dead_code)]
async fn invalidate_redfish_session(creds: &BmcCreds) {
    let key = redfish_pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    if let Some(ref old) = *guard {
        delete_redfish_session(
            old.creds_base_url.clone(),
            old.token.clone(),
            old.session_uri.clone(),
        )
        .await;
        debug!("Redfish: invalidated session for {}", key);
    }
    *guard = None;
}

/// DELETE a Redfish session to free up the BMC session slot
async fn delete_redfish_session(
    base_url: String,
    token: Option<String>,
    session_uri: Option<String>,
) {
    if let (Some(tok), Some(uri)) = (token, session_uri) {
        let client = match make_client() {
            Ok(c) => c,
            Err(_) => return,
        };
        let url = format!("{}{}", base_url, uri);
        match client
            .delete(&url)
            .header("X-Auth-Token", &tok)
            .send()
            .await
        {
            Ok(resp) => debug!("Redfish session DELETE {} => {}", url, resp.status()),
            Err(e) => warn!("Redfish session DELETE failed: {}", e),
        }
    }
}

/// Try session-based auth, returns (token, session_uri) for later DELETE
async fn get_auth_token(creds: &BmcCreds) -> (Option<String>, Option<String>) {
    let client = match make_client() {
        Ok(c) => c,
        Err(_) => return (None, None),
    };
    let url = format!(
        "{}{}/SessionService/Sessions",
        creds.base_url(),
        creds.base_path
    );
    let body = serde_json::json!({"UserName": creds.username, "Password": creds.password});

    match client.post(&url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 201 => {
            let token = resp
                .headers()
                .get("x-auth-token")
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            let location = resp
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            // Session URI from Location header or from response body @odata.id
            let session_uri = location.or_else(|| {
                resp.headers()
                    .get("odata-entityid")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from)
            });
            if token.is_some() {
                debug!(
                    "Redfish session auth success, token={}..., uri={:?}",
                    token.as_ref().map(|t| &t[..t.len().min(10)]).unwrap_or(""),
                    session_uri
                );
            }
            (token, session_uri)
        }
        Ok(resp) => {
            debug!("Redfish session auth failed: HTTP {}", resp.status());
            (None, None)
        }
        Err(e) => {
            debug!("Redfish session auth request failed: {}", e);
            (None, None)
        }
    }
}

#[allow(dead_code)]
async fn redfish_get(creds: &BmcCreds, path: &str) -> BmcResult<serde_json::Value> {
    redfish_get_with_token(creds, path, None).await
}

pub(crate) async fn redfish_get_with_token(
    creds: &BmcCreds,
    path: &str,
    token: Option<&str>,
) -> BmcResult<serde_json::Value> {
    let client = make_client()?;
    let url = creds.redfish_url(path);
    debug!("Redfish GET {}", url);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.header("X-Auth-Token", t);
    } else {
        req = req.basic_auth(&creds.username, Some(&creds.password));
    }

    let resp = req
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("Redfish request failed: {} url={}", e, url)))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        error!(
            "Redfish HTTP {} for {}: {}",
            status,
            url,
            &body[..body.len().min(500)]
        );
        return Err(BmcError::internal(format!(
            "Redfish error: HTTP {} url={}",
            status, url
        )));
    }

    let body = resp
        .text()
        .await
        .map_err(|e| BmcError::internal(format!("Redfish read body failed: {} url={}", e, url)))?;
    debug!("Redfish response from {} ({} bytes)", path, body.len());

    serde_json::from_str(&body).map_err(|e| {
        error!(
            "Redfish JSON parse error for {}: {}. Raw body (first 1000 chars): {}",
            url,
            e,
            &body[..body.len().min(1000)]
        );
        BmcError::internal(format!("Redfish parse error for {}: {}", url, e))
    })
}

/// Discover the first member ID from a Redfish collection (e.g. /Systems -> /Systems/Self or /Systems/1)
async fn discover_first_member(
    creds: &BmcCreds,
    collection_path: &str,
    token: Option<&str>,
) -> BmcResult<String> {
    let data = redfish_get_with_token(creds, collection_path, token).await?;
    if let Some(members) = data.get("Members").and_then(|m| m.as_array()) {
        if let Some(first) = members.first() {
            if let Some(id) = first.get("@odata.id").and_then(|u| u.as_str()) {
                return Ok(id.to_string());
            }
        }
    }
    Err(BmcError::internal(format!(
        "No members found in Redfish collection {}",
        collection_path
    )))
}

pub(crate) struct RedfishSession {
    pub(crate) token: Option<String>,
    pub(crate) session_uri: Option<String>,
    pub(crate) system_path: String,
    pub(crate) chassis_path: String,
    pub(crate) manager_path: String,
    pub(crate) storage_path: Option<String>,
}

pub(crate) async fn create_redfish_session_internal(creds: &BmcCreds) -> BmcResult<RedfishSession> {
    let (token, session_uri) = get_auth_token(creds).await;
    let tok = token.as_deref();

    let system_path =
        match discover_first_member(creds, &format!("{}/Systems", creds.base_path), tok).await {
            Ok(p) => p,
            Err(_) => format!("{}/Systems/1", creds.base_path),
        };
    let chassis_path =
        match discover_first_member(creds, &format!("{}/Chassis", creds.base_path), tok).await {
            Ok(p) => p,
            Err(_) => format!("{}/Chassis/1", creds.base_path),
        };
    let manager_path =
        match discover_first_member(creds, &format!("{}/Managers", creds.base_path), tok).await {
            Ok(p) => p,
            Err(_) => format!("{}/Managers/1", creds.base_path),
        };

    let storage_path = if let Ok(sys_data) = redfish_get_with_token(creds, &system_path, tok).await
    {
        sys_data
            .get("Storage")
            .and_then(|s| s.get("@odata.id"))
            .and_then(|v| v.as_str())
            .map(String::from)
    } else {
        None
    };

    debug!(
        "Redfish session: system={}, chassis={}, manager={}, storage={:?}, has_token={}",
        system_path,
        chassis_path,
        manager_path,
        storage_path,
        token.is_some()
    );

    Ok(RedfishSession {
        token,
        session_uri,
        system_path,
        chassis_path,
        manager_path,
        storage_path,
    })
}

pub(crate) async fn redfish_post_with_token(
    creds: &BmcCreds,
    path: &str,
    body: &serde_json::Value,
    token: Option<&str>,
) -> BmcResult<serde_json::Value> {
    let client = make_client()?;
    let url = creds.redfish_url(path);
    debug!("Redfish POST {}", url);
    let mut req = client.post(&url).json(body);
    if let Some(t) = token {
        req = req.header("X-Auth-Token", t);
    } else {
        req = req.basic_auth(&creds.username, Some(&creds.password));
    }

    let resp = req
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("Redfish POST failed: {} url={}", e, url)))?;

    if !resp.status().is_success() {
        let resp_body = resp.text().await.unwrap_or_default();
        error!(
            "Redfish POST HTTP error for {}: {}",
            url,
            &resp_body[..resp_body.len().min(500)]
        );
        return Err(BmcError::internal(format!(
            "Redfish POST error: url={}",
            url
        )));
    }

    Ok(resp
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|_| serde_json::json!({"status": "ok"})))
}

pub(crate) fn extract_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(String::from)
}

pub(crate) fn extract_u32(v: &serde_json::Value, key: &str) -> Option<u32> {
    v.get(key).and_then(|x| {
        x.as_u64()
            .map(|n| n as u32)
            .or_else(|| x.as_f64().map(|n| n as u32))
            .or_else(|| {
                x.as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(|n| n as u32)
            })
    })
}

pub(crate) fn extract_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

pub(crate) async fn get_collection_members_with_token(
    creds: &BmcCreds,
    path: &str,
    token: Option<&str>,
) -> BmcResult<Vec<serde_json::Value>> {
    let data = redfish_get_with_token(creds, path, token).await?;
    let members = data
        .get("Members")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();
    let mut results = Vec::new();
    for member in members {
        if let Some(uri) = member.get("@odata.id").and_then(|u| u.as_str()) {
            if let Ok(item) = redfish_get_with_token(creds, uri, token).await {
                results.push(item);
            }
        }
    }
    Ok(results)
}

#[async_trait]
impl BmcProvider for GenericRedfishProvider {
    fn name(&self) -> &str {
        "GenericRedfish"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        redfish_get_with_token(creds, &session.system_path, tok).await?;
        Ok(true)
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let data = redfish_get_with_token(creds, &session.system_path, tok).await?;
        Ok(extract_str(&data, "PowerState").unwrap_or_else(|| "Unknown".to_string()))
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        let reset_type = match action {
            "on" => "On",
            "off" => "ForceOff",
            "reset" => "ForceRestart",
            "graceful_shutdown" => "GracefulShutdown",
            _ => {
                return Err(BmcError::bad_request(format!(
                    "Unknown power action: {}",
                    action
                )))
            }
        };

        let session = get_redfish_session(creds).await?;
        let body = serde_json::json!({"ResetType": reset_type});
        let path = format!("{}/Actions/ComputerSystem.Reset", session.system_path);
        redfish_post_with_token(creds, &path, &body, session.token.as_deref()).await?;
        Ok(format!("Power action '{}' executed successfully", action))
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let sys = redfish_get_with_token(creds, &session.system_path, tok).await?;
        let mgr = redfish_get_with_token(creds, &session.manager_path, tok)
            .await
            .ok();
        let summary_memory_gib = sys
            .get("MemorySummary")
            .and_then(|m| extract_f64(m, "TotalSystemMemoryGiB"));

        let dimm_memory_gib = get_collection_members_with_token(
            creds,
            &format!("{}/Memory", session.system_path),
            tok,
        )
        .await
        .ok()
        .map(|items| {
            items
                .iter()
                .filter_map(|m| {
                    let cap_mib = extract_u32(m, "CapacityMiB").unwrap_or(0);
                    let status_state = m
                        .get("Status")
                        .and_then(|s| extract_str(s, "State"))
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    if cap_mib == 0 || status_state == "absent" {
                        None
                    } else {
                        Some(cap_mib as f64 / 1024.0)
                    }
                })
                .sum::<f64>()
        });

        let total_memory_gib = match (summary_memory_gib, dimm_memory_gib) {
            (Some(summary), Some(dimms)) if dimms > summary + 0.5 => Some(dimms),
            (Some(summary), _) => Some(summary),
            (None, Some(dimms)) if dimms > 0.0 => Some(dimms),
            _ => None,
        };

        Ok(SystemInfo {
            manufacturer: extract_str(&sys, "Manufacturer"),
            model: extract_str(&sys, "Model"),
            serial_number: extract_str(&sys, "SerialNumber"),
            bios_version: extract_str(&sys, "BiosVersion"),
            bmc_version: mgr.as_ref().and_then(|m| extract_str(m, "FirmwareVersion")),
            hostname: extract_str(&sys, "HostName"),
            power_state: extract_str(&sys, "PowerState"),
            total_cpu_count: sys
                .get("ProcessorSummary")
                .and_then(|p| extract_u32(p, "Count")),
            total_memory_gib,
        })
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let items = get_collection_members_with_token(
            creds,
            &format!("{}/Processors", session.system_path),
            tok,
        )
        .await?;
        Ok(items
            .iter()
            .map(|p| ProcessorInfo {
                id: extract_str(p, "Id").unwrap_or_default(),
                socket: extract_str(p, "Socket"),
                model: extract_str(p, "Model"),
                manufacturer: extract_str(p, "Manufacturer"),
                total_cores: extract_u32(p, "TotalCores"),
                total_threads: extract_u32(p, "TotalThreads"),
                max_speed_mhz: extract_u32(p, "MaxSpeedMHz"),
                temperature_celsius: p.get("ReadingCelsius").and_then(|v| v.as_f64()).or_else(
                    || {
                        p.get("OperatingSpeedRangeCelsius")
                            .and_then(|v| v.get("Reading"))
                            .and_then(|v| v.as_f64())
                    },
                ),
                status: p.get("Status").and_then(|s| extract_str(s, "Health")),
                architecture: extract_str(p, "ProcessorArchitecture"),
                frequency_mhz: extract_u32(p, "OperatingSpeedMHz"),
                l1_cache_kib: None,
                l2_cache_kib: None,
                l3_cache_kib: p
                    .get("TotalCaches")
                    .and_then(|c| c.as_array())
                    .and_then(|arr| {
                        arr.iter()
                            .find(|c| extract_str(c, "Level").as_deref() == Some("L3"))
                    })
                    .and_then(|c| extract_u32(c, "InstalledSizeKB")),
                serial_number: extract_str(p, "SerialNumber"),
                part_number: extract_str(p, "PartNumber"),
                instruction_set: extract_str(p, "InstructionSet"),
            })
            .collect())
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let items = get_collection_members_with_token(
            creds,
            &format!("{}/Memory", session.system_path),
            tok,
        )
        .await?;
        Ok(items
            .iter()
            .map(|m| {
                let cap_mib = extract_u32(m, "CapacityMiB").unwrap_or(0);
                let populated = cap_mib > 0;
                let status_state = m.get("Status").and_then(|s| extract_str(s, "State"));
                let is_absent = status_state
                    .as_ref()
                    .map_or(false, |s| s.to_lowercase() == "absent");
                MemoryInfo {
                    id: extract_str(m, "Id").unwrap_or_default(),
                    capacity_gib: if populated {
                        Some(cap_mib as f64 / 1024.0)
                    } else {
                        None
                    },
                    memory_type: extract_str(m, "MemoryDeviceType")
                        .or_else(|| extract_str(m, "MemoryType")),
                    speed_mhz: extract_u32(m, "OperatingSpeedMhz"),
                    manufacturer: extract_str(m, "Manufacturer"),
                    serial_number: extract_str(m, "SerialNumber"),
                    slot: extract_str(m, "DeviceLocator"),
                    channel: extract_str(m, "MemoryLocation").or_else(|| {
                        extract_str(m, "DeviceLocator").and_then(|loc| {
                            loc.chars()
                                .find(|c| c.is_ascii_uppercase() && *c >= 'A' && *c <= 'H')
                                .map(|c| c.to_string())
                        })
                    }),
                    slot_index: extract_str(m, "DeviceLocator")
                        .and_then(|loc| loc.chars().last().and_then(|c| c.to_digit(10))),
                    temperature_celsius: None,
                    populated: populated && !is_absent,
                    status: m.get("Status").and_then(|s| extract_str(s, "Health")),
                    part_number: extract_str(m, "PartNumber"),
                    rank_count: extract_u32(m, "RankCount"),
                    module_type: extract_str(m, "BaseModuleType"),
                    data_width_bits: extract_u32(m, "DataWidthBits"),
                }
            })
            .collect())
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let storage_collection = session
            .storage_path
            .clone()
            .unwrap_or_else(|| format!("{}/Storage", session.system_path));
        let controllers =
            get_collection_members_with_token(creds, &storage_collection, tok).await?;
        let mut drives = Vec::new();
        for ctrl in &controllers {
            let ctrl_name = ctrl
                .get("StorageControllers")
                .and_then(|sc| sc.as_array())
                .and_then(|arr| arr.first())
                .and_then(|c| extract_str(c, "Name"));
            if let Some(drv_list) = ctrl.get("Drives").and_then(|d| d.as_array()) {
                for drv_ref in drv_list {
                    if let Some(uri) = drv_ref.get("@odata.id").and_then(|u| u.as_str()) {
                        if let Ok(d) = redfish_get_with_token(creds, uri, tok).await {
                            let cap_bytes =
                                d.get("CapacityBytes").and_then(|c| c.as_u64()).unwrap_or(0);
                            drives.push(StorageInfo {
                                id: extract_str(&d, "Id").unwrap_or_default(),
                                name: extract_str(&d, "Name"),
                                capacity_gib: Some(cap_bytes as f64 / 1024.0 / 1024.0 / 1024.0),
                                media_type: extract_str(&d, "MediaType"),
                                protocol: extract_str(&d, "Protocol"),
                                manufacturer: extract_str(&d, "Manufacturer"),
                                model: extract_str(&d, "Model"),
                                serial_number: extract_str(&d, "SerialNumber"),
                                status: d.get("Status").and_then(|s| extract_str(s, "Health")),
                                firmware_version: extract_str(&d, "Revision"),
                                rotation_speed_rpm: extract_u32(&d, "RotationSpeedRPM"),
                                capable_speed_gbps: extract_f64(&d, "CapableSpeedGbs"),
                                negotiated_speed_gbps: extract_f64(&d, "NegotiatedSpeedGbs"),
                                failure_predicted: d
                                    .get("FailurePredicted")
                                    .and_then(|v| v.as_bool()),
                                predicted_media_life_left_percent: extract_u32(
                                    &d,
                                    "PredictedMediaLifeLeftPercent",
                                ),
                                hotspare_type: extract_str(&d, "HotspareType"),
                                temperature_celsius: None,
                                hours_powered_on: None,
                                slot_number: None,
                                form_factor: None,
                                firmware_status: None,
                                raid_level: None,
                                controller_name: ctrl_name.clone(),
                                rebuild_state: None,
                            });
                        }
                    }
                }
            }
        }
        Ok(drives)
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let items = get_collection_members_with_token(
            creds,
            &format!("{}/EthernetInterfaces", session.system_path),
            tok,
        )
        .await?;
        Ok(items
            .iter()
            .map(|n| NetworkInterfaceInfo {
                id: extract_str(n, "Id").unwrap_or_default(),
                name: extract_str(n, "Name"),
                mac_address: extract_str(n, "MACAddress"),
                speed_mbps: extract_u32(n, "SpeedMbps"),
                speed_gbps: extract_f64(n, "SpeedGbps"),
                port_max_speed: None,
                link_status: extract_str(n, "LinkStatus"),
                ipv4_address: n
                    .get("IPv4Addresses")
                    .and_then(|a| a.as_array()?.first()?.get("Address")?.as_str())
                    .map(String::from),
                manufacturer: extract_str(n, "Manufacturer"),
                model: extract_str(n, "Model"),
                slot: n.get("Location").and_then(|loc| extract_str(loc, "Info")),
                associated_resource: None,
                bdf: None,
                position: None,
            })
            .collect())
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let data = redfish_get_with_token(creds, &format!("{}/Thermal", session.chassis_path), tok)
            .await?;
        let temps = data
            .get("Temperatures")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let fans = data
            .get("Fans")
            .and_then(|f| f.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(ThermalInfo {
            temperatures: temps
                .iter()
                .map(|t| TemperatureReading {
                    name: extract_str(t, "Name").unwrap_or_default(),
                    reading_celsius: extract_f64(t, "ReadingCelsius"),
                    upper_threshold: extract_f64(t, "UpperThresholdCritical"),
                    status: t.get("Status").and_then(|s| extract_str(s, "Health")),
                })
                .collect(),
            fans: fans
                .iter()
                .map(|f| FanReading {
                    name: extract_str(f, "Name")
                        .unwrap_or_else(|| extract_str(f, "FanName").unwrap_or_default()),
                    reading_rpm: extract_u32(f, "Reading")
                        .or_else(|| extract_u32(f, "ReadingRPM"))
                        .or_else(|| extract_u32(f, "ReadingValue")),
                    status: f.get("Status").and_then(|s| extract_str(s, "Health")),
                })
                .collect(),
        })
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let data =
            redfish_get_with_token(creds, &format!("{}/Power", session.chassis_path), tok).await?;
        let consumed = data
            .get("PowerControl")
            .and_then(|pc| pc.as_array()?.first()?.get("PowerConsumedWatts")?.as_f64());
        let capacity = data
            .get("PowerControl")
            .and_then(|pc| pc.as_array()?.first()?.get("PowerCapacityWatts")?.as_f64());
        let power_control_huawei_oem = data
            .get("PowerControl")
            .and_then(|pc| pc.as_array())
            .and_then(|arr| arr.first())
            .and_then(|item| item.get("Oem"))
            .and_then(|o| o.get("Huawei"));
        let current_cpu_power = power_control_huawei_oem
            .and_then(|o| o.get("PowerMetricsExtended"))
            .and_then(|m| extract_f64(m, "CurrentCPUPowerWatts"))
            .or_else(|| {
                power_control_huawei_oem.and_then(|o| extract_f64(o, "CurrentCPUPowerWatts"))
            });
        let current_memory_power = power_control_huawei_oem
            .and_then(|o| o.get("PowerMetricsExtended"))
            .and_then(|m| extract_f64(m, "CurrentMemoryPowerWatts"))
            .or_else(|| {
                power_control_huawei_oem.and_then(|o| extract_f64(o, "CurrentMemoryPowerWatts"))
            });
        let redundancy_mode = data
            .get("Redundancy")
            .and_then(|r| r.as_array())
            .and_then(|arr| arr.iter().find_map(|item| extract_str(item, "Mode")));
        let redundancy_health = data
            .get("Redundancy")
            .and_then(|r| r.as_array())
            .and_then(|arr| {
                arr.iter()
                    .find_map(|item| item.get("Status").and_then(|s| extract_str(s, "Health")))
            });
        let supplies = data
            .get("PowerSupplies")
            .and_then(|ps| ps.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(PowerInfo {
            power_consumed_watts: consumed,
            power_capacity_watts: capacity,
            current_cpu_power_watts: current_cpu_power,
            current_memory_power_watts: current_memory_power,
            redundancy_mode,
            redundancy_health,
            power_supplies: supplies
                .iter()
                .map(|s| {
                    let huawei_oem = s.get("Oem").and_then(|o| o.get("Huawei"));
                    PowerSupplyInfo {
                        id: extract_str(s, "MemberId")
                            .or_else(|| extract_str(s, "Name"))
                            .unwrap_or_default(),
                        input_watts: extract_f64(s, "PowerInputWatts")
                            .or_else(|| huawei_oem.and_then(|h| extract_f64(h, "PowerInputWatts"))),
                        output_watts: extract_f64(s, "LastPowerOutputWatts")
                            .or_else(|| extract_f64(s, "PowerOutputWatts"))
                            .or_else(|| {
                                huawei_oem.and_then(|h| extract_f64(h, "PowerOutputWatts"))
                            }),
                        capacity_watts: extract_f64(s, "PowerCapacityWatts"),
                        serial_number: extract_str(s, "SerialNumber"),
                        firmware_version: extract_str(s, "FirmwareVersion"),
                        manufacturer: extract_str(s, "Manufacturer"),
                        model: extract_str(s, "Model"),
                        status: s.get("Status").and_then(|st| extract_str(st, "Health")),
                    }
                })
                .collect(),
        })
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        let mut results = Vec::new();

        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        if let Ok(items) = get_collection_members_with_token(
            creds,
            &format!("{}/PCIeDevices", session.system_path),
            tok,
        )
        .await
        {
            for d in &items {
                let funcs = d.get("PCIeFunctions").and_then(|f| f.as_array());
                let device_class = funcs
                    .and_then(|fs| {
                        fs.first().and_then(|f| {
                            if let Some(_uri) = f.get("@odata.id").and_then(|u| u.as_str()) {
                                None // will be resolved below
                            } else {
                                extract_str(f, "DeviceClass")
                            }
                        })
                    })
                    .or_else(|| extract_str(d, "DeviceClass"));

                results.push(PCIeDeviceInfo {
                    id: extract_str(d, "Id").unwrap_or_default(),
                    slot: extract_str(d, "Slot").or_else(|| {
                        d.get("Location").and_then(|loc| {
                            extract_str(loc, "PartLocation").or_else(|| extract_str(loc, "Info"))
                        })
                    }),
                    name: extract_str(d, "Name"),
                    description: extract_str(d, "Description"),
                    manufacturer: extract_str(d, "Manufacturer"),
                    model: extract_str(d, "Model"),
                    device_class: device_class.or_else(|| extract_str(d, "DeviceType")),
                    device_id: None,
                    vendor_id: None,
                    subsystem_id: None,
                    subsystem_vendor_id: None,
                    associated_resource: None,
                    position: d.get("Location").and_then(|loc| extract_str(loc, "Info")),
                    source_type: Some("pcie_device".to_string()),
                    serial_number: extract_str(d, "SerialNumber"),
                    firmware_version: extract_str(d, "FirmwareVersion"),
                    link_width: d.get("PCIeInterface").and_then(|p| {
                        extract_str(p, "LanesInUse")
                            .or_else(|| extract_u32(p, "LanesInUse").map(|v| format!("x{}", v)))
                    }),
                    link_speed: d.get("PCIeInterface").and_then(|p| {
                        extract_str(p, "PCIeType").or_else(|| extract_str(p, "MaxPCIeType"))
                    }),
                    status: d.get("Status").and_then(|s| extract_str(s, "Health")),
                    populated: true,
                });
            }
        }

        if let Ok(slots_data) =
            redfish_get_with_token(creds, &format!("{}/PCIeSlots", session.chassis_path), tok).await
        {
            if let Some(slots) = slots_data.get("Slots").and_then(|s| s.as_array()) {
                for (i, slot) in slots.iter().enumerate() {
                    let slot_name = extract_str(slot, "SlotType")
                        .map(|t| format!("Slot {} ({})", i + 1, t))
                        .unwrap_or_else(|| format!("Slot {}", i + 1));
                    let has_device = slot
                        .get("Links")
                        .and_then(|l| l.get("PCIeDevice"))
                        .and_then(|d| d.as_array())
                        .map_or(false, |a| !a.is_empty());
                    if !has_device {
                        results.push(PCIeDeviceInfo {
                            id: format!("slot_{}", i + 1),
                            slot: Some(slot_name),
                            name: None,
                            description: None,
                            manufacturer: None,
                            model: None,
                            device_class: extract_str(slot, "SlotType"),
                            device_id: None,
                            vendor_id: None,
                            subsystem_id: None,
                            subsystem_vendor_id: None,
                            associated_resource: None,
                            position: None,
                            source_type: Some("pcie_slot".to_string()),
                            serial_number: None,
                            firmware_version: None,
                            link_width: extract_str(slot, "Lanes")
                                .or_else(|| extract_u32(slot, "Lanes").map(|v| format!("x{}", v))),
                            link_speed: extract_str(slot, "PCIeType"),
                            status: slot.get("Status").and_then(|s| extract_str(s, "Health")),
                            populated: false,
                        });
                    }
                }
            }
        }

        Ok(results)
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let data = match redfish_get_with_token(
            creds,
            &format!("{}/LogServices/SEL/Entries", session.manager_path),
            tok,
        )
        .await
        {
            Ok(d) => d,
            Err(_) => {
                redfish_get_with_token(
                    creds,
                    &format!("{}/LogServices/Log1/Entries", session.manager_path),
                    tok,
                )
                .await?
            }
        };

        let members = data
            .get("Members")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();
        let limit = limit.unwrap_or(100) as usize;

        Ok(members
            .iter()
            .take(limit)
            .map(|e| EventLogEntry {
                id: extract_str(e, "Id").unwrap_or_default(),
                severity: extract_str(e, "Severity").or_else(|| extract_str(e, "EntryType")),
                message: extract_str(e, "Message"),
                created: extract_str(e, "Created"),
                entry_type: extract_str(e, "EntryType"),
                subject: None,
                suggestion: None,
                event_code: None,
                alert_status: None,
            })
            .collect())
    }

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()> {
        let session = get_redfish_session(creds).await?;
        let paths = [
            format!(
                "{}/LogServices/SEL/Actions/LogService.ClearLog",
                session.manager_path
            ),
            format!(
                "{}/LogServices/Log1/Actions/LogService.ClearLog",
                session.manager_path
            ),
        ];
        for path in &paths {
            if redfish_post_with_token(
                creds,
                path,
                &serde_json::json!({}),
                session.token.as_deref(),
            )
            .await
            .is_ok()
            {
                return Ok(());
            }
        }
        Err(BmcError::internal("Failed to clear event logs"))
    }
}
