use async_trait::async_trait;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;

pub struct HuaweiImcOldProvider;

#[derive(Clone)]
struct ImcOldSession {
    client: Client,
    base_url: String,
    token: String,
    created_at: std::time::Instant,
}

static SESSION_POOL: std::sync::LazyLock<
    StdMutex<HashMap<String, Arc<AsyncMutex<Option<ImcOldSession>>>>>,
> = std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const SESSION_MAX_AGE_SECS: u64 = 180;

fn pool_key(creds: &BmcCreds) -> String {
    format!("{}:{}", creds.host, creds.port)
}

fn get_slot(key: &str) -> Arc<AsyncMutex<Option<ImcOldSession>>> {
    let mut pool = SESSION_POOL.lock().unwrap();
    pool.entry(key.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

fn normalize_text(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            return None;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper == "N/A" || upper == "UNKNOWN" || upper == "NO DIMM" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn parse_u32_from_text(value: Option<String>) -> Option<u32> {
    let raw = value?;
    let digits: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    digits.parse::<u32>().ok()
}

fn parse_f64_from_text(value: Option<String>) -> Option<f64> {
    let raw = value?;
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            out.push(ch);
        } else if !out.is_empty() {
            break;
        }
    }
    out.parse::<f64>().ok()
}

fn status_from_health_code(code: Option<i64>) -> Option<String> {
    match code {
        Some(0) => Some("OK".to_string()),
        Some(1) | Some(2) => Some("Warning".to_string()),
        Some(_) => Some("Critical".to_string()),
        None => None,
    }
}

fn get_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        if let Some(s) = x.as_str() {
            Some(s.to_string())
        } else if x.is_number() || x.is_boolean() {
            Some(x.to_string())
        } else {
            None
        }
    })
}

fn get_i64(v: &serde_json::Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64()).or_else(|| {
        v.get(key)
            .and_then(|x| x.as_str())
            .and_then(|s| s.trim().parse::<i64>().ok())
    })
}

fn get_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64()).or_else(|| {
        v.get(key)
            .and_then(|x| x.as_str())
            .and_then(|s| s.trim().parse::<f64>().ok())
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'%' && idx + 2 < bytes.len() {
            let hex = &input[idx + 1..idx + 3];
            if let Ok(v) = u8::from_str_radix(hex, 16) {
                out.push(v);
                idx += 3;
                continue;
            }
        }
        out.push(bytes[idx]);
        idx += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn parse_legacy_json(raw: &str) -> BmcResult<serde_json::Value> {
    let mut decoded = percent_decode(raw.trim());
    if decoded.ends_with('%') {
        decoded.pop();
    }
    let trimmed = decoded.trim();
    serde_json::from_str(trimmed)
        .map_err(|e| BmcError::internal(format!("huawei_imc_old parse json failed: {}", e)))
}

fn dimm_channel_slot(name: &str) -> (Option<String>, Option<u32>) {
    let digits: String = name.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 2 {
        return (None, None);
    }
    let channel = digits
        .chars()
        .nth(digits.len().saturating_sub(2))
        .map(|c| c.to_string());
    let slot = digits
        .chars()
        .nth(digits.len().saturating_sub(1))
        .and_then(|c| c.to_digit(10));
    (channel, slot)
}

fn parse_core_thread(value: Option<String>) -> (Option<u32>, Option<u32>) {
    let raw = value.unwrap_or_default();
    let mut parts = raw.split('/');
    let core = parts.next().and_then(|v| v.trim().parse::<u32>().ok());
    let thread = parts.next().and_then(|v| v.trim().parse::<u32>().ok());
    (core, thread)
}

impl ImcOldSession {
    async fn query_multiproperty(&self, str_input: &str) -> BmcResult<serde_json::Value> {
        let url = format!("{}/bmc/php/getmultiproperty.php", self.base_url);
        let body = self
            .client
            .post(&url)
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=UTF-8",
            )
            .header("X-Requested-With", "XMLHttpRequest")
            .header("Origin", &self.base_url)
            .header("Referer", format!("{}/", self.base_url))
            .form(&[("token", self.token.as_str()), ("str_input", str_input)])
            .send()
            .await
            .map_err(|e| BmcError::internal(format!("huawei_imc_old query failed: {}", e)))?;

        let text = body
            .text()
            .await
            .map_err(|e| BmcError::internal(format!("huawei_imc_old query body failed: {}", e)))?;

        parse_legacy_json(&text)
    }
}

fn make_client() -> BmcResult<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .gzip(true)
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| BmcError::internal(format!("huawei_imc_old create client failed: {}", e)))
}

async fn do_login(creds: &BmcCreds) -> BmcResult<ImcOldSession> {
    let client = make_client()?;
    let base_url = creds.base_url();

    let login_url = format!("{}/bmc/php/processparameter.php", base_url);
    let login_resp = client
        .post(&login_url)
        .header(
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        )
        .header("X-Requested-With", "XMLHttpRequest")
        .header("Origin", &base_url)
        .header("Referer", format!("{}/", base_url))
        .form(&[
            ("check_pwd", creds.password.as_str()),
            ("logtype", "0"),
            ("user_name", creds.username.as_str()),
            ("func", "AddSession"),
            ("IsKvmApp", "0"),
        ])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("huawei_imc_old login failed: {}", e)))?;

    if !login_resp.status().is_success() {
        return Err(BmcError::internal(format!(
            "huawei_imc_old login http status {}",
            login_resp.status()
        )));
    }

    let token_url = format!("{}/bmc/php/gettoken.php", base_url);
    let token_resp = client
        .post(&token_url)
        .header(
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        )
        .header("X-Requested-With", "XMLHttpRequest")
        .header("Origin", &base_url)
        .header("Referer", format!("{}/", base_url))
        .form(&[("", "")])
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("huawei_imc_old get token failed: {}", e)))?;

    if !token_resp.status().is_success() {
        return Err(BmcError::internal(format!(
            "huawei_imc_old gettoken http status {}",
            token_resp.status()
        )));
    }

    let token = token_resp
        .text()
        .await
        .map_err(|e| {
            BmcError::internal(format!("huawei_imc_old gettoken read body failed: {}", e))
        })?
        .trim()
        .to_string();

    if token.is_empty() {
        return Err(BmcError::internal("huawei_imc_old token is empty"));
    }

    Ok(ImcOldSession {
        client,
        base_url,
        token,
        created_at: std::time::Instant::now(),
    })
}

async fn get_session(creds: &BmcCreds) -> BmcResult<ImcOldSession> {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;

    if let Some(ref session) = *guard {
        if session.created_at.elapsed().as_secs() < SESSION_MAX_AGE_SECS {
            return Ok(session.clone());
        }
    }

    let session = do_login(creds).await?;
    *guard = Some(session.clone());
    Ok(session)
}

async fn invalidate_session(creds: &BmcCreds) {
    let key = pool_key(creds);
    let slot = get_slot(&key);
    let mut guard = slot.lock().await;
    *guard = None;
}

async fn with_session<F, Fut, T>(creds: &BmcCreds, f: F) -> BmcResult<T>
where
    F: Fn(ImcOldSession) -> Fut + Send,
    Fut: std::future::Future<Output = BmcResult<T>> + Send,
{
    let session = get_session(creds).await?;
    match f(session).await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("{}", e).to_ascii_lowercase();
            if msg.contains("403") || msg.contains("token") || msg.contains("session") {
                invalidate_session(creds).await;
                let session = get_session(creds).await?;
                f(session).await
            } else {
                Err(e)
            }
        }
    }
}

#[async_trait]
impl BmcProvider for HuaweiImcOldProvider {
    fn name(&self) -> &str {
        "Huawei iMC-old"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"BMC","obj_name":"BMC","property_list":["SystemName","HostName"]}]"#;
            let _ = session.query_multiproperty(str_input).await?;
            Ok(true)
        }).await
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"Payload","obj_name":"ChassisPayload","property_list":["ChassisPowerState"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let state = json
                .get("Payload")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|obj| get_i64(obj, "ChassisPowerState"));
            Ok(match state {
                Some(1) => "On".to_string(),
                Some(0) => "Off".to_string(),
                _ => "Unknown".to_string(),
            })
        }).await
    }

    async fn power_action(&self, _creds: &BmcCreds, _action: &str) -> BmcResult<String> {
        Err(BmcError::internal(
            "Huawei iMC-old provider power action is not enabled yet",
        ))
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"BMC","obj_name":"BMC","property_list":["SystemName","HostName"]},{"class_name":"DNSSetting","obj_name":"DNSSetting","property_list":["DomainName"]},{"class_name":"Payload","obj_name":"ChassisPayload","property_list":["ChassisPowerState"]}]"#;
            let json = session.query_multiproperty(str_input).await?;

            let bmc_obj = json
                .get("BMC")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .cloned()
                .unwrap_or_default();
            let payload_obj = json
                .get("Payload")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .cloned()
                .unwrap_or_default();

            let power_state = match get_i64(&payload_obj, "ChassisPowerState") {
                Some(1) => Some("On".to_string()),
                Some(0) => Some("Off".to_string()),
                _ => Some("Unknown".to_string()),
            };

            Ok(SystemInfo {
                manufacturer: Some("Huawei".to_string()),
                model: normalize_text(get_str(&bmc_obj, "SystemName")),
                serial_number: normalize_text(get_str(&bmc_obj, "HostName")),
                bios_version: None,
                bmc_version: None,
                hostname: normalize_text(get_str(&bmc_obj, "HostName")),
                power_state,
                total_cpu_count: None,
                total_memory_gib: None,
            })
        }).await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"Cpu","obj_name":"","property_list":["Name","Presence","Manufacturer","Version","ProcessorID","CurrentSpeed",["CoreCount","/","ThreadCount"],"MemoryTec",["L1Cache","/","L2Cache","/","L3Cache"],"DisableCpuHw","CpuHealth","PartNum","SN"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let Some(items) = json.get("Cpu").and_then(|v| v.as_array()) else {
                return Ok(vec![]);
            };

            let mut out = Vec::new();
            for item in items {
                if get_i64(item, "Presence").unwrap_or(1) == 0 {
                    continue;
                }
                let (total_cores, total_threads) = parse_core_thread(get_str(item, "CoreCount_ThreadCount"));
                let mut l1 = None;
                let mut l2 = None;
                let mut l3 = None;
                if let Some(cache) = get_str(item, "L1Cache_L2Cache_L3Cache") {
                    let mut parts = cache.split('/');
                    l1 = parts.next().and_then(|v| v.trim().parse::<u32>().ok());
                    l2 = parts.next().and_then(|v| v.trim().parse::<u32>().ok());
                    l3 = parts.next().and_then(|v| v.trim().parse::<u32>().ok());
                }
                let id = get_str(item, "obj_name")
                    .or_else(|| get_str(item, "Name"))
                    .unwrap_or_else(|| "CPU".to_string());
                out.push(ProcessorInfo {
                    id,
                    socket: normalize_text(get_str(item, "Name")),
                    model: normalize_text(get_str(item, "Version")),
                    manufacturer: normalize_text(get_str(item, "Manufacturer")),
                    total_cores,
                    total_threads,
                    max_speed_mhz: parse_u32_from_text(get_str(item, "CurrentSpeed")),
                    temperature_celsius: None,
                    status: status_from_health_code(get_i64(item, "CpuHealth")),
                    architecture: None,
                    frequency_mhz: parse_u32_from_text(get_str(item, "CurrentSpeed")),
                    l1_cache_kib: l1,
                    l2_cache_kib: l2,
                    l3_cache_kib: l3,
                    serial_number: normalize_text(get_str(item, "SN")),
                    part_number: normalize_text(get_str(item, "PartNum")),
                    instruction_set: normalize_text(get_str(item, "MemoryTec")),
                });
            }
            Ok(out)
        }).await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"Memory","obj_name":"","property_list":["DimmName","Presence","Location","Manufacturer","Capacity","ClockSpeed","SN","Type","MinimumVoltage","Rank","BitWidth","Technology","PartNum","MemHealth","RemainLife","MediaTemp","ControllerTemp","VolatileCapacity","PersistentCapacity","HealthStatus"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let Some(items) = json.get("Memory").and_then(|v| v.as_array()) else {
                return Ok(vec![]);
            };

            let mut out = Vec::new();
            for item in items {
                let slot = get_str(item, "DimmName");
                let (channel, slot_index) = slot
                    .as_deref()
                    .map(dimm_channel_slot)
                    .unwrap_or((None, None));
                let populated = get_i64(item, "Presence").unwrap_or(0) == 1;
                let capacity_mib = parse_f64_from_text(get_str(item, "Capacity"));
                let media_temp = get_f64(item, "MediaTemp").filter(|v| (*v - 16384.0).abs() > f64::EPSILON);
                let ctrl_temp = get_f64(item, "ControllerTemp").filter(|v| (*v - 16384.0).abs() > f64::EPSILON);

                out.push(MemoryInfo {
                    id: get_str(item, "obj_name")
                        .or_else(|| slot.clone())
                        .unwrap_or_else(|| "Memory".to_string()),
                    capacity_gib: capacity_mib.map(|mib| mib / 1024.0),
                    memory_type: normalize_text(get_str(item, "Type")),
                    speed_mhz: parse_u32_from_text(get_str(item, "ClockSpeed")),
                    manufacturer: normalize_text(get_str(item, "Manufacturer")),
                    serial_number: normalize_text(get_str(item, "SN")),
                    slot,
                    channel,
                    slot_index,
                    temperature_celsius: media_temp.or(ctrl_temp),
                    populated,
                    status: status_from_health_code(get_i64(item, "MemHealth")),
                    part_number: normalize_text(get_str(item, "PartNum")),
                    rank_count: get_i64(item, "Rank").and_then(|v| u32::try_from(v).ok()),
                    module_type: None,
                    data_width_bits: get_i64(item, "BitWidth").and_then(|v| u32::try_from(v).ok()),
                });
            }
            Ok(out)
        }).await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"RaidController","obj_name":"","property_list":["ComponentName","Name","Type","HealthStatusCode","FirmwareVersion","PartNum","Id"]},{"class_name":"Raid","obj_name":"","property_list":["ProductName","Manufacturer","SlotId","Type","Firmware","PartNum"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let mut out = Vec::new();

            if let Some(items) = json.get("RaidController").and_then(|v| v.as_array()) {
                for item in items {
                    out.push(StorageInfo {
                        id: get_str(item, "obj_name")
                            .or_else(|| get_str(item, "ComponentName"))
                            .or_else(|| get_str(item, "Id"))
                            .unwrap_or_else(|| "RaidController".to_string()),
                        name: normalize_text(get_str(item, "ComponentName")),
                        capacity_gib: None,
                        media_type: None,
                        protocol: None,
                        manufacturer: None,
                        model: normalize_text(get_str(item, "Type").or_else(|| get_str(item, "Name"))),
                        serial_number: None,
                        status: status_from_health_code(get_i64(item, "HealthStatusCode")),
                        firmware_version: normalize_text(get_str(item, "FirmwareVersion")),
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
                        controller_name: normalize_text(get_str(item, "ComponentName")),
                        rebuild_state: None,
                    });
                }
            }

            if let Some(items) = json.get("Raid").and_then(|v| v.as_array()) {
                for item in items {
                    out.push(StorageInfo {
                        id: get_str(item, "obj_name")
                            .or_else(|| get_str(item, "ProductName"))
                            .unwrap_or_else(|| "Raid".to_string()),
                        name: normalize_text(get_str(item, "ProductName")),
                        capacity_gib: None,
                        media_type: None,
                        protocol: None,
                        manufacturer: normalize_text(get_str(item, "Manufacturer")),
                        model: normalize_text(get_str(item, "Type")),
                        serial_number: None,
                        status: Some("OK".to_string()),
                        firmware_version: normalize_text(get_str(item, "Firmware")),
                        rotation_speed_rpm: None,
                        capable_speed_gbps: None,
                        negotiated_speed_gbps: None,
                        failure_predicted: None,
                        predicted_media_life_left_percent: None,
                        hotspare_type: None,
                        temperature_celsius: None,
                        hours_powered_on: None,
                        slot_number: get_i64(item, "SlotId").and_then(|v| u32::try_from(v).ok()),
                        form_factor: None,
                        firmware_status: None,
                        raid_level: None,
                        controller_name: normalize_text(get_str(item, "ProductName")),
                        rebuild_state: None,
                    });
                }
            }

            Ok(out)
        }).await
    }

    async fn get_storage_controllers(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<StorageControllerInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"RaidController","obj_name":"","property_list":["ComponentName","Type","FirmwareVersion","HealthStatusCode","Id"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let Some(items) = json.get("RaidController").and_then(|v| v.as_array()) else {
                return Ok(vec![]);
            };
            let mut out = Vec::new();
            for item in items {
                out.push(StorageControllerInfo {
                    id: get_str(item, "obj_name")
                        .or_else(|| get_str(item, "Id"))
                        .unwrap_or_else(|| "RaidController".to_string()),
                    name: normalize_text(get_str(item, "ComponentName")),
                    manufacturer: None,
                    model: normalize_text(get_str(item, "Type")),
                    serial_number: None,
                    firmware_version: normalize_text(get_str(item, "FirmwareVersion")),
                    speed_gbps: None,
                    supported_raid_types: vec![],
                    cache_size_mib: None,
                    mode: None,
                    drive_count: None,
                    status: status_from_health_code(get_i64(item, "HealthStatusCode")),
                });
            }
            Ok(out)
        }).await
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"BusinessPort","obj_name":"","property_list":["RefNetCard","NetDevFuncType","MacAddr","CardType","OSEthName","IPv6Info","IPv4Info"]},{"class_name":"NetCard","obj_name":"","property_list":["ProductName","VirtualNetCardFlag"]}]"#;
            let json = session.query_multiproperty(str_input).await?;

            let mut card_name_map: HashMap<String, String> = HashMap::new();
            if let Some(cards) = json.get("NetCard").and_then(|v| v.as_array()) {
                for c in cards {
                    if let (Some(obj), Some(name)) = (get_str(c, "obj_name"), normalize_text(get_str(c, "ProductName"))) {
                        card_name_map.insert(obj, name);
                    }
                }
            }

            let Some(items) = json.get("BusinessPort").and_then(|v| v.as_array()) else {
                return Ok(vec![]);
            };
            let mut out = Vec::new();
            for item in items {
                let ref_card = get_str(item, "RefNetCard");
                let model = ref_card
                    .as_ref()
                    .and_then(|ref_name| card_name_map.get(ref_name).cloned());
                out.push(NetworkInterfaceInfo {
                    id: get_str(item, "obj_name").unwrap_or_else(|| "Port".to_string()),
                    name: normalize_text(get_str(item, "OSEthName")),
                    mac_address: normalize_text(get_str(item, "MacAddr")),
                    speed_mbps: None,
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: None,
                    ipv4_address: None,
                    manufacturer: None,
                    model,
                    slot: ref_card,
                    associated_resource: None,
                    bdf: None,
                    position: None,
                });
            }
            Ok(out)
        }).await
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"ThresholdSensor","obj_name":"","property_list":["SensorName","SensorUnitStr","ReaddingConvert","UpperNoncriticalConvert","UpperCriticalConvert","ReadingStatus"]},{"class_name":"CLASSFANTYPE","obj_name":"","property_list":["Name","Fspeed","Rspeed"]}]"#;
            let json = session.query_multiproperty(str_input).await?;

            let mut temperatures = Vec::new();
            let mut fans = Vec::new();

            if let Some(items) = json.get("ThresholdSensor").and_then(|v| v.as_array()) {
                for item in items {
                    let name = get_str(item, "SensorName").unwrap_or_default();
                    let unit = get_str(item, "SensorUnitStr").unwrap_or_default().to_ascii_lowercase();
                    let value = get_f64(item, "ReaddingConvert");
                    let upper = get_f64(item, "UpperCriticalConvert").or_else(|| get_f64(item, "UpperNoncriticalConvert"));
                    let status = match get_i64(item, "ReadingStatus") {
                        Some(55) => Some("OK".to_string()),
                        Some(_) => Some("Warning".to_string()),
                        None => None,
                    };

                    if unit.contains("rpm") || name.to_ascii_lowercase().contains("fan") {
                        fans.push(FanReading {
                            name,
                            reading_rpm: value.map(|v| v as u32),
                            status,
                        });
                    } else if unit.contains("degrees") {
                        temperatures.push(TemperatureReading {
                            name,
                            reading_celsius: value,
                            upper_threshold: upper,
                            status,
                        });
                    }
                }
            }

            if let Some(items) = json.get("CLASSFANTYPE").and_then(|v| v.as_array()) {
                for item in items {
                    let name = get_str(item, "Name").unwrap_or_else(|| "Fan".to_string());
                    if let Some(v) = get_i64(item, "Fspeed").and_then(|x| u32::try_from(x).ok()) {
                        fans.push(FanReading {
                            name: format!("{} F", name),
                            reading_rpm: Some(v),
                            status: Some("OK".to_string()),
                        });
                    }
                    if let Some(v) = get_i64(item, "Rspeed").and_then(|x| u32::try_from(x).ok()) {
                        fans.push(FanReading {
                            name: format!("{} R", name),
                            reading_rpm: Some(v),
                            status: Some("OK".to_string()),
                        });
                    }
                }
            }

            Ok(ThermalInfo { temperatures, fans })
        }).await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"OnePower","obj_name":"","property_list":["AnchorSlot","Presence","Manufacture","PsType","SN","PsFwVer","PsRate","PartNum"]},{"class_name":"ThresholdSensor","obj_name":"","property_list":["SensorName","SensorUnitStr","ReaddingConvert","obj_name"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let mut power_supplies = Vec::new();

            if let Some(items) = json.get("OnePower").and_then(|v| v.as_array()) {
                for item in items {
                    if get_i64(item, "Presence").unwrap_or(0) == 0 {
                        continue;
                    }
                    let slot = get_i64(item, "AnchorSlot").unwrap_or(0);
                    power_supplies.push(PowerSupplyInfo {
                        id: if slot > 0 { format!("PSU{}", slot) } else { "PSU".to_string() },
                        input_watts: None,
                        output_watts: None,
                        capacity_watts: get_f64(item, "PsRate"),
                        serial_number: normalize_text(get_str(item, "SN")),
                        firmware_version: normalize_text(get_str(item, "PsFwVer")),
                        manufacturer: normalize_text(get_str(item, "Manufacture")),
                        model: normalize_text(get_str(item, "PsType")),
                        status: Some("OK".to_string()),
                    });
                }
            }

            let mut power_consumed_watts = None;
            if let Some(items) = json.get("ThresholdSensor").and_then(|v| v.as_array()) {
                for item in items {
                    let unit = get_str(item, "SensorUnitStr").unwrap_or_default().to_ascii_lowercase();
                    let name = get_str(item, "SensorName").unwrap_or_default().to_ascii_lowercase();
                    let obj_name = get_str(item, "obj_name").unwrap_or_default().to_ascii_lowercase();
                    if unit.contains("watts") && (name == "power" || obj_name.contains("systotalpower")) {
                        power_consumed_watts = get_f64(item, "ReaddingConvert");
                        break;
                    }
                }
            }

            let power_capacity_watts = if power_supplies.is_empty() {
                None
            } else {
                Some(power_supplies.iter().filter_map(|ps| ps.capacity_watts).sum())
            };

            Ok(PowerInfo {
                power_consumed_watts,
                power_capacity_watts,
                current_cpu_power_watts: None,
                current_memory_power_watts: None,
                redundancy_mode: None,
                redundancy_health: None,
                power_supplies,
            })
        }).await
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        with_session(creds, |session| async move {
            let str_input = r#"[{"class_name":"Raid","obj_name":"","property_list":["ProductName","Manufacturer","SlotId","Type","PartNum"]},{"class_name":"RiserPcieCard","obj_name":"","property_list":["Name","Presence","Manufacturer","Slot","Type","PcbVer","LogicVer","BoardId"]},{"class_name":"HDDBackplane","obj_name":"","property_list":["Name","Presence","Manufacturer","Slot","Type","PcbVer","LogicVer","BoardId"]}]"#;
            let json = session.query_multiproperty(str_input).await?;
            let mut out = Vec::new();

            if let Some(items) = json.get("Raid").and_then(|v| v.as_array()) {
                for item in items {
                    out.push(PCIeDeviceInfo {
                        id: get_str(item, "obj_name").unwrap_or_else(|| "Raid".to_string()),
                        slot: get_i64(item, "SlotId").map(|v| format!("{}", v)),
                        name: normalize_text(get_str(item, "ProductName")),
                        description: normalize_text(get_str(item, "Type")),
                        manufacturer: normalize_text(get_str(item, "Manufacturer")),
                        model: normalize_text(get_str(item, "Type")),
                        device_class: Some("Storage".to_string()),
                        device_id: None,
                        vendor_id: None,
                        subsystem_id: None,
                        subsystem_vendor_id: None,
                        associated_resource: None,
                        position: None,
                        source_type: Some("raid".to_string()),
                        serial_number: None,
                        firmware_version: None,
                        link_width: None,
                        link_speed: None,
                        status: Some("OK".to_string()),
                        populated: true,
                    });
                }
            }

            if let Some(items) = json.get("RiserPcieCard").and_then(|v| v.as_array()) {
                for item in items {
                    let populated = get_i64(item, "Presence").unwrap_or(0) == 1;
                    out.push(PCIeDeviceInfo {
                        id: get_str(item, "obj_name").unwrap_or_else(|| "Riser".to_string()),
                        slot: get_i64(item, "Slot").map(|v| format!("{}", v)),
                        name: normalize_text(get_str(item, "Name")),
                        description: normalize_text(get_str(item, "Type")),
                        manufacturer: normalize_text(get_str(item, "Manufacturer")),
                        model: normalize_text(get_str(item, "Type")),
                        device_class: Some("Riser".to_string()),
                        device_id: None,
                        vendor_id: None,
                        subsystem_id: None,
                        subsystem_vendor_id: None,
                        associated_resource: None,
                        position: None,
                        source_type: Some("riser".to_string()),
                        serial_number: None,
                        firmware_version: normalize_text(get_str(item, "LogicVer")),
                        link_width: None,
                        link_speed: None,
                        status: Some(if populated { "OK".to_string() } else { "Absent".to_string() }),
                        populated,
                    });
                }
            }

            if let Some(items) = json.get("HDDBackplane").and_then(|v| v.as_array()) {
                for item in items {
                    let populated = get_i64(item, "Presence").unwrap_or(0) == 1;
                    out.push(PCIeDeviceInfo {
                        id: get_str(item, "obj_name").unwrap_or_else(|| "Backplane".to_string()),
                        slot: get_i64(item, "Slot").map(|v| format!("{}", v)),
                        name: normalize_text(get_str(item, "Name")),
                        description: normalize_text(get_str(item, "Type")),
                        manufacturer: normalize_text(get_str(item, "Manufacturer")),
                        model: normalize_text(get_str(item, "Type")),
                        device_class: Some("Backplane".to_string()),
                        device_id: None,
                        vendor_id: None,
                        subsystem_id: None,
                        subsystem_vendor_id: None,
                        associated_resource: None,
                        position: Some("chassis".to_string()),
                        source_type: Some("backplane".to_string()),
                        serial_number: None,
                        firmware_version: normalize_text(get_str(item, "LogicVer")),
                        link_width: None,
                        link_speed: None,
                        status: Some(if populated { "OK".to_string() } else { "Absent".to_string() }),
                        populated,
                    });
                }
            }

            Ok(out)
        }).await
    }

    async fn get_event_logs(
        &self,
        _creds: &BmcCreds,
        _limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        Ok(vec![])
    }

    async fn clear_event_logs(&self, _creds: &BmcCreds) -> BmcResult<()> {
        warn!("Huawei iMC-old clear_event_logs not implemented");
        Ok(())
    }
}
