use async_trait::async_trait;
use std::collections::HashMap;
use tokio::task::JoinSet;
use tracing::warn;

use crate::error::BmcResult;
use crate::generic_redfish::{
    extract_f64, extract_str, extract_u32, get_collection_members_with_token, get_redfish_session,
    redfish_get_with_token, GenericRedfishProvider,
};
use crate::types::*;
use crate::BmcProvider;

pub struct NettrixProvider;

fn parse_speed_mbps_from_text(value: &str) -> Option<u32> {
    let digits: String = value.chars().filter(|ch| ch.is_ascii_digit()).collect();
    let base = digits.parse::<u32>().ok()?;
    let upper = value.to_ascii_uppercase();
    if upper.contains("GB") || upper.contains("G/") || upper.contains("G ") {
        Some(base * 1000)
    } else {
        Some(base)
    }
}

fn extract_odata_id(value: &serde_json::Value) -> Option<String> {
    if let Some(path) = value.as_str() {
        return Some(path.to_string());
    }
    value
        .get("@odata.id")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
}

fn normalize_redfish_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn event_log_from_value(v: &serde_json::Value) -> Option<EventLogEntry> {
    let id = extract_str(v, "Id").unwrap_or_default();
    let message = extract_str(v, "Message");
    let created = extract_str(v, "Created").or_else(|| extract_str(v, "EventTimestamp"));
    let has_payload = !id.is_empty() || message.is_some() || created.is_some();
    if !has_payload {
        return None;
    }
    Some(EventLogEntry {
        id,
        severity: extract_str(v, "Severity").or_else(|| extract_str(v, "EntryType")),
        message,
        created,
        entry_type: extract_str(v, "EntryType"),
        subject: None,
        suggestion: None,
        event_code: extract_str(v, "MessageId"),
        alert_status: None,
    })
}

fn build_threshold_thermal(threshold: &serde_json::Value) -> ThermalInfo {
    let mut temperatures = Vec::new();
    let mut fans = Vec::new();
    let sensors = threshold
        .get("Sensors")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    for sensor in &sensors {
        let name = extract_str(sensor, "Name").unwrap_or_default();
        let unit = extract_str(sensor, "Unit")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let reading = extract_f64(sensor, "ReadingValue");
        let status = extract_str(sensor, "Status");

        if unit.contains("rpm") || name.to_ascii_uppercase().contains("FAN") {
            fans.push(FanReading {
                name,
                reading_rpm: reading.map(|v| v as u32),
                status,
            });
            continue;
        }

        if unit.contains("degree")
            || unit.contains("celsius")
            || name.to_ascii_uppercase().contains("TEMP")
        {
            temperatures.push(TemperatureReading {
                name,
                reading_celsius: reading,
                upper_threshold: extract_f64(sensor, "UpperThresholdCritical"),
                status,
            });
        }
    }

    ThermalInfo { temperatures, fans }
}

fn build_threshold_power(threshold: &serde_json::Value) -> Option<PowerInfo> {
    let sensors = threshold.get("Sensors").and_then(|v| v.as_array())?;
    let mut consumed = None;
    let mut capacity = None;
    let mut supplies: HashMap<String, PowerSupplyInfo> = HashMap::new();

    for sensor in sensors {
        let name = extract_str(sensor, "Name").unwrap_or_default();
        let upper_name = name.to_ascii_uppercase();
        let reading = extract_f64(sensor, "ReadingValue");
        let status = extract_str(sensor, "Status");

        if upper_name == "SYS_TOTAL_POWER" {
            consumed = reading;
            capacity = extract_f64(sensor, "UpperThresholdCritical");
            continue;
        }

        if !upper_name.starts_with("PSU") {
            continue;
        }

        let Some((psu_id, metric)) = upper_name.split_once('_') else {
            continue;
        };

        let entry = supplies
            .entry(psu_id.to_string())
            .or_insert_with(|| PowerSupplyInfo {
                id: psu_id.to_string(),
                input_watts: None,
                output_watts: None,
                capacity_watts: None,
                serial_number: None,
                firmware_version: None,
                manufacturer: None,
                model: None,
                status: status.clone(),
            });

        if metric == "PIN" {
            entry.output_watts = reading;
        }
        if entry.status.is_none() {
            entry.status = status.clone();
        }
    }

    let power_supplies: Vec<PowerSupplyInfo> = supplies.into_values().collect();
    if consumed.is_none() && power_supplies.is_empty() {
        return None;
    }

    Some(PowerInfo {
        power_consumed_watts: consumed,
        power_capacity_watts: capacity,
        current_cpu_power_watts: None,
        current_memory_power_watts: None,
        redundancy_mode: None,
        redundancy_health: None,
        power_supplies,
    })
}

fn has_meaningful_power(value: &PowerInfo) -> bool {
    value.power_consumed_watts.is_some()
        || value.power_capacity_watts.is_some()
        || !value.power_supplies.is_empty()
}

#[async_trait]
impl BmcProvider for NettrixProvider {
    fn name(&self) -> &str {
        "Nettrix Redfish"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        GenericRedfishProvider.test_connection(creds).await
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        GenericRedfishProvider.get_power_state(creds).await
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        GenericRedfishProvider.power_action(creds, action).await
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        let mut info = GenericRedfishProvider.get_system_info(creds).await?;
        let dimms = GenericRedfishProvider
            .get_memory(creds)
            .await
            .unwrap_or_default();
        let total_from_dimms: f64 = dimms
            .iter()
            .filter(|m| m.populated)
            .map(|m| m.capacity_gib.unwrap_or(0.0))
            .sum();

        // Nettrix can report per-socket value in MemorySummary; prefer DIMM aggregate when larger.
        if total_from_dimms > 0.0 {
            match info.total_memory_gib {
                Some(summary) if total_from_dimms > summary + 0.5 => {
                    info.total_memory_gib = Some(total_from_dimms);
                }
                None => {
                    info.total_memory_gib = Some(total_from_dimms);
                }
                _ => {}
            }
        }

        Ok(info)
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        GenericRedfishProvider.get_processors(creds).await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        GenericRedfishProvider.get_memory(creds).await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        GenericRedfishProvider.get_storage(creds).await
    }

    async fn get_storage_controllers(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<StorageControllerInfo>> {
        GenericRedfishProvider.get_storage_controllers(creds).await
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let adapters_path = format!("{}/NetworkAdapters", session.chassis_path);
        let adapters = match get_collection_members_with_token(creds, &adapters_path, tok).await {
            Ok(items) => items,
            Err(_) => return GenericRedfishProvider.get_network_interfaces(creds).await,
        };

        let mut results = Vec::new();
        for adapter in &adapters {
            let adapter_id = extract_str(adapter, "Id").unwrap_or_default();
            let adapter_name = extract_str(adapter, "Name");
            let adapter_manufacturer = extract_str(adapter, "Manufacturer")
                .or_else(|| extract_str(adapter, "CardManufacturer"));
            let adapter_model =
                extract_str(adapter, "Model").or_else(|| extract_str(adapter, "CardModel"));
            let slot = extract_u32(adapter, "SlotNumber").map(|v| v.to_string());
            let position = extract_str(adapter, "Position");

            let mut port_paths = Vec::new();
            let mut ports = Vec::new();

            if let Some(network_ports) = adapter.get("NetworkPorts") {
                if let Some(array) = network_ports.as_array() {
                    for item in array {
                        if let Some(path) = extract_odata_id(item) {
                            port_paths.push(path);
                        }
                    }
                } else if let Some(collection_path) = extract_odata_id(network_ports) {
                    if let Ok(collection_items) =
                        get_collection_members_with_token(creds, &collection_path, tok).await
                    {
                        ports.extend(collection_items);
                    }
                }
            }

            if let Some(link_ports) = adapter
                .get("Controllers")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|ctrl| ctrl.get("Links"))
                .and_then(|links| links.get("NetworkPorts"))
                .and_then(|v| v.as_array())
            {
                for item in link_ports {
                    if let Some(path) = extract_odata_id(item) {
                        port_paths.push(path);
                    }
                }
            }

            for path in port_paths {
                if let Ok(port) = redfish_get_with_token(creds, &path, tok).await {
                    ports.push(port);
                }
            }

            if ports.is_empty() {
                results.push(NetworkInterfaceInfo {
                    id: adapter_id.clone(),
                    name: adapter_name.clone(),
                    mac_address: None,
                    speed_mbps: None,
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: adapter.get("Status").and_then(|s| extract_str(s, "State")),
                    ipv4_address: None,
                    manufacturer: adapter_manufacturer.clone(),
                    model: adapter_model.clone(),
                    slot: slot.clone(),
                    associated_resource: None,
                    bdf: extract_str(adapter, "RootBDF"),
                    position: position.clone(),
                });
                continue;
            }

            for port in &ports {
                let port_id = extract_str(port, "Id")
                    .or_else(|| extract_str(port, "PhysicalPortNumber"))
                    .unwrap_or_default();
                let speed_text = extract_str(port, "PortSpeed");
                results.push(NetworkInterfaceInfo {
                    id: format!("{}-{}", adapter_id, port_id),
                    name: Some(format!(
                        "{} Port{}",
                        adapter_name.clone().unwrap_or_else(|| adapter_id.clone()),
                        if port_id.is_empty() { "?" } else { &port_id }
                    )),
                    mac_address: port
                        .get("AssociatedNetworkAddresses")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string()),
                    speed_mbps: extract_u32(port, "CurrentLinkSpeedMbps")
                        .or_else(|| extract_u32(port, "LinkSpeedMbps"))
                        .or_else(|| {
                            speed_text
                                .as_ref()
                                .and_then(|v| parse_speed_mbps_from_text(v))
                        }),
                    speed_gbps: None,
                    port_max_speed: speed_text,
                    link_status: extract_str(port, "LinkStatus"),
                    ipv4_address: None,
                    manufacturer: adapter_manufacturer.clone(),
                    model: adapter_model.clone(),
                    slot: slot.clone(),
                    associated_resource: None,
                    bdf: extract_str(port, "BDF").or_else(|| extract_str(adapter, "RootBDF")),
                    position: position.clone(),
                });
            }
        }

        if results.is_empty() {
            return GenericRedfishProvider.get_network_interfaces(creds).await;
        }
        Ok(results)
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        let generic = GenericRedfishProvider.get_thermal(creds).await;

        let mut threshold_data = None;
        if let Ok(session) = get_redfish_session(creds).await {
            threshold_data = redfish_get_with_token(
                creds,
                &format!("{}/ThresholdSensors", session.chassis_path),
                session.token.as_deref(),
            )
            .await
            .ok();
        }

        let threshold_thermal = threshold_data
            .as_ref()
            .map(build_threshold_thermal)
            .filter(|v| !v.temperatures.is_empty() || !v.fans.is_empty());

        match generic {
            Ok(mut value) => {
                if let Some(fallback) = threshold_thermal {
                    if value.temperatures.is_empty() {
                        value.temperatures = fallback.temperatures;
                    }
                    if value.fans.is_empty() {
                        value.fans = fallback.fans;
                    }
                }
                Ok(value)
            }
            Err(err) => threshold_thermal.ok_or(err),
        }
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        let generic = GenericRedfishProvider.get_power(creds).await;

        let mut threshold_power = None;
        if let Ok(session) = get_redfish_session(creds).await {
            if let Ok(threshold) = redfish_get_with_token(
                creds,
                &format!("{}/ThresholdSensors", session.chassis_path),
                session.token.as_deref(),
            )
            .await
            {
                threshold_power = build_threshold_power(&threshold);
            }
        }

        match generic {
            Ok(value) if has_meaningful_power(&value) => Ok(value),
            Ok(_) => threshold_power.ok_or_else(|| {
                warn!("Nettrix power fallback is empty");
                crate::error::BmcError::internal("Nettrix power data unavailable")
            }),
            Err(err) => threshold_power.ok_or(err),
        }
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        let generic = GenericRedfishProvider.get_pcie_devices(creds).await?;
        if !generic.is_empty() {
            return Ok(generic);
        }

        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let path = format!("{}/PCIeDevices", session.chassis_path);
        let devices = match get_collection_members_with_token(creds, &path, tok).await {
            Ok(items) => items,
            Err(_) => return Ok(vec![]),
        };

        let mut results = Vec::new();
        for device in &devices {
            let mut function_details = None;

            if let Some(uri) = device
                .get("Links")
                .and_then(|v| v.get("Functions"))
                .and_then(extract_odata_id)
            {
                if let Ok(functions) = get_collection_members_with_token(creds, &uri, tok).await {
                    function_details = functions.first().cloned();
                }
            }

            if function_details.is_none() {
                if let Some(uri) = device
                    .get("PCIeFunctions")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(extract_odata_id)
                {
                    function_details = redfish_get_with_token(creds, &uri, tok).await.ok();
                }
            }

            let function_ref = function_details.as_ref();
            let link_speed = device
                .get("PCIeInterface")
                .and_then(|v| extract_str(v, "LinkSpeedGTps"))
                .or_else(|| {
                    device
                        .get("PCIeInterface")
                        .and_then(|v| extract_f64(v, "LinkSpeedGTps"))
                        .map(|v| format!("{} GT/s", v))
                });

            results.push(PCIeDeviceInfo {
                id: extract_str(device, "Id").unwrap_or_default(),
                slot: extract_str(device, "Name"),
                name: extract_str(device, "Name"),
                description: extract_str(device, "Description"),
                manufacturer: extract_str(device, "Manufacturer"),
                model: extract_str(device, "Model"),
                device_class: function_ref
                    .and_then(|f| extract_str(f, "DeviceClass"))
                    .or_else(|| extract_str(device, "DeviceClass")),
                device_id: function_ref.and_then(|f| extract_str(f, "DeviceId")),
                vendor_id: function_ref.and_then(|f| extract_str(f, "VendorId")),
                subsystem_id: function_ref.and_then(|f| extract_str(f, "SubsystemId")),
                subsystem_vendor_id: function_ref.and_then(|f| extract_str(f, "SubsystemVendorId")),
                associated_resource: function_ref
                    .and_then(|f| extract_str(f, "AssociatedResource")),
                position: None,
                source_type: Some("pcie_device".to_string()),
                serial_number: extract_str(device, "SerialNumber"),
                firmware_version: extract_str(device, "FirmwareVersion"),
                link_width: device
                    .get("PCIeInterface")
                    .and_then(|v| extract_u32(v, "LanesInUse"))
                    .map(|v| format!("x{}", v)),
                link_speed,
                status: device.get("Status").and_then(|s| extract_str(s, "Health")),
                populated: true,
            });
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
        let limit = limit.unwrap_or(100) as usize;

        let services_path = format!("{}/LogServices", session.manager_path);
        let services = match get_collection_members_with_token(creds, &services_path, tok).await {
            Ok(items) if !items.is_empty() => items,
            _ => {
                return GenericRedfishProvider
                    .get_event_logs(creds, Some(limit as u32))
                    .await
            }
        };

        let mut entries_candidates: Vec<(String, String)> = services
            .iter()
            .filter_map(|svc| {
                let id = extract_str(svc, "Id").unwrap_or_default();
                let entries = svc
                    .get("Entries")
                    .and_then(extract_odata_id)
                    .map(|p| normalize_redfish_path(&p))?;
                Some((id, entries))
            })
            .collect();

        entries_candidates.sort_by_key(|(id, _)| {
            let lower = id.to_ascii_lowercase();
            if lower == "auditlog" {
                0
            } else if lower == "sel" {
                1
            } else if lower == "log1" {
                2
            } else {
                3
            }
        });

        for (_, entries_path) in &entries_candidates {
            let query_path = format!("{}?$top={}", entries_path, limit);
            let collection = match redfish_get_with_token(creds, &query_path, tok).await {
                Ok(v) => Ok(v),
                Err(_) => redfish_get_with_token(creds, entries_path, tok).await,
            };
            let Ok(collection) = collection else {
                continue;
            };
            let members = collection
                .get("Members")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default();
            if members.is_empty() {
                continue;
            }

            let mut results = Vec::new();
            let mut detail_uris: Vec<String> = Vec::new();
            for member in members.iter().take(limit) {
                if let Some(inline) = event_log_from_value(member) {
                    results.push(inline);
                    continue;
                }

                let Some(entry_uri) = member
                    .get("@odata.id")
                    .and_then(|v| v.as_str())
                    .map(normalize_redfish_path)
                else {
                    continue;
                };
                detail_uris.push(entry_uri);
            }

            if !detail_uris.is_empty() && results.len() < limit {
                let max_concurrency = 20usize;
                let token_owned = session.token.clone();

                for chunk in detail_uris.chunks(max_concurrency) {
                    let mut join_set: JoinSet<(usize, Option<EventLogEntry>)> = JoinSet::new();
                    for (offset, uri) in chunk.iter().enumerate() {
                        let creds_cloned = creds.clone();
                        let token_cloned = token_owned.clone();
                        let uri_cloned = uri.clone();
                        join_set.spawn(async move {
                            let entry = redfish_get_with_token(
                                &creds_cloned,
                                &uri_cloned,
                                token_cloned.as_deref(),
                            )
                            .await
                            .ok()
                            .and_then(|v| event_log_from_value(&v));
                            (offset, entry)
                        });
                    }

                    let mut ordered: Vec<Option<EventLogEntry>> = vec![None; chunk.len()];
                    while let Some(joined) = join_set.join_next().await {
                        if let Ok((offset, Some(entry))) = joined {
                            if offset < ordered.len() {
                                ordered[offset] = Some(entry);
                            }
                        }
                    }

                    for item in ordered.into_iter().flatten() {
                        results.push(item);
                        if results.len() >= limit {
                            break;
                        }
                    }

                    if results.len() >= limit {
                        break;
                    }
                }
            }
            if !results.is_empty() {
                return Ok(results);
            }
        }

        GenericRedfishProvider
            .get_event_logs(creds, Some(limit as u32))
            .await
    }

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()> {
        GenericRedfishProvider.clear_event_logs(creds).await
    }
}
