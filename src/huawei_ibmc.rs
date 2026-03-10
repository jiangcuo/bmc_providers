use crate::error::{BmcError, BmcResult};
use crate::generic_redfish::{
    extract_f64, extract_str, extract_u32, get_collection_members_with_token, get_redfish_session,
    redfish_get_with_token, redfish_post_with_token, GenericRedfishProvider,
};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{debug, info, warn};

pub struct HuaweiIbmcProvider;

fn parse_speed_mbps_from_text(value: &str) -> Option<u32> {
    let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
    let raw = digits.parse::<u32>().ok()?;
    let upper = value.to_uppercase();
    if upper.contains("GE") || upper.contains("GBPS") || upper.contains('G') {
        Some(raw * 1000)
    } else if upper.contains("ME") || upper.contains("MBPS") || upper.contains('M') {
        Some(raw)
    } else {
        Some(raw)
    }
}

#[async_trait]
impl BmcProvider for HuaweiIbmcProvider {
    fn name(&self) -> &str {
        "Huawei iBMC"
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
        GenericRedfishProvider.get_system_info(creds).await
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let view_path = format!("{}/ProcessorView", session.system_path);
        if let Ok(data) = redfish_get_with_token(creds, &view_path, tok).await {
            //info!("Retrieved data from Huawei ProcessorView {}",data);
            if let Some(info_arr) = data.get("Information").and_then(|v| v.as_array()) {
                return Ok(info_arr
                    .iter()
                    .filter_map(|p| {
                        let socket_num = p.get("Socket").and_then(|v| v.as_u64());
                        let device_loc = extract_str(p, "DeviceLocator");
                        let model = extract_str(p, "Model");
                        let id = extract_str(p, "Id").unwrap_or_default();
                        let total_cores = extract_u32(p, "TotalCores");
                        let total_threads = extract_u32(p, "TotalThreads");
                        let architecture = extract_str(p, "ProcessorArchitecture");
                        let indicator = format!(
                            "{} {} {}",
                            id,
                            device_loc.clone().unwrap_or_default(),
                            model.clone().unwrap_or_default()
                        )
                        .to_lowercase();
                        let looks_non_cpu = indicator.contains("pcie")
                            || indicator.contains("gpu")
                            || indicator.contains("tesla");
                        let has_cpu_signals = total_cores.is_some()
                            || total_threads.is_some()
                            || architecture.is_some()
                            || socket_num.is_some();
                        if looks_non_cpu || !has_cpu_signals {
                            return None;
                        }
                        let socket_str =
                            device_loc.or_else(|| socket_num.map(|n| format!("CPU{}", n + 1)));
                        Some(ProcessorInfo {
                            id,
                            socket: socket_str,
                            model,
                            manufacturer: extract_str(p, "Manufacturer"),
                            total_cores,
                            total_threads,
                            max_speed_mhz: extract_u32(p, "MaxSpeedMHz"),
                            temperature_celsius: extract_f64(p, "Temperature"),
                            status: p.get("Status").and_then(|s| extract_str(s, "Health")),
                            architecture,
                            frequency_mhz: extract_u32(p, "FrequencyMHz"),
                            l1_cache_kib: extract_u32(p, "L1CacheKiB"),
                            l2_cache_kib: extract_u32(p, "L2CacheKiB"),
                            l3_cache_kib: extract_u32(p, "L3CacheKiB"),
                            serial_number: extract_str(p, "SerialNumber"),
                            part_number: extract_str(p, "PartNumber"),
                            instruction_set: extract_str(p, "InstructionSet"),
                        })
                    })
                    .collect());
            }
        }

        warn!("Huawei ProcessorView unavailable, falling back to standard Processors");
        GenericRedfishProvider.get_processors(creds).await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let view_path = format!("{}/MemoryView", session.system_path);
        if let Ok(data) = redfish_get_with_token(creds, &view_path, tok).await {
            if let Some(info_arr) = data.get("Information").and_then(|v| v.as_array()) {
                return Ok(info_arr
                    .iter()
                    .map(|m| {
                        let cap_mib = extract_u32(m, "CapacityMiB").unwrap_or(0);
                        let populated = cap_mib > 0;
                        let status_state = m.get("Status").and_then(|s| extract_str(s, "State"));
                        let is_absent = status_state
                            .as_ref()
                            .map_or(false, |s| s.to_lowercase() == "absent");
                        let sn = extract_str(m, "SerialNumber");
                        let sn_valid = sn
                            .as_ref()
                            .map_or(false, |s| s != "NO DIMM" && !s.is_empty());

                        let _socket = m.get("Socket").and_then(|v| v.as_u64());
                        let channel = m.get("Channel").and_then(|v| v.as_u64());
                        let slot = m.get("Slot").and_then(|v| v.as_u64());

                        MemoryInfo {
                            id: extract_str(m, "Id").unwrap_or_default(),
                            capacity_gib: if populated {
                                Some(cap_mib as f64 / 1024.0)
                            } else {
                                None
                            },
                            memory_type: extract_str(m, "MemoryDeviceType")
                                .or_else(|| extract_str(m, "Type")),
                            speed_mhz: extract_u32(m, "OperatingSpeedMhz"),
                            manufacturer: extract_str(m, "Manufacturer"),
                            serial_number: if sn_valid { sn } else { None },
                            slot: extract_str(m, "DeviceLocator"),
                            channel: channel.map(|c| format!("{}", c)),
                            slot_index: slot.map(|s| s as u32),
                            temperature_celsius: extract_f64(m, "MediumTemperatureCelsius")
                                .or_else(|| extract_f64(m, "ControllerTemperatureCelsius")),
                            populated: populated && !is_absent,
                            status: m.get("Status").and_then(|s| extract_str(s, "Health")),
                            part_number: {
                                let pn = extract_str(m, "PartNumber");
                                if pn.as_ref().map_or(false, |s| s == "NO DIMM") {
                                    None
                                } else {
                                    pn
                                }
                            },
                            rank_count: extract_u32(m, "RankCount"),
                            module_type: extract_str(m, "BaseModuleType"),
                            data_width_bits: extract_u32(m, "DataWidthBits"),
                        }
                    })
                    .collect());
            }
        }

        warn!("Huawei MemoryView unavailable, falling back to standard Memory");
        GenericRedfishProvider.get_memory(creds).await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let storage_collection = session
            .storage_path
            .clone()
            .unwrap_or_else(|| format!("{}/Storages", session.system_path));

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
                    let uri = drv_ref.get("@odata.id").and_then(|u| u.as_str());
                    if uri.is_none() {
                        continue;
                    }
                    let uri = uri.unwrap();

                    if let Ok(d) = redfish_get_with_token(creds, uri, tok).await {
                        let cap_bytes =
                            d.get("CapacityBytes").and_then(|c| c.as_u64()).unwrap_or(0);
                        let oem = d.get("Oem").and_then(|o| o.get("Huawei"));

                        let raid_level = oem
                            .and_then(|h| h.get("RelatedArrayInfo"))
                            .and_then(|r| extract_str(r, "VolumeRaidLevel"));

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
                            failure_predicted: d.get("FailurePredicted").and_then(|v| v.as_bool()),
                            predicted_media_life_left_percent: extract_u32(
                                &d,
                                "PredictedMediaLifeLeftPercent",
                            ),
                            hotspare_type: extract_str(&d, "HotspareType"),
                            temperature_celsius: oem
                                .and_then(|h| extract_f64(h, "TemperatureCelsius")),
                            hours_powered_on: oem.and_then(|h| extract_f64(h, "HoursOfPoweredUp")),
                            slot_number: oem.and_then(|h| extract_u32(h, "SlotNumber")),
                            form_factor: oem.and_then(|h| extract_str(h, "FormFactor")),
                            firmware_status: oem.and_then(|h| extract_str(h, "FirmwareStatus")),
                            raid_level,
                            controller_name: ctrl_name.clone(),
                            rebuild_state: oem.and_then(|h| extract_str(h, "RebuildState")),
                        });
                    }
                }
            }
        }
        Ok(drives)
    }

    async fn get_storage_controllers(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<StorageControllerInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let storage_collection = session
            .storage_path
            .clone()
            .unwrap_or_else(|| format!("{}/Storages", session.system_path));

        let storages = get_collection_members_with_token(creds, &storage_collection, tok).await?;
        let mut results = Vec::new();

        for storage in &storages {
            let drive_count = storage
                .get("Drives@odata.count")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    storage
                        .get("Drives")
                        .and_then(|d| d.as_array())
                        .map(|a| a.len() as u64)
                });

            if let Some(ctrls) = storage
                .get("StorageControllers")
                .and_then(|sc| sc.as_array())
            {
                for ctrl in ctrls {
                    let oem = ctrl.get("Oem").and_then(|o| o.get("Huawei"));
                    let model_oem = oem.and_then(|h| extract_str(h, "Type"));
                    let model_std = extract_str(ctrl, "Model");

                    let supported = ctrl
                        .get("SupportedRAIDTypes")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect::<Vec<_>>()
                        })
                        .or_else(|| {
                            oem.and_then(|h| h.get("SupportedRAIDLevels"))
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                })
                        })
                        .unwrap_or_default();

                    results.push(StorageControllerInfo {
                        id: extract_str(ctrl, "MemberId")
                            .or_else(|| extract_str(storage, "Id"))
                            .unwrap_or_default(),
                        name: extract_str(ctrl, "Name"),
                        manufacturer: extract_str(ctrl, "Manufacturer"),
                        model: model_oem.or(model_std),
                        serial_number: extract_str(ctrl, "SerialNumber"),
                        firmware_version: extract_str(ctrl, "FirmwareVersion"),
                        speed_gbps: extract_f64(ctrl, "SpeedGbps"),
                        supported_raid_types: supported,
                        cache_size_mib: ctrl
                            .get("CacheSummary")
                            .and_then(|c| extract_u32(c, "TotalCacheSizeMiB")),
                        mode: oem.and_then(|h| extract_str(h, "Mode")),
                        drive_count: drive_count.map(|c| c as u32),
                        status: ctrl.get("Status").and_then(|s| extract_str(s, "Health")),
                    });
                }
            }
        }
        Ok(results)
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
            let adapter_status = adapter.get("Status").and_then(|s| extract_str(s, "State"));
            let adapter_manufacturer = extract_str(adapter, "Manufacturer").or_else(|| {
                adapter
                    .get("Oem")
                    .and_then(|o| o.get("Huawei"))
                    .and_then(|h| extract_str(h, "CardManufacturer"))
            });
            let adapter_model = extract_str(adapter, "Model").or_else(|| {
                adapter
                    .get("Oem")
                    .and_then(|o| o.get("Huawei"))
                    .and_then(|h| extract_str(h, "CardModel"))
            });
            let adapter_oem = adapter.get("Oem").and_then(|o| o.get("Huawei"));
            let slot = adapter_oem
                .and_then(|h| extract_u32(h, "SlotNumber").map(|s| s.to_string()))
                .or_else(|| adapter_oem.and_then(|h| extract_str(h, "DeviceLocator")));
            let associated_resource =
                adapter_oem.and_then(|h| extract_str(h, "AssociatedResource"));
            let position = adapter_oem.and_then(|h| extract_str(h, "Position"));

            let mut mac_by_port: HashMap<String, String> = HashMap::new();
            if let Some(ports_cfg) = adapter_oem
                .and_then(|h| h.get("Configuration"))
                .and_then(|c| c.get("PortsConfig"))
                .and_then(|p| p.as_array())
            {
                for item in ports_cfg {
                    let port_id = extract_u32(item, "PortId").map(|v| v.to_string());
                    let mac = item
                        .get("PFsInfo")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|pf| extract_str(pf, "PermanentAddress"))
                        .map(|m| {
                            if m.contains(':') || m.len() != 12 {
                                m
                            } else {
                                (0..6)
                                    .map(|idx| &m[idx * 2..idx * 2 + 2])
                                    .collect::<Vec<_>>()
                                    .join(":")
                            }
                        });
                    if let (Some(pid), Some(mac_addr)) = (port_id, mac) {
                        mac_by_port.insert(pid, mac_addr);
                    }
                }
            }

            let mut port_uris: Vec<String> = Vec::new();
            if let Some(ports) = adapter
                .get("Controllers")
                .and_then(|c| c.as_array())
                .and_then(|arr| arr.first())
                .and_then(|ctrl| ctrl.get("Links"))
                .and_then(|l| l.get("NetworkPorts"))
                .and_then(|p| p.as_array())
            {
                for p in ports {
                    if let Some(uri) = p.get("@odata.id").and_then(|u| u.as_str()) {
                        port_uris.push(uri.to_string());
                    }
                }
            }
            if port_uris.is_empty() {
                if let Some(ports_collection_uri) = adapter
                    .get("NetworkPorts")
                    .and_then(|p| p.get("@odata.id"))
                    .and_then(|u| u.as_str())
                {
                    if let Ok(ports) =
                        get_collection_members_with_token(creds, ports_collection_uri, tok).await
                    {
                        for port in &ports {
                            if let Some(uri) = port.get("@odata.id").and_then(|u| u.as_str()) {
                                port_uris.push(uri.to_string());
                            }
                        }
                    }
                }
            }

            let mut pushed_port = false;
            for port_uri in &port_uris {
                let Ok(port) = redfish_get_with_token(creds, port_uri, tok).await else {
                    continue;
                };

                let port_id = extract_str(&port, "Id")
                    .or_else(|| extract_str(&port, "PhysicalPortNumber"))
                    .unwrap_or_default();
                let port_oem = port.get("Oem").and_then(|o| o.get("Huawei"));

                let max_speed = port_oem.and_then(|o| extract_str(o, "PortMaxSpeed"));
                let speed_gbps = port_oem.and_then(|o| extract_f64(o, "LinkSpeedGbps"));
                let speed_mbps = extract_u32(&port, "CurrentLinkSpeedMbps")
                    .or_else(|| extract_u32(&port, "LinkSpeedMbps"))
                    .or_else(|| speed_gbps.map(|g| (g * 1000.0) as u32))
                    .or_else(|| {
                        max_speed
                            .as_ref()
                            .and_then(|s| parse_speed_mbps_from_text(s))
                    });

                let mac = port
                    .get("AssociatedNetworkAddresses")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|m| m.as_str())
                    .map(String::from)
                    .or_else(|| mac_by_port.get(&port_id).cloned());

                let mut ipv4 = None;
                if let Some(related_uri) = port_oem
                    .and_then(|o| o.get("RelatedPort"))
                    .and_then(|r| r.get("@odata.id"))
                    .and_then(|u| u.as_str())
                {
                    if let Ok(eth) = redfish_get_with_token(creds, related_uri, tok).await {
                        ipv4 = eth
                            .get("IPv4Addresses")
                            .and_then(|v| v.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|ip| ip.get("Address"))
                            .and_then(|v| v.as_str())
                            .map(String::from);
                    }
                }

                results.push(NetworkInterfaceInfo {
                    id: format!("{}-{}", adapter_id, port_id),
                    name: Some(format!(
                        "{} Port{}",
                        adapter_name.clone().unwrap_or_else(|| adapter_id.clone()),
                        if port_id.is_empty() { "?" } else { &port_id }
                    )),
                    mac_address: mac,
                    speed_mbps,
                    speed_gbps,
                    port_max_speed: max_speed,
                    link_status: extract_str(&port, "LinkStatus")
                        .or_else(|| port.get("Status").and_then(|s| extract_str(s, "State"))),
                    ipv4_address: ipv4,
                    manufacturer: adapter_manufacturer.clone(),
                    model: adapter_model.clone(),
                    slot: slot.clone(),
                    associated_resource: associated_resource.clone(),
                    bdf: port_oem.and_then(|o| extract_str(o, "BDF")),
                    position: position.clone(),
                });
                pushed_port = true;
            }

            if !pushed_port {
                results.push(NetworkInterfaceInfo {
                    id: adapter_id.clone(),
                    name: adapter_name.clone(),
                    mac_address: None,
                    speed_mbps: None,
                    speed_gbps: None,
                    port_max_speed: None,
                    link_status: adapter_status,
                    ipv4_address: None,
                    manufacturer: adapter_manufacturer,
                    model: adapter_model,
                    slot,
                    associated_resource,
                    bdf: None,
                    position,
                });
            }
        }

        if results.is_empty() {
            return GenericRedfishProvider.get_network_interfaces(creds).await;
        }
        Ok(results)
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let mut thermal = GenericRedfishProvider.get_thermal(creds).await?;
        let need_fallback_fans =
            thermal.fans.is_empty() || thermal.fans.iter().all(|f| f.reading_rpm.is_none());

        if need_fallback_fans {
            let threshold_paths = [
                format!("{}/ThresholdSensors", session.chassis_path),
                format!("{}/ThresholdSensors", session.system_path),
            ];

            for threshold_path in &threshold_paths {
                let Ok(data) = redfish_get_with_token(creds, threshold_path, tok).await else {
                    continue;
                };
                let Some(sensors) = data.get("Sensors").and_then(|s| s.as_array()) else {
                    continue;
                };

                let mut threshold_fans = Vec::new();
                for sensor in sensors {
                    let name = extract_str(sensor, "Name").unwrap_or_default();
                    let unit = extract_str(sensor, "Unit")
                        .unwrap_or_default()
                        .to_lowercase();
                    let is_fan = unit == "rpm" || name.to_lowercase().contains("fan");
                    if !is_fan {
                        continue;
                    }
                    threshold_fans.push(FanReading {
                        name,
                        reading_rpm: extract_u32(sensor, "ReadingValue")
                            .or_else(|| extract_u32(sensor, "Reading")),
                        status: extract_str(sensor, "Status"),
                    });
                }

                if threshold_fans.is_empty() {
                    continue;
                }

                if thermal.fans.is_empty() {
                    thermal.fans = threshold_fans;
                } else {
                    for fan in &mut thermal.fans {
                        if fan.reading_rpm.is_some() {
                            continue;
                        }
                        let fan_key = fan.name.to_lowercase();
                        if let Some(src) = threshold_fans.iter().find(|src| {
                            let src_key = src.name.to_lowercase();
                            src_key == fan_key
                                || src_key.contains(&fan_key)
                                || fan_key.contains(&src_key)
                        }) {
                            fan.reading_rpm = src.reading_rpm;
                            if fan.status.is_none() {
                                fan.status = src.status.clone();
                            }
                        }
                    }
                }

                if thermal.fans.iter().any(|f| f.reading_rpm.is_some()) {
                    break;
                }
            }
        }

        Ok(thermal)
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        GenericRedfishProvider.get_power(creds).await
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let mut results = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        let chassis_pcie_path = format!("{}/PCIeDevices", session.chassis_path);
        if let Ok(items) = get_collection_members_with_token(creds, &chassis_pcie_path, tok).await {
            for d in &items {
                let function_uri = d
                    .get("Links")
                    .and_then(|l| l.get("PCIeFunctions"))
                    .and_then(|f| f.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|entry| entry.get("@odata.id"))
                    .and_then(|u| u.as_str())
                    .or_else(|| {
                        d.get("PCIeFunctions")
                            .and_then(|f| f.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|entry| entry.get("@odata.id"))
                            .and_then(|u| u.as_str())
                    });
                let function_data = if let Some(uri) = function_uri {
                    redfish_get_with_token(creds, uri, tok).await.ok()
                } else {
                    None
                };

                let function_oem = function_data
                    .as_ref()
                    .and_then(|f| f.get("Oem"))
                    .and_then(|o| o.get("Huawei"));
                let device_oem = d.get("Oem").and_then(|o| o.get("Huawei"));

                let device_class = function_data
                    .as_ref()
                    .and_then(|f| extract_str(f, "DeviceClass"))
                    .or_else(|| extract_str(d, "DeviceClass"))
                    .or_else(|| extract_str(d, "DeviceType"));

                let slot_info = device_oem
                    .and_then(|h| extract_str(h, "DeviceLocator"))
                    .or_else(|| extract_str(d, "Name"));

                let id = extract_str(d, "Id").unwrap_or_default();
                if !seen_ids.insert(id.clone()) {
                    continue;
                }

                results.push(PCIeDeviceInfo {
                    id,
                    slot: slot_info.or_else(|| {
                        d.get("Location").and_then(|loc| {
                            extract_str(loc, "PartLocation").or_else(|| extract_str(loc, "Info"))
                        })
                    }),
                    name: extract_str(d, "Name"),
                    description: extract_str(d, "Description"),
                    manufacturer: extract_str(d, "Manufacturer"),
                    model: extract_str(d, "Model")
                        .or_else(|| device_oem.and_then(|o| extract_str(o, "ProductName"))),
                    device_class,
                    device_id: function_data
                        .as_ref()
                        .and_then(|f| extract_str(f, "DeviceId")),
                    vendor_id: function_data
                        .as_ref()
                        .and_then(|f| extract_str(f, "VendorId")),
                    subsystem_id: function_data
                        .as_ref()
                        .and_then(|f| extract_str(f, "SubsystemId")),
                    subsystem_vendor_id: function_data
                        .as_ref()
                        .and_then(|f| extract_str(f, "SubsystemVendorId")),
                    associated_resource: function_oem
                        .and_then(|o| extract_str(o, "AssociatedResource")),
                    position: device_oem.and_then(|o| extract_str(o, "Position")),
                    source_type: Some("pcie_device".to_string()),
                    serial_number: extract_str(d, "SerialNumber"),
                    firmware_version: extract_str(d, "FirmwareVersion"),
                    link_width: function_oem
                        .and_then(|o| extract_str(o, "LinkWidth"))
                        .or_else(|| function_oem.and_then(|o| extract_str(o, "LinkWidthAbility")))
                        .or_else(|| {
                            d.get("PCIeInterface").and_then(|p| {
                                extract_str(p, "LanesInUse").or_else(|| {
                                    extract_u32(p, "LanesInUse").map(|v| format!("x{}", v))
                                })
                            })
                        }),
                    link_speed: function_oem
                        .and_then(|o| extract_str(o, "LinkSpeed"))
                        .or_else(|| function_oem.and_then(|o| extract_str(o, "LinkSpeedAbility")))
                        .or_else(|| {
                            d.get("PCIeInterface").and_then(|p| {
                                extract_str(p, "PCIeType").or_else(|| extract_str(p, "MaxPCIeType"))
                            })
                        }),
                    status: d.get("Status").and_then(|s| extract_str(s, "Health")),
                    populated: true,
                });
            }
        }

        let boards_path = format!("{}/Boards", session.chassis_path);
        if let Ok(boards) = get_collection_members_with_token(creds, &boards_path, tok).await {
            for board in &boards {
                let device_type = extract_str(board, "DeviceType");
                let include = device_type
                    .as_ref()
                    .map(|v| v.contains("RAID") || v.contains("Riser") || v.contains("Backplane"))
                    .unwrap_or(false)
                    || extract_str(board, "Id")
                        .as_deref()
                        .map(|id| id.to_lowercase().contains("raid"))
                        .unwrap_or(false);
                if !include {
                    continue;
                }

                let id = extract_str(board, "Id").unwrap_or_default();
                if !seen_ids.insert(id.clone()) {
                    continue;
                }

                results.push(PCIeDeviceInfo {
                    id,
                    slot: extract_str(board, "DeviceLocator")
                        .or_else(|| extract_str(board, "PositionId")),
                    name: extract_str(board, "Name")
                        .or_else(|| extract_str(board, "DeviceLocator")),
                    description: extract_str(board, "Description"),
                    manufacturer: extract_str(board, "Manufacturer"),
                    model: extract_str(board, "PartNumber")
                        .or_else(|| extract_str(board, "BoardName"))
                        .or_else(|| extract_str(board, "ProductName")),
                    device_class: device_type,
                    device_id: extract_str(board, "BoardId"),
                    vendor_id: None,
                    subsystem_id: None,
                    subsystem_vendor_id: None,
                    associated_resource: extract_str(board, "AssociatedResource"),
                    position: extract_str(board, "Location"),
                    source_type: Some("board".to_string()),
                    serial_number: extract_str(board, "SerialNumber"),
                    firmware_version: extract_str(board, "CPLDVersion")
                        .or_else(|| extract_str(board, "PCBVersion")),
                    link_width: extract_str(board, "LinkWidth")
                        .or_else(|| extract_str(board, "LinkWidthAbility")),
                    link_speed: extract_str(board, "LinkSpeed")
                        .or_else(|| extract_str(board, "LinkSpeedAbility")),
                    status: board.get("Status").and_then(|s| extract_str(s, "Health")),
                    populated: true,
                });
            }
        }

        if results.is_empty() {
            warn!("Huawei chassis PCIeDevices empty, falling back to generic");
            return GenericRedfishProvider.get_pcie_devices(creds).await;
        }

        Ok(results)
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        info!("get ibmc log");
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();
        let limit = limit.unwrap_or(100) as usize;

        let query_path = format!(
            "{}/LogServices/Log1/Actions/Oem/Huawei/LogService.QuerySelLogEntries",
            session.system_path
        );

        let mut all_entries = Vec::new();
        let mut start_id = 1u32;
        let page_size = 50u32;

        loop {
            let body = serde_json::json!({
                "StartEntryId": start_id,
                "EntriesCount": page_size
            });

            let resp = match redfish_post_with_token(creds, &query_path, &body, tok).await {
                Ok(r) => r,
                Err(_) => {
                    warn!("Huawei QuerySelLogEntries failed, falling back to standard");
                    return GenericRedfishProvider
                        .get_event_logs(creds, Some(limit as u32))
                        .await;
                }
            };

            let sel_entries = resp
                .get("error")
                .and_then(|e| e.get("@Message.ExtendedInfo"))
                .and_then(|i| i.as_array())
                .and_then(|arr| arr.first())
                .and_then(|msg| msg.get("Oem"))
                .and_then(|o| o.get("Huawei"))
                .and_then(|h| h.get("SelLogEntries"))
                .and_then(|s| s.as_array());

            let entries = match sel_entries {
                Some(e) => e,
                None => break,
            };

            let mut total_count: Option<u32> = None;
            for entry in entries {
                if let Some(num) = entry.get("number").and_then(|n| n.as_u64()) {
                    total_count = Some(num as u32);
                    continue;
                }

                let level = entry.get("level").and_then(|l| l.as_str()).unwrap_or("0");
                let severity = match level {
                    "3" => "Critical",
                    "2" => "Warning",
                    "1" => "Minor",
                    _ => "Informational",
                };

                let suggestion = extract_str(entry, "eventsugg").map(|s| s.replace("@#AB;", "\n"));

                all_entries.push(EventLogEntry {
                    id: extract_str(entry, "eventid").unwrap_or_default(),
                    severity: Some(severity.to_string()),
                    message: extract_str(entry, "eventdesc"),
                    created: extract_str(entry, "alerttime"),
                    entry_type: None,
                    subject: extract_str(entry, "eventsubject"),
                    suggestion,
                    event_code: extract_str(entry, "eventcode"),
                    alert_status: extract_str(entry, "status"),
                });
            }

            if all_entries.len() >= limit {
                all_entries.truncate(limit);
                break;
            }

            match total_count {
                Some(total) if start_id + page_size > total => break,
                None if entries.len() <= 1 => break,
                _ => {}
            }

            start_id += page_size;
        }

        Ok(all_entries)
    }

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()> {
        let session = get_redfish_session(creds).await?;
        let paths = [
            format!(
                "{}/LogServices/Log1/Actions/LogService.ClearLog",
                session.system_path
            ),
            format!(
                "{}/LogServices/SEL/Actions/LogService.ClearLog",
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

    fn console_types(&self) -> Vec<ConsoleType> {
        vec![ConsoleType::Html5, ConsoleType::Java]
    }

    async fn get_kvm_console(
        &self,
        creds: &BmcCreds,
        console_type: &ConsoleType,
    ) -> BmcResult<KvmConsoleInfo> {
        let session = get_redfish_session(creds).await?;
        let tok = session.token.as_deref();

        let session_id = session
            .session_uri
            .as_deref()
            .and_then(|uri| uri.rsplit('/').next())
            .unwrap_or("")
            .to_string();

        let jnlp = export_and_download_jnlp(creds, &session.manager_path, tok).await?;

        let kvm_port = get_param_value(&jnlp, "port")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(2198);
        let vmm_port = get_param_value(&jnlp, "vmmPort")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(8208);

        info!(
            "Huawei iBMC KVM: session_id={}, kvm_port={}, vmm_port={}",
            session_id, kvm_port, vmm_port
        );

        match console_type {
            ConsoleType::Html5 => {
                let params = parse_jnlp_params(&jnlp);
                let kvm_h5_info = serde_json::to_string(&params)
                    .unwrap_or_else(|_| "{}".to_string());

                let mut extra_ports = vec![(kvm_port, "kvm".to_string())];
                if vmm_port > 0 {
                    extra_ports.push((vmm_port, "vmm".to_string()));
                }

                Ok(KvmConsoleInfo {
                    console_type: ConsoleType::Html5,
                    jnlp_content: None,
                    html5_path: Some("/src/virtualControl/kvm_h5.html".into()),
                    cookies: vec![],
                    csrf_header: None,
                    session_storage: vec![],
                    local_storage: vec![
                        ("kvmHtml5Info".into(), kvm_h5_info),
                        ("softwareName".into(), "iBMC".into()),
                        ("manufacturer".into(), "Huawei".into()),
                    ],
                    bmc_extra_ports: extra_ports,
                })
            }
            ConsoleType::Java => {
                let mut bmc_extra_ports = Vec::new();
                if vmm_port > 0 {
                    bmc_extra_ports.push((vmm_port, "vmm".to_string()));
                }

                Ok(KvmConsoleInfo {
                    console_type: ConsoleType::Java,
                    jnlp_content: Some(jnlp),
                    html5_path: None,
                    cookies: vec![KvmCookie {
                        name: "SessionId".into(),
                        value: session_id,
                    }],
                    csrf_header: None,
                    session_storage: vec![],
                    local_storage: vec![],
                    bmc_extra_ports,
                })
            }
            _ => Err(BmcError::Unsupported(
                "Huawei iBMC: unsupported console type".into(),
            )),
        }
    }

    fn rewrite_jnlp_for_proxy(
        &self,
        jnlp: &str,
        proxy_host: &str,
        proxy_port: u16,
        codebase_url: &str,
        port_map: &std::collections::HashMap<u16, u16>,
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

        result = set_param(&result, "IPA", proxy_host);
        result = set_param(&result, "IPB", proxy_host);
        result = set_param(&result, "port", &proxy_port.to_string());

        if let Some(vmm_proxy) = get_param_value(&result, "vmmPort")
            .and_then(|v| v.parse::<u16>().ok())
            .and_then(|bmc_port| port_map.get(&bmc_port))
        {
            result = set_param(&result, "vmmPort", &vmm_proxy.to_string());
        }

        result = result.replace(r#"href="/bmc/"#, r#"href="bmc/"#);

        result
    }
}

/// Replace `<param name="NAME" value="OLD"/>` → `<param name="NAME" value="NEW"/>`
fn set_param(xml: &str, name: &str, new_val: &str) -> String {
    let pattern = format!(r#"name="{}" value=""#, name);
    let Some(name_pos) = xml.find(&pattern) else {
        return xml.to_string();
    };
    let val_start = name_pos + pattern.len();
    let Some(val_end_offset) = xml[val_start..].find('"') else {
        return xml.to_string();
    };
    let mut out = String::with_capacity(xml.len());
    out.push_str(&xml[..val_start]);
    out.push_str(new_val);
    out.push_str(&xml[val_start + val_end_offset..]);
    out
}

fn get_param_value(xml: &str, name: &str) -> Option<String> {
    let pattern = format!(r#"name="{}" value=""#, name);
    let name_pos = xml.find(&pattern)?;
    let val_start = name_pos + pattern.len();
    let val_end_offset = xml[val_start..].find('"')?;
    Some(xml[val_start..val_start + val_end_offset].to_string())
}

/// 解析 JNLP 中所有 `<param name="x" value="y"/>` 为 HashMap
fn parse_jnlp_params(xml: &str) -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    let pattern = r#"<param name=""#;
    let mut search_from = 0;

    while let Some(pos) = xml[search_from..].find(pattern) {
        let abs_pos = search_from + pos;
        let name_start = abs_pos + pattern.len();
        let Some(name_end) = xml[name_start..].find('"') else {
            break;
        };
        let name = &xml[name_start..name_start + name_end];

        let after_name = name_start + name_end;
        let value_marker = r#"value=""#;
        if let Some(val_offset) = xml[after_name..].find(value_marker) {
            let val_start = after_name + val_offset + value_marker.len();
            if let Some(val_end) = xml[val_start..].find('"') {
                let value = &xml[val_start..val_start + val_end];
                if let Ok(n) = value.parse::<i64>() {
                    params.insert(name.to_string(), serde_json::json!(n));
                } else {
                    params.insert(name.to_string(), serde_json::json!(value));
                }
                search_from = val_start + val_end;
                continue;
            }
        }
        search_from = after_name;
    }
    params
}

async fn export_and_download_jnlp(
    creds: &BmcCreds,
    manager_path: &str,
    tok: Option<&str>,
) -> BmcResult<String> {
    let export_path = format!(
        "{}/KvmService/Actions/KvmService.ExportKvmStartupFile",
        manager_path
    );
    debug!("Huawei iBMC: exporting KVM startup file via {}", export_path);
    redfish_post_with_token(
        creds,
        &export_path,
        &serde_json::json!({"Type": "URI", "Content": "/tmp/web/kvm.jnlp", "Mode": "Shared"}),
        tok,
    )
    .await?;

    let download_path = format!(
        "{}/Actions/Oem/Huawei/Manager.GeneralDownload",
        manager_path
    );
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| BmcError::internal(format!("HTTP client error: {}", e)))?;

    let url = creds.redfish_url(&download_path);
    debug!("Huawei iBMC: downloading JNLP from {}", url);
    let mut req = client
        .post(&url)
        .json(&serde_json::json!({"TransferProtocol": "HTTPS", "Path": "/tmp/web/kvm.jnlp"}));
    if let Some(t) = tok {
        req = req.header("X-Auth-Token", t);
    } else {
        req = req.basic_auth(&creds.username, Some(&creds.password));
    }

    let resp = req
        .send()
        .await
        .map_err(|e| BmcError::internal(format!("JNLP download failed: {}", e)))?;

    if !resp.status().is_success() {
        return Err(BmcError::internal(format!(
            "JNLP download returned HTTP {}",
            resp.status()
        )));
    }

    resp.text()
        .await
        .map_err(|e| BmcError::internal(format!("JNLP read failed: {}", e)))
}
