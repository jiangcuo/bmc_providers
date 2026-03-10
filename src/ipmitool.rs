use crate::error::{BmcError, BmcResult};
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;
use tokio::process::Command;
use tokio::time::{timeout, Duration};
use tracing::{debug, warn};

pub struct IpmitoolProvider;

const IPMITOOL_TIMEOUT_SECS: u64 = 30;

async fn run_ipmitool(creds: &BmcCreds, args: &[&str]) -> BmcResult<String> {
    debug!(
        "ipmitool -H {} -p {} {}",
        creds.host,
        creds.port,
        args.join(" ")
    );

    let child = Command::new("ipmitool")
        .args([
            "-I",
            "lanplus",
            "-H",
            &creds.host,
            "-p",
            &creds.port.to_string(),
            "-U",
            &creds.username,
            "-P",
            &creds.password,
        ])
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            BmcError::internal(format!(
                "Failed to spawn ipmitool: {}. Is ipmitool installed?",
                e
            ))
        })?;

    let pid = child.id();

    match timeout(
        Duration::from_secs(IPMITOOL_TIMEOUT_SECS),
        child.wait_with_output(),
    )
    .await
    {
        Err(_) => {
            if let Some(pid) = pid {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
            warn!(
                "ipmitool {} timed out after {}s for {}",
                args.join(" "),
                IPMITOOL_TIMEOUT_SECS,
                creds.host
            );
            Err(BmcError::internal(format!(
                "ipmitool timeout after {}s: {}",
                IPMITOOL_TIMEOUT_SECS,
                args.join(" ")
            )))
        }
        Ok(Err(e)) => Err(BmcError::internal(format!("ipmitool process error: {}", e))),
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                warn!(
                    "ipmitool {} failed for {}: {}",
                    args.join(" "),
                    creds.host,
                    stderr
                );
                return Err(BmcError::internal(format!("ipmitool error: {}", stderr)));
            }

            debug!(
                "ipmitool response ({} bytes): {}",
                stdout.len(),
                &stdout[..stdout.len().min(500)]
            );
            Ok(stdout)
        }
    }
}

fn parse_field(text: &str, key: &str) -> Option<String> {
    text.lines()
        .find(|l| l.trim_start().starts_with(key))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[derive(Debug)]
struct SdrEntry {
    name: String,
    _sensor_id: String,
    status: String,
    _entity: String,
    reading: String,
}

async fn get_sdr_elist(creds: &BmcCreds) -> BmcResult<Vec<SdrEntry>> {
    let output = run_ipmitool(creds, &["sdr", "elist", "full"]).await?;
    let entries: Vec<SdrEntry> = output
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 5 {
                Some(SdrEntry {
                    name: parts[0].trim().to_string(),
                    _sensor_id: parts[1].trim().to_string(),
                    status: parts[2].trim().to_string(),
                    _entity: parts[3].trim().to_string(),
                    reading: parts[4].trim().to_string(),
                })
            } else {
                None
            }
        })
        .collect();
    debug!("sdr elist: parsed {} entries", entries.len());
    Ok(entries)
}

fn parse_reading_f64(reading: &str) -> Option<f64> {
    reading
        .split_whitespace()
        .next()
        .and_then(|v| v.parse::<f64>().ok())
}

fn is_degrees_c(reading: &str) -> bool {
    reading.contains("degrees C")
}

fn is_rpm(reading: &str) -> bool {
    reading.contains("RPM")
}

#[async_trait]
impl BmcProvider for IpmitoolProvider {
    fn name(&self) -> &str {
        "IPMI Tool"
    }

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool> {
        run_ipmitool(creds, &["mc", "info"]).await?;
        Ok(true)
    }

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String> {
        let output = run_ipmitool(creds, &["power", "status"]).await?;
        if output.contains("on") {
            Ok("On".to_string())
        } else if output.contains("off") {
            Ok("Off".to_string())
        } else {
            Ok("Unknown".to_string())
        }
    }

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String> {
        let cmd = match action {
            "on" => "on",
            "off" => "off",
            "reset" => "reset",
            "graceful_shutdown" => "soft",
            _ => return Err(BmcError::bad_request(format!("Unknown action: {}", action))),
        };
        let output = run_ipmitool(creds, &["power", cmd]).await?;
        Ok(output.trim().to_string())
    }

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo> {
        let mc_info = run_ipmitool(creds, &["mc", "info"])
            .await
            .unwrap_or_default();
        let fru = run_ipmitool(creds, &["fru", "print"])
            .await
            .unwrap_or_default();

        let fru_has_data = fru
            .lines()
            .any(|l| l.contains("Product Manufacturer") || l.contains("Board Mfg"));

        let manufacturer = if fru_has_data {
            parse_field(&fru, "Product Manufacturer").or_else(|| parse_field(&fru, "Board Mfg "))
        } else {
            parse_field(&mc_info, "Manufacturer Name")
        };

        let model = if fru_has_data {
            parse_field(&fru, "Product Name").or_else(|| parse_field(&fru, "Board Product"))
        } else {
            parse_field(&mc_info, "Product Name")
        };

        let serial = if fru_has_data {
            parse_field(&fru, "Product Serial").or_else(|| parse_field(&fru, "Board Serial"))
        } else {
            None
        };

        Ok(SystemInfo {
            manufacturer,
            model,
            serial_number: serial,
            bios_version: None,
            bmc_version: parse_field(&mc_info, "Firmware Revision"),
            hostname: None,
            power_state: Some(
                self.get_power_state(creds)
                    .await
                    .unwrap_or_else(|_| "Unknown".to_string()),
            ),
            total_cpu_count: None,
            total_memory_gib: None,
        })
    }

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>> {
        let entries = get_sdr_elist(creds).await?;
        let mut cpus: std::collections::BTreeMap<String, ProcessorInfo> =
            std::collections::BTreeMap::new();

        for e in &entries {
            if e.name.starts_with("CPU") && e.name.contains("_Stat") {
                let cpu_id = e.name.split('_').next().unwrap_or(&e.name).to_string();
                let present = e.status == "ok";
                cpus.entry(cpu_id.clone()).or_insert(ProcessorInfo {
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
            if e.name.starts_with("CPU") && e.name.contains("_Temp") && is_degrees_c(&e.reading) {
                let cpu_id = e.name.split('_').next().unwrap_or(&e.name).to_string();
                let temp = parse_reading_f64(&e.reading);
                cpus.entry(cpu_id.clone())
                    .and_modify(|p| {
                        p.temperature_celsius = temp;
                    })
                    .or_insert(ProcessorInfo {
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

        Ok(cpus.into_values().collect())
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        let entries = get_sdr_elist(creds).await?;
        let mut dimms: std::collections::BTreeMap<String, MemoryInfo> =
            std::collections::BTreeMap::new();

        for e in &entries {
            if !e.name.contains("DIMM") {
                continue;
            }

            let slot_name = if e.name.contains("_Stat") {
                e.name.replace("_Stat", "")
            } else if e.name.contains("_Temp") {
                e.name.replace("_Temp", "")
            } else {
                continue;
            };

            let is_stat = e.name.contains("_Stat");
            let is_temp = e.name.contains("_Temp");

            let (channel, slot_index) = parse_dimm_location(&slot_name);

            let entry = dimms.entry(slot_name.clone()).or_insert(MemoryInfo {
                id: slot_name.clone(),
                capacity_gib: None,
                memory_type: None,
                speed_mhz: None,
                manufacturer: None,
                serial_number: None,
                slot: Some(slot_name.clone()),
                channel,
                slot_index,
                temperature_celsius: None,
                populated: true,
                status: Some("Present".to_string()),
                part_number: None,
                rank_count: None,
                module_type: None,
                data_width_bits: None,
            });

            if is_stat {
                let populated = e.status == "ok" || e.reading.contains("Presence");
                entry.populated = populated;
                entry.status = Some(if populated {
                    "Present".to_string()
                } else {
                    "Empty".to_string()
                });
            }
            if is_temp && is_degrees_c(&e.reading) {
                entry.temperature_celsius = parse_reading_f64(&e.reading);
            }
        }

        let mut result: Vec<MemoryInfo> = dimms.into_values().collect();
        result.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(result)
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        let entries = get_sdr_elist(creds).await?;
        let drives: Vec<StorageInfo> = entries
            .iter()
            .filter(|e| {
                let n = e.name.to_lowercase();
                n.contains("hdd") || n.contains("ssd") || n.contains("drive") || n.contains("disk")
            })
            .map(|e| StorageInfo {
                id: e.name.clone(),
                name: Some(e.name.clone()),
                capacity_gib: None,
                media_type: None,
                protocol: None,
                manufacturer: None,
                model: None,
                serial_number: None,
                status: Some(e.status.clone()),
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
            .collect();
        Ok(drives)
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        let output = run_ipmitool(creds, &["lan", "print"])
            .await
            .unwrap_or_default();
        let ip = parse_field(&output, "IP Address  ");
        let mac = parse_field(&output, "MAC Address ");
        if ip.is_some() || mac.is_some() {
            Ok(vec![NetworkInterfaceInfo {
                id: "BMC_LAN".to_string(),
                name: Some("BMC Management Interface".to_string()),
                mac_address: mac,
                speed_mbps: None,
                speed_gbps: None,
                port_max_speed: None,
                link_status: Some("Up".to_string()),
                ipv4_address: ip,
                manufacturer: None,
                model: None,
                slot: None,
                associated_resource: None,
                bdf: None,
                position: None,
            }])
        } else {
            Ok(vec![])
        }
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        let entries = get_sdr_elist(creds).await?;

        let temps: Vec<TemperatureReading> = entries
            .iter()
            .filter(|e| is_degrees_c(&e.reading))
            .map(|e| TemperatureReading {
                name: e.name.clone(),
                reading_celsius: parse_reading_f64(&e.reading),
                upper_threshold: None,
                status: Some(e.status.clone()),
            })
            .collect();

        let fans: Vec<FanReading> = entries
            .iter()
            .filter(|e| is_rpm(&e.reading))
            .map(|e| FanReading {
                name: e.name.clone(),
                reading_rpm: parse_reading_f64(&e.reading).map(|v| v as u32),
                status: Some(e.status.clone()),
            })
            .collect();

        Ok(ThermalInfo {
            temperatures: temps,
            fans,
        })
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        let dcmi = run_ipmitool(creds, &["dcmi", "power", "reading"])
            .await
            .unwrap_or_default();
        let current_power = dcmi
            .lines()
            .find(|l| l.contains("Instantaneous power reading"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().split_whitespace().next())
            .and_then(|v| v.parse::<f64>().ok());

        let entries = get_sdr_elist(creds).await.unwrap_or_default();
        let supplies: Vec<PowerSupplyInfo> = entries
            .iter()
            .filter(|e| {
                let n = e.name.to_lowercase();
                (n.contains("psu") || n.contains("ps") || n.contains("power supply"))
                    && e.reading.contains("Watts")
            })
            .map(|e| PowerSupplyInfo {
                id: e.name.clone(),
                input_watts: None,
                output_watts: parse_reading_f64(&e.reading),
                capacity_watts: None,
                serial_number: None,
                firmware_version: None,
                manufacturer: None,
                model: None,
                status: Some(e.status.clone()),
            })
            .collect();

        Ok(PowerInfo {
            power_consumed_watts: current_power,
            power_capacity_watts: None,
            current_cpu_power_watts: None,
            current_memory_power_watts: None,
            redundancy_mode: None,
            redundancy_health: None,
            power_supplies: supplies,
        })
    }

    async fn get_pcie_devices(&self, _creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        Ok(vec![])
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        let output = run_ipmitool(creds, &["sel", "list"])
            .await
            .unwrap_or_default();
        let limit = limit.unwrap_or(100) as usize;
        let entries: Vec<EventLogEntry> = output
            .lines()
            .take(limit)
            .enumerate()
            .map(|(i, line)| {
                let parts: Vec<&str> = line.split('|').collect();
                EventLogEntry {
                    id: format!("{}", i + 1),
                    severity: parts.get(3).map(|s| s.trim().to_string()),
                    message: parts
                        .get(4)
                        .map(|s| s.trim().to_string())
                        .or_else(|| Some(line.to_string())),
                    created: parts.get(1).map(|s| s.trim().to_string()),
                    entry_type: Some("SEL".to_string()),
                    subject: None,
                    suggestion: None,
                    event_code: None,
                    alert_status: None,
                }
            })
            .collect();
        Ok(entries)
    }

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()> {
        run_ipmitool(creds, &["sel", "clear"]).await?;
        Ok(())
    }
}

fn parse_dimm_location(slot_name: &str) -> (Option<String>, Option<u32>) {
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
