pub mod ami_web;
pub mod dell_idrac;
pub mod error;
pub mod generic_redfish;
pub mod guoxin;
pub mod hpe_ilo;
pub mod huanan;
pub mod huawei_ibmc;
pub mod huawei_imc_old;
pub mod inspur;
pub mod ipmitool;
pub mod lenovo_xcc;
pub mod asrockrack;
pub mod nettrix;
pub mod sugon;
pub mod types;

use async_trait::async_trait;
use error::{BmcError, BmcResult};
use serde::Serialize;
use types::*;

pub const BMC_TYPE_REDFISH: &str = "redfish";
pub const BMC_TYPE_IPMI: &str = "ipmi";
pub const BMC_TYPE_IDRAC: &str = "idrac";
pub const BMC_TYPE_ILO: &str = "ilo";
pub const BMC_TYPE_IBMC: &str = "ibmc";
//pub const BMC_TYPE_IBMC_OLD: &str = "ibmc-old";
pub const BMC_TYPE_XCC: &str = "xcc";
pub const BMC_TYPE_AMI: &str = "ami";
pub const BMC_TYPE_ASROCKRACK: &str = "asrockrack";
pub const BMC_TYPE_SUGON: &str = "sugon";
pub const BMC_TYPE_NETTRIX: &str = "nettrix";
pub const BMC_TYPE_INSPUR: &str = "inspur";
pub const BMC_TYPE_GUOXIN: &str = "guoxin";
pub const BMC_TYPE_HUANAN: &str = "huanan";
pub const BMC_TYPE_UNKNOWN: &str = "unknown";
pub const SUPPORTED_BMC_TYPES: &[&str] = &[
    BMC_TYPE_REDFISH,
    BMC_TYPE_IPMI,
    BMC_TYPE_IDRAC,
    BMC_TYPE_ILO,
    BMC_TYPE_IBMC,
    //BMC_TYPE_IBMC_OLD,
    BMC_TYPE_XCC,
    BMC_TYPE_AMI,
    BMC_TYPE_ASROCKRACK,
    BMC_TYPE_SUGON,
    BMC_TYPE_NETTRIX,
    BMC_TYPE_INSPUR,
    BMC_TYPE_GUOXIN,
    BMC_TYPE_HUANAN,
];

#[derive(Debug, Clone, Serialize)]
pub struct BmcTypeDescriptor {
    pub value: &'static str,
    pub label: &'static str,
    pub protocol: &'static str,
}

pub fn supported_bmc_type_descriptors() -> Vec<BmcTypeDescriptor> {
    vec![
        BmcTypeDescriptor {
            value: BMC_TYPE_REDFISH,
            label: "Redfish (Generic)",
            protocol: "redfish",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_IPMI,
            label: "IPMI",
            protocol: "ipmi",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_IDRAC,
            label: "Dell iDRAC",
            protocol: "redfish",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_ILO,
            label: "HPE iLO",
            protocol: "redfish",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_IBMC,
            label: "Huawei iBMC",
            protocol: "redfish",
        },
        //  BmcTypeDescriptor { value: BMC_TYPE_IBMC_OLD, label: "Huawei iBMC-old (Legacy CGI)", protocol: "http" },
        BmcTypeDescriptor {
            value: BMC_TYPE_XCC,
            label: "Lenovo XCC",
            protocol: "redfish",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_AMI,
            label: "AMI Web (HTTP)",
            protocol: "http",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_ASROCKRACK,
            label: "ASRockRack (HTTP)",
            protocol: "http",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_SUGON,
            label: "Sugon AMI (Legacy)",
            protocol: "http",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_NETTRIX,
            label: "Nettrix BMC",
            protocol: "redfish",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_INSPUR,
            label: "Inspur BMC",
            protocol: "hybrid",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_GUOXIN,
            label: "Guoxin / GOOXI BMC",
            protocol: "hybrid",
        },
        BmcTypeDescriptor {
            value: BMC_TYPE_HUANAN,
            label: "Huanan BMC (SP-X HTML5)",
            protocol: "http",
        },
    ]
}

fn normalized_bmc_key(bmc_type: &str) -> String {
    bmc_type
        .trim()
        .to_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

pub fn canonical_bmc_type(bmc_type: &str) -> String {
    match normalized_bmc_key(bmc_type).as_str() {
        "redfish" | "genericredfish" => BMC_TYPE_REDFISH.to_string(),
        "ipmi" => BMC_TYPE_IPMI.to_string(),
        "idrac" | "idrac8" | "idrac9" | "dellidrac" => BMC_TYPE_IDRAC.to_string(),
        "ilo" | "ilo5" | "ilo6" | "hpeilo" | "hpilo" => BMC_TYPE_ILO.to_string(),
        "ibmc" | "huawei" | "huaweibmc" | "huaweiibmc" => BMC_TYPE_IBMC.to_string(),
        // "ibmcold" | "imcold" | "huaweiimcold" | "huaweibmcold" | "huaweiimcoldcgi" | "huaweiimcoldbmc" => {
        //     BMC_TYPE_IBMC_OLD.to_string()
        // }
        "xcc" | "lenovoxcc" => BMC_TYPE_XCC.to_string(),
        "ami" | "amiweb" => BMC_TYPE_AMI.to_string(),
        "sugon" | "sugonweb" | "dawning" => BMC_TYPE_SUGON.to_string(),
        "asrockrack" | "megaracapi" | "spx" | "asrock" => BMC_TYPE_ASROCKRACK.to_string(),
        "nettrix" | "ningchang" | "ningchangbmc" => BMC_TYPE_NETTRIX.to_string(),
        "inspur" | "langchao" | "inspurbmc" => BMC_TYPE_INSPUR.to_string(),
        "guoxin" | "gooxi" | "gooxibmc" => BMC_TYPE_GUOXIN.to_string(),
        "huanan" | "zakj" | "huananzhi" => BMC_TYPE_HUANAN.to_string(),
        "unknown" => BMC_TYPE_UNKNOWN.to_string(),
        _ => BMC_TYPE_REDFISH.to_string(),
    }
}

pub fn is_supported_bmc_type(input: &str) -> bool {
    SUPPORTED_BMC_TYPES.contains(&input.trim())
}

#[async_trait]
pub trait BmcProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn test_connection(&self, creds: &BmcCreds) -> BmcResult<bool>;

    async fn get_power_state(&self, creds: &BmcCreds) -> BmcResult<String>;

    async fn power_action(&self, creds: &BmcCreds, action: &str) -> BmcResult<String>;

    async fn get_system_info(&self, creds: &BmcCreds) -> BmcResult<SystemInfo>;

    async fn get_processors(&self, creds: &BmcCreds) -> BmcResult<Vec<ProcessorInfo>>;

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>>;

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>>;

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>>;

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo>;

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo>;

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>>;

    async fn get_storage_controllers(
        &self,
        _creds: &BmcCreds,
    ) -> BmcResult<Vec<StorageControllerInfo>> {
        Ok(vec![])
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>>;

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()>;

    fn console_types(&self) -> Vec<ConsoleType> {
        vec![]
    }

    async fn get_kvm_console(
        &self,
        _creds: &BmcCreds,
        _console_type: &ConsoleType,
    ) -> BmcResult<KvmConsoleInfo> {
        Err(BmcError::Unsupported("KVM console not supported by this provider".into()))
    }

    fn rewrite_jnlp_for_proxy(
        &self,
        jnlp: &str,
        _proxy_host: &str,
        _proxy_port: u16,
        _codebase_url: &str,
        _port_map: &std::collections::HashMap<u16, u16>,
    ) -> String {
        jnlp.to_string()
    }
}

/// Auto-detect the best BMC type for a given host.
/// Priority: ASRockRack > Redfish > Sugon > AMI Web > IPMI
pub async fn detect_bmc_type(creds: &BmcCreds) -> String {
    use tokio::time::{timeout, Duration};
    use tracing::{debug, info};
    let timeout_dur = Duration::from_secs(8);

    debug!(
        "detect_bmc_type: probing {} (tls={})",
        creds.host, creds.use_tls
    );

    let asrr = asrockrack::AsrockrackProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, asrr.test_connection(creds)).await {
        info!("detect_bmc_type: ASRockRack API works for {}", creds.host);
        return BMC_TYPE_ASROCKRACK.to_string();
    }

    let redfish = generic_redfish::GenericRedfishProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, redfish.test_connection(creds)).await {
        if let Ok(sys_info) = timeout(timeout_dur, redfish.get_system_info(creds)).await {
            if let Ok(info) = sys_info {
                let mfr = info.manufacturer.as_deref().unwrap_or("");
                if mfr.contains("Huawei") {
                    info!("detect_bmc_type: Huawei iBMC detected for {}", creds.host);
                    return BMC_TYPE_IBMC.to_string();
                }
                if mfr.contains("Dell") {
                    info!("detect_bmc_type: Dell iDRAC detected for {}", creds.host);
                    return BMC_TYPE_IDRAC.to_string();
                }
                if mfr.contains("HPE") || mfr.contains("HP") {
                    info!("detect_bmc_type: HPE iLO detected for {}", creds.host);
                    return BMC_TYPE_ILO.to_string();
                }
                if mfr.contains("Lenovo") {
                    info!("detect_bmc_type: Lenovo XCC detected for {}", creds.host);
                    return BMC_TYPE_XCC.to_string();
                }
                if mfr.contains("Nettrix") {
                    info!("detect_bmc_type: Nettrix BMC detected for {}", creds.host);
                    return BMC_TYPE_NETTRIX.to_string();
                }
                if mfr.contains("Inspur") {
                    info!("detect_bmc_type: Inspur BMC detected for {}", creds.host);
                    return BMC_TYPE_INSPUR.to_string();
                }
                if mfr.contains("GOOXI") || mfr.contains("Gooxi") || mfr.contains("Guoxin") {
                    info!("detect_bmc_type: Guoxin BMC detected for {}", creds.host);
                    return BMC_TYPE_GUOXIN.to_string();
                }
                if mfr.contains("zakj") || mfr.contains("ZAKJ") || mfr.contains("Huanan") || mfr.contains("huanan") {
                    info!("detect_bmc_type: Huanan BMC detected for {}", creds.host);
                    return BMC_TYPE_HUANAN.to_string();
                }
            }
        }
        info!("detect_bmc_type: Redfish works for {}", creds.host);
        return BMC_TYPE_REDFISH.to_string();
    }

    // SP-X HTML5 BMCs (Gooxi / Huanan): probe /api/session login then check FRU manufacturer
    let gx = guoxin::GuoxinProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, gx.test_connection(creds)).await {
        let hn = huanan::HuananProvider;
        if let Ok(Ok(info)) = timeout(timeout_dur, hn.get_system_info(creds)).await {
            let mfr = info.manufacturer.as_deref().unwrap_or("");
            if mfr.contains("zakj") || mfr.contains("ZAKJ") || mfr.contains("Huanan") {
                info!("detect_bmc_type: Huanan SP-X BMC detected for {}", creds.host);
                return BMC_TYPE_HUANAN.to_string();
            }
        }
        info!("detect_bmc_type: Guoxin SP-X BMC detected for {}", creds.host);
        return BMC_TYPE_GUOXIN.to_string();
    }

    let sugon = sugon::SugonProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, sugon.test_connection(creds)).await {
        info!("detect_bmc_type: Sugon Web API works for {}", creds.host);
        return BMC_TYPE_SUGON.to_string();
    }

    let ami = ami_web::AmiWebProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, ami.test_connection(creds)).await {
        info!("detect_bmc_type: AMI Web works for {}", creds.host);
        return BMC_TYPE_AMI.to_string();
    }

    let mut ipmi_creds = creds.clone();
    ipmi_creds.port = 623;
    let ipmi = ipmitool::IpmitoolProvider;
    if let Ok(Ok(true)) = timeout(timeout_dur, ipmi.test_connection(&ipmi_creds)).await {
        info!("detect_bmc_type: IPMI works for {}", creds.host);
        return BMC_TYPE_IPMI.to_string();
    }

    info!(
        "detect_bmc_type: no BMC protocol detected for {}, returning unknown",
        creds.host
    );
    BMC_TYPE_UNKNOWN.to_string()
}

pub fn get_provider(bmc_type: &str) -> Box<dyn BmcProvider> {
    match canonical_bmc_type(bmc_type).as_str() {
        BMC_TYPE_IDRAC => Box::new(dell_idrac::DellIdracProvider),
        BMC_TYPE_ILO => Box::new(hpe_ilo::HpeIloProvider),
        BMC_TYPE_IBMC => Box::new(huawei_ibmc::HuaweiIbmcProvider),
        // BMC_TYPE_IBMC_OLD => Box::new(huawei_imc_old::HuaweiImcOldProvider),
        BMC_TYPE_XCC => Box::new(lenovo_xcc::LenovoXccProvider),
        BMC_TYPE_IPMI => Box::new(ipmitool::IpmitoolProvider),
        BMC_TYPE_AMI => Box::new(ami_web::AmiWebProvider),
        BMC_TYPE_SUGON => Box::new(sugon::SugonProvider),
        BMC_TYPE_ASROCKRACK => Box::new(asrockrack::AsrockrackProvider),
        BMC_TYPE_NETTRIX => Box::new(nettrix::NettrixProvider),
        BMC_TYPE_INSPUR => Box::new(inspur::InspurProvider),
        BMC_TYPE_GUOXIN => Box::new(guoxin::GuoxinProvider),
        BMC_TYPE_HUANAN => Box::new(huanan::HuananProvider),
        _ => Box::new(generic_redfish::GenericRedfishProvider),
    }
}

#[cfg(test)]
mod tests {
    use super::get_provider;

    #[test]
    fn huawei_aliases_route_to_ibmc_provider() {
        for bmc_type in [
            "iBMC",
            "ibmc",
            "Huawei",
            "Huawei iBMC",
            "Huawei_BMC",
            "huawei-ibmc",
        ] {
            assert_eq!(
                get_provider(bmc_type).name(),
                "Huawei iBMC",
                "alias `{}` should map to Huawei provider",
                bmc_type
            );
        }
    }

    #[test]
    fn unknown_types_fall_back_to_generic_redfish() {
        assert_eq!(get_provider("totally-unknown").name(), "GenericRedfish");
    }

    #[test]
    fn nettrix_aliases_route_to_nettrix_provider() {
        for bmc_type in ["nettrix", "Nettrix", "ningchang", "NingChangBMC"] {
            assert_eq!(
                get_provider(bmc_type).name(),
                "Nettrix Redfish",
                "alias `{}` should map to Nettrix provider",
                bmc_type
            );
        }
    }

    #[test]
    fn guoxin_aliases_route_to_guoxin_provider() {
        for bmc_type in ["guoxin", "Guoxin", "gooxi", "GooxiBMC"] {
            assert_eq!(
                get_provider(bmc_type).name(),
                "Guoxin BMC",
                "alias `{}` should map to Guoxin provider",
                bmc_type
            );
        }
    }

    #[test]
    fn huanan_aliases_route_to_huanan_provider() {
        for bmc_type in ["huanan", "Huanan", "zakj", "huananzhi"] {
            assert_eq!(
                get_provider(bmc_type).name(),
                "Huanan BMC",
                "alias `{}` should map to Huanan provider",
                bmc_type
            );
        }
    }
}
