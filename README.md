# bmc-providers

A Rust library of BMC (Baseboard Management Controller) provider implementations for various server hardware vendors. Provides a unified async API for power management, system info, sensors, KVM console, and more across different protocols.

Rust 实现的 BMC 提供商库，支持多种服务器厂商的基板管理控制器，通过不同协议提供统一的异步 API，用于电源管理、系统信息、传感器、KVM 控制台等操作。

---

## Supported BMC Types / 支持的 BMC 类型

| Type | Vendor / Protocol | Base Protocol |
|------|-------------------|---------------|
| `redfish` | Generic Redfish | Redfish |
| `idrac` | Dell iDRAC | Redfish |
| `ilo` | HPE iLO | Redfish |
| `ibmc` | Huawei iBMC | Redfish |
| `xcc` | Lenovo XCC | Redfish |
| `nettrix` | Nettrix / 宁畅 | Redfish |
| `inspur` | Inspur / 浪潮 | Hybrid |
| `guoxin` | Guoxin / GOOXI / 国鑫 | Hybrid |
| `ipmi` | IPMI (via ipmitool) | IPMI |
| `ami` | AMI Web (Legacy) | HTTP |
| `asrockrack` | ASRockRack / MegaRAC | HTTP |
| `sugon` | Sugon / 曙光 | HTTP |

---

## Capabilities / 功能

Via the `BmcProvider` trait:

通过 `BmcProvider` 特性实现：

- **Connection** — `test_connection()`
- **Power** — `get_power_state()`, `power_action()` (On/Off/Reset/Cycle)
- **System Info** — `get_system_info()` (manufacturer, model, serial, BIOS, BMC version)
- **Hardware** — processors, memory, storage, storage controllers, network, PCIe
- **Sensors** — `get_thermal()` (temperatures, fans), `get_power()` (consumption, PSUs)
- **Event Logs** — `get_event_logs()`, `clear_event_logs()`
- **KVM** — `get_kvm_console()` (Java/Html5/SOL where supported)

---

## Usage / 使用示例

```rust
use bmc_providers::{get_provider, detect_bmc_type};
use bmc_providers::types::BmcCreds;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let creds = BmcCreds {
        host: "192.168.1.100".to_string(),
        port: 443,
        use_tls: true,
        username: "root".to_string(),
        password: "secret".to_string(),
        base_path: String::new(),
    };

    // Auto-detect BMC type
    let bmc_type = detect_bmc_type(&creds).await;
    println!("Detected: {}", bmc_type);

    // Or specify type manually
    let provider = get_provider(&bmc_type);

    // Test connection
    let ok = provider.test_connection(&creds).await?;
    if !ok {
        eprintln!("Connection failed");
        return Ok(());
    }

    // Get system info
    let sys = provider.get_system_info(&creds).await?;
    println!("Manufacturer: {:?}", sys.manufacturer);
    println!("Model: {:?}", sys.model);

    // Power control
    let state = provider.get_power_state(&creds).await?;
    println!("Power: {}", state);

    Ok(())
}
```

---

## BMC Type Detection / BMC 类型检测

`detect_bmc_type()` probes protocols in order:

按以下顺序检测协议：

1. ASRockRack HTTP API  
2. Redfish → selects Dell/HPE/Huawei/Lenovo/Nettrix/Inspur/Guoxin or generic by manufacturer  
3. Sugon Web API  
4. AMI Web  
5. IPMI (port 623)

If nothing matches, returns `unknown`; generic Redfish is used as fallback.

若均不匹配则返回 `unknown`，回退使用通用 Redfish。

---

## Type Normalization / 类型规范化

`canonical_bmc_type()` maps aliases to canonical types:

将各种别名映射为规范类型：

```rust
canonical_bmc_type("Huawei iBMC")  // → "ibmc"
canonical_bmc_type("iLO 5")        // → "ilo"
canonical_bmc_type("idrac9")       // → "idrac"
canonical_bmc_type("ningchang")    // → "nettrix"
```

---

## Dependencies / 依赖

- **IPMI**: For `ipmi` type, `ipmitool` must be installed on the system.
- **Redfish/HTTP**: Uses `reqwest` with `native-tls` (not rustls).

IPMI 类型需要系统已安装 `ipmitool`；Redfish/HTTP 使用 `reqwest` + `native-tls`。

---

## Crate Structure / 项目结构

```
src/
├── lib.rs              # BmcProvider trait, detect, get_provider
├── types.rs            # BmcCreds, SystemInfo, ThermalInfo, ...
├── error.rs            # BmcError, BmcResult
├── generic_redfish.rs  # Base Redfish + session pool
├── dell_idrac.rs       # Dell iDRAC
├── hpe_ilo.rs          # HPE iLO
├── huawei_ibmc.rs      # Huawei iBMC
├── lenovo_xcc.rs       # Lenovo XCC
├── nettrix.rs          # Nettrix
├── inspur.rs           # Inspur (Redfish + legacy)
├── guoxin.rs           # Guoxin / GOOXI
├── ipmitool.rs         # IPMI via ipmitool
├── ami_web.rs          # AMI Web
├── asrockrack.rs       # ASRockRack
└── sugon.rs            # Sugon
apis/                   # Vendor API documentation
```

---

## License

MIT OR Apache-2.0
