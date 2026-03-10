use crate::error::BmcResult;
use crate::generic_redfish::GenericRedfishProvider;
use crate::types::*;
use crate::BmcProvider;
use async_trait::async_trait;

pub struct LenovoXccProvider;

/// Lenovo XCC provider — delegates to GenericRedfish with vendor-specific overrides
#[async_trait]
impl BmcProvider for LenovoXccProvider {
    fn name(&self) -> &str {
        "Lenovo XCC"
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
        GenericRedfishProvider.get_processors(creds).await
    }

    async fn get_memory(&self, creds: &BmcCreds) -> BmcResult<Vec<MemoryInfo>> {
        GenericRedfishProvider.get_memory(creds).await
    }

    async fn get_storage(&self, creds: &BmcCreds) -> BmcResult<Vec<StorageInfo>> {
        GenericRedfishProvider.get_storage(creds).await
    }

    async fn get_network_interfaces(
        &self,
        creds: &BmcCreds,
    ) -> BmcResult<Vec<NetworkInterfaceInfo>> {
        GenericRedfishProvider.get_network_interfaces(creds).await
    }

    async fn get_thermal(&self, creds: &BmcCreds) -> BmcResult<ThermalInfo> {
        GenericRedfishProvider.get_thermal(creds).await
    }

    async fn get_power(&self, creds: &BmcCreds) -> BmcResult<PowerInfo> {
        GenericRedfishProvider.get_power(creds).await
    }

    async fn get_pcie_devices(&self, creds: &BmcCreds) -> BmcResult<Vec<PCIeDeviceInfo>> {
        GenericRedfishProvider.get_pcie_devices(creds).await
    }

    async fn get_event_logs(
        &self,
        creds: &BmcCreds,
        limit: Option<u32>,
    ) -> BmcResult<Vec<EventLogEntry>> {
        GenericRedfishProvider.get_event_logs(creds, limit).await
    }

    async fn clear_event_logs(&self, creds: &BmcCreds) -> BmcResult<()> {
        GenericRedfishProvider.clear_event_logs(creds).await
    }
}
