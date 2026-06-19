use serde::{Deserialize, Serialize};
use std::{net::Ipv4Addr, sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::sleep};

use crate::{
    ios_lan_scanner::{local_ipv4, IosLanScanner},
    registry::DeviceRegistry,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HelloStatusPayload {
    pub tlinkauto: TLinkauto,
    pub device: IosDeviceInfo,
    pub script: ScriptStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TLinkauto {
    pub port: u16,
    pub protocols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IosDeviceInfo {
    pub system_name: String,
    pub model: String,
    pub name: String,
    pub system_version: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScriptStatus {
    pub is_playing: bool,
    pub last_error: String,
    pub last_error_ts: i64,
    pub bundle_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IosDevice {
    pub id: String,
    pub display_name: String,
    pub ip: String,
    pub status: HelloStatusPayload,
}

pub struct IosProvider {
    registry: Arc<DeviceRegistry>,
    scanner: IosLanScanner,
    interval: Duration,
}

impl IosProvider {
    pub fn new(registry: Arc<DeviceRegistry>, scanner: IosLanScanner, interval: Duration) -> Self {
        Self {
            registry,
            scanner,
            interval,
        }
    }

    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if self.registry.has_active_ios_control() {
                    sleep(self.interval).await;
                    continue;
                }

                if let Ok(local_ip) = local_ipv4() {
                    let subnet = subnet_base(local_ip);
                    let devices = self.scanner.scan_subnet(subnet).await;
                    self.registry.update_ios_devices(devices).await;
                }
                sleep(self.interval).await;
            }
        })
    }
}

fn subnet_base(ip: Ipv4Addr) -> Ipv4Addr {
    let octets = ip.octets();
    Ipv4Addr::new(octets[0], octets[1], octets[2], 0)
}
