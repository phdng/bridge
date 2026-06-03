use std::{collections::HashMap, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use serde::Serialize;
use serde_json::json;
use tokio::sync::{broadcast, RwLock};

use crate::ios_provider::IosDevice;

#[derive(Debug, Clone)]
pub enum DeviceEvent {
    IosOnline(IosDevice),
    IosOffline { id: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct UnifiedDevice {
    pub id: String,
    pub platform: String,
    pub status: String,
    pub display_name: String,
    pub meta: serde_json::Value,
    pub capabilities: Vec<String>,
}

pub struct DeviceRegistry {
    ios_devices: RwLock<HashMap<String, IosDevice>>,
    android_devices: RwLock<HashMap<String, UnifiedDevice>>,
    events: broadcast::Sender<DeviceEvent>,
    active_ios_controls: AtomicUsize,
}

impl DeviceRegistry {
    pub fn new() -> Arc<Self> {
        let (events, _) = broadcast::channel(128);
        Arc::new(Self {
            ios_devices: RwLock::new(HashMap::new()),
            android_devices: RwLock::new(HashMap::new()),
            events,
            active_ios_controls: AtomicUsize::new(0),
        })
    }

    pub fn begin_ios_control(&self) {
        self.active_ios_controls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_ios_control(&self) {
        let _ = self.active_ios_controls.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        });
    }

    pub fn has_active_ios_control(&self) -> bool {
        self.active_ios_controls.load(Ordering::Relaxed) > 0
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DeviceEvent> {
        self.events.subscribe()
    }

    pub async fn update_ios_devices(&self, devices: Vec<IosDevice>) {
        let mut ios_devices = self.ios_devices.write().await;
        let mut incoming = HashMap::new();

        for device in devices {
            incoming.insert(device.id.clone(), device);
        }

        let mut offline_ids = Vec::new();
        for id in ios_devices.keys() {
            if !incoming.contains_key(id) {
                offline_ids.push(id.clone());
            }
        }

        for id in offline_ids {
            ios_devices.remove(&id);
            let _ = self.events.send(DeviceEvent::IosOffline { id });
        }

        for (id, device) in incoming {
            let needs_emit = match ios_devices.get(&id) {
                Some(existing) => {
                    existing.ip != device.ip || existing.display_name != device.display_name
                }
                None => true,
            };
            ios_devices.insert(id.clone(), device.clone());
            if needs_emit {
                let _ = self.events.send(DeviceEvent::IosOnline(device));
            }
        }
    }

    pub async fn get_ios_device(&self, id: &str) -> Option<IosDevice> {
        let ios_devices = self.ios_devices.read().await;
        ios_devices.get(id).cloned()
    }

    pub async fn list_unified_devices(&self) -> Vec<UnifiedDevice> {
        let ios_devices = self.ios_devices.read().await;
        let android_devices = self.android_devices.read().await;
        let mut devices = Vec::with_capacity(ios_devices.len());

        for device in ios_devices.values() {
            devices.push(UnifiedDevice {
                id: device.id.clone(),
                platform: "ios".to_string(),
                status: "online".to_string(),
                display_name: device.display_name.clone(),
                meta: json!({
                    "ip": device.ip,
                    "device": device.status.device,
                    "zxtouch": device.status.zxtouch,
                    "script": device.status.script,
                }),
                capabilities: vec![
                    "stream_mpegts_ws".to_string(),
                    "stream_h264_ws".to_string(),
                    "stream_h264_worker_ws".to_string(),
                    "stream_rtc_auto".to_string(),
                    "zxtouch".to_string(),
                ],
            });
        }

        devices.extend(android_devices.values().cloned());

        devices
    }

    pub async fn update_android_devices(&self, devices: Vec<UnifiedDevice>) {
        let mut android_devices = self.android_devices.write().await;
        android_devices.clear();
        for device in devices {
            android_devices.insert(device.id.clone(), device);
        }
    }
}
