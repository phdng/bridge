#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    collections::HashMap,
    env,
    future::IntoFuture,
    sync::{Arc, OnceLock},
    thread,
    time::{Duration, Instant},
};

use auto_launch::AutoLaunchBuilder;
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket},
        Path, Query, Request, State, WebSocketUpgrade,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes as MediaBytes;
use futures_util::{SinkExt, StreamExt};
use http::{Method, StatusCode};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use tao::event_loop::EventLoopBuilder;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc::channel, Mutex},
};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tray_icon::{
    menu::{CheckMenuItem, Menu, MenuEvent, MenuItem, PredefinedMenuItem},
    TrayIconBuilder, TrayIconEvent,
};

mod adb;
mod ios_lan_scanner;
mod ios_provider;
mod registry;

use ios_lan_scanner::IosLanScanner;
use ios_provider::IosProvider;
use registry::DeviceRegistry;

use webrtc::{
    api::{interceptor_registry::register_default_interceptors, media_engine::{MediaEngine, MIME_TYPE_H264}, APIBuilder},
    ice_transport::{
        ice_connection_state::RTCIceConnectionState,
        ice_server::RTCIceServer,
    },
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration,
        policy::ice_transport_policy::RTCIceTransportPolicy,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{track_local_static_sample::TrackLocalStaticSample, TrackLocal},
};

fn start_browser() {
    open::that_detached("https://app.tangoapp.dev/?desktop=true").unwrap();
}

async fn handle_websocket(ws: WebSocket) {
    let (mut ws_writer, mut ws_reader) = ws.split();
    let adb_stream = adb::connect_or_start().await.unwrap();
    // Reduce latency for small writes.
    let _ = adb_stream.set_nodelay(true);
    let (mut adb_reader, mut adb_writer) = adb_stream.into_split();

    let (ws_to_adb_sender, mut ws_to_adb_receiver) = channel::<Bytes>(16);
    let (adb_to_ws_sender, mut adb_to_ws_receiver) = channel::<Vec<u8>>(16);

    tokio::join!(
        async move {
            while let Some(Ok(message)) = ws_reader.next().await {
                // Don't merge with `if` above to ignore other message types
                if let Message::Binary(packet) = message {
                    if ws_to_adb_sender.send(packet).await.is_err() {
                        break;
                    }
                }
            }
        },
        async move {
            while let Some(buf) = ws_to_adb_receiver.recv().await {
                if adb_writer.write_all(buf.as_ref()).await.is_err() {
                    break;
                }
            }
            let _ = adb_writer.shutdown().await;
        },
        async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match adb_reader.read(&mut buf).await {
                    Ok(0) | Err(_) => {
                        break;
                    }
                    Ok(n) => {
                        if adb_to_ws_sender.send(buf[..n].to_vec()).await.is_err() {
                            break;
                        }
                    }
                }
            }
        },
        async move {
            while let Some(buf) = adb_to_ws_receiver.recv().await {
                if ws_writer.send(Message::binary(buf)).await.is_err() {
                    break;
                }
            }
            let _ = ws_writer.close().await;
        }
    );
}

#[derive(Clone)]
struct AppState {
    registry: std::sync::Arc<DeviceRegistry>,
    rtc_sessions: Arc<Mutex<HashMap<String, CancellationToken>>>,
    rtc_ice: Arc<RtcIceService>,
}

#[derive(Deserialize)]
struct IosRtcOfferRequest {
    sdp: RTCSessionDescription,
    profile: Option<String>,
    #[serde(rename = "iceTransportPolicy")]
    ice_transport_policy: Option<String>,
}

#[derive(Serialize)]
struct IosRtcOfferResponse {
    sdp: RTCSessionDescription,
    profile: String,
    port: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RtcIceConfigResponse {
    ice_servers: Vec<RtcIceServerConfig>,
    ice_transport_policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(default)]
    turn_enabled: bool,
    #[serde(default)]
    cf_app_id_configured: bool,
    #[serde(default)]
    cf_api_token_configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RtcIceServerConfig {
    urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RtcConfigQuery {
    force_relay: Option<String>,
}

struct RtcIceCache {
    config: RtcIceConfigResponse,
    expires_at: Instant,
}

struct RtcIceService {
    client: reqwest::Client,
    cf_app_id: Option<String>,
    cf_api_token: Option<String>,
    turn_enabled: bool,
    ttl: u64,
    refresh_skew: u64,
    fallback_stun_urls: Vec<String>,
    cache: Mutex<Option<RtcIceCache>>,
}

impl RtcIceService {
    fn from_env() -> Self {
        let turn_enabled = env::var("RTC_TURN_ENABLED")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        let ttl = env::var("CF_TURN_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86_400);
        let refresh_skew = env::var("RTC_TURN_REFRESH_SKEW_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let fallback_stun_urls = env::var("RTC_STUN_URLS")
            .unwrap_or_else(|_| "stun:stun.cloudflare.com:3478,stun:stun.cloudflare.com:53".to_string())
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        Self {
            client: reqwest::Client::new(),
            cf_app_id: env::var("CF_TURN_APP_ID").ok().filter(|v| !v.is_empty()),
            cf_api_token: env::var("CF_TURN_API_TOKEN").ok().filter(|v| !v.is_empty()),
            turn_enabled,
            ttl,
            refresh_skew,
            fallback_stun_urls,
            cache: Mutex::new(None),
        }
    }

    async fn config(&self, force_relay: bool) -> RtcIceConfigResponse {
        let mut config = if self.turn_enabled {
            match self.cloudflare_config().await {
                Ok(mut config) => {
                    config.source = Some("cloudflare".to_string());
                    config.error = None;
                    config
                }
                Err(e) => {
                    println!("[rtc-ice] cloudflare TURN config failed: {e}; using STUN fallback");
                    let mut config = self.stun_fallback_config();
                    config.error = Some(e);
                    config
                }
            }
        } else {
            let mut config = self.stun_fallback_config();
            config.error = Some("RTC_TURN_ENABLED is not enabled".to_string());
            config
        };

        config.ice_transport_policy = if force_relay { "relay" } else { "all" }.to_string();
        config.turn_enabled = self.turn_enabled;
        config.cf_app_id_configured = self.cf_app_id.is_some();
        config.cf_api_token_configured = self.cf_api_token.is_some();
        config
    }

    async fn cloudflare_config(&self) -> Result<RtcIceConfigResponse, String> {
        let app_id = self
            .cf_app_id
            .as_deref()
            .ok_or_else(|| "missing CF_TURN_APP_ID".to_string())?;
        let api_token = self
            .cf_api_token
            .as_deref()
            .ok_or_else(|| "missing CF_TURN_API_TOKEN".to_string())?;

        let now = Instant::now();
        {
            let cache = self.cache.lock().await;
            if let Some(cache) = cache.as_ref() {
                if now < cache.expires_at {
                    return Ok(cache.config.clone());
                }
            }
        }

        #[derive(Serialize)]
        struct TurnRequest {
            ttl: u64,
        }

        let url = format!(
            "https://rtc.live.cloudflare.com/v1/turn/keys/{app_id}/credentials/generate-ice-servers"
        );
        let config = self
            .client
            .post(url)
            .bearer_auth(api_token)
            .json(&TurnRequest { ttl: self.ttl })
            .send()
            .await
            .map_err(|e| format!("request: {e}"))?
            .error_for_status()
            .map_err(|e| format!("status: {e}"))?
            .json::<RtcIceConfigResponse>()
            .await
            .map_err(|e| format!("decode: {e}"))?;

        let valid_for = self.ttl.saturating_sub(self.refresh_skew).max(60);
        let mut cache = self.cache.lock().await;
        *cache = Some(RtcIceCache {
            config: config.clone(),
            expires_at: Instant::now() + Duration::from_secs(valid_for),
        });
        Ok(config)
    }

    fn stun_fallback_config(&self) -> RtcIceConfigResponse {
        let ice_servers = if self.fallback_stun_urls.is_empty() {
            Vec::new()
        } else {
            vec![RtcIceServerConfig {
                urls: self.fallback_stun_urls.clone(),
                username: None,
                credential: None,
            }]
        };
        RtcIceConfigResponse {
            ice_servers,
            ice_transport_policy: "all".to_string(),
            source: Some("stun-fallback".to_string()),
            turn_enabled: self.turn_enabled,
            cf_app_id_configured: self.cf_app_id.is_some(),
            cf_api_token_configured: self.cf_api_token.is_some(),
            error: None,
        }
    }
}

fn parse_force_relay(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "TRUE" | "yes" | "YES"))
}

fn to_webrtc_config(config: &RtcIceConfigResponse) -> RTCConfiguration {
    let ice_servers = config
        .ice_servers
        .iter()
        .map(|server| RTCIceServer {
            urls: server.urls.clone(),
            username: server.username.clone().unwrap_or_default(),
            credential: server.credential.clone().unwrap_or_default(),
            ..Default::default()
        })
        .collect();

    let ice_transport_policy = if config.ice_transport_policy == "relay" {
        RTCIceTransportPolicy::Relay
    } else {
        RTCIceTransportPolicy::All
    };

    RTCConfiguration {
        ice_servers,
        ice_transport_policy,
        ..Default::default()
    }
}

async fn list_devices(State(state): State<AppState>) -> impl IntoResponse {
    let devices = state.registry.list_unified_devices().await;
    Json(devices)
}

async fn ios_stream_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    Ok(ws.on_upgrade(move |socket| handle_ios_stream(socket, device.ip, 7001)))
}

async fn ios_stream_eco_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    Ok(ws.on_upgrade(move |socket| handle_ios_stream(socket, device.ip, 7002)))
}

async fn ios_h264_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    Ok(ws.on_upgrade(move |socket| handle_ios_stream(socket, device.ip, 7003)))
}

async fn ios_h264_worker_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    Ok(ws.on_upgrade(move |socket| handle_ios_stream(socket, device.ip, 7004)))
}

async fn ios_rtc_offer_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<IosRtcOfferRequest>,
) -> Result<Json<IosRtcOfferResponse>, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    let (profile, port) = select_ios_rtc_profile(&device.ip, request.profile.as_deref());
    println!("[ios-rtc] selected profile={profile} port={port} ip={}", device.ip);

    let rtc_cancel = CancellationToken::new();
    if let Some(previous) = state
        .rtc_sessions
        .lock()
        .await
        .insert(id.clone(), rtc_cancel.clone())
    {
        previous.cancel();
    }

    let force_relay = request.ice_transport_policy.as_deref() == Some("relay");
    let ice_config = state.rtc_ice.config(force_relay).await;
    println!(
        "[ios-rtc] ice servers={} policy={}",
        ice_config.ice_servers.len(),
        ice_config.ice_transport_policy
    );

    let answer = match create_ios_rtc_answer(device.ip, request.sdp, rtc_cancel, port, profile.clone(), ice_config).await {
        Ok(answer) => answer,
        Err(e) => {
            if let Some(cancel) = state.rtc_sessions.lock().await.remove(&id) {
                cancel.cancel();
            }
            return Err((StatusCode::BAD_GATEWAY, e).into_response());
        }
    };

    Ok(Json(IosRtcOfferResponse { sdp: answer, profile, port }))
}

async fn rtc_config_handler(
    State(state): State<AppState>,
    Query(query): Query<RtcConfigQuery>,
) -> Json<RtcIceConfigResponse> {
    let force_relay = parse_force_relay(query.force_relay.as_deref());
    Json(state.rtc_ice.config(force_relay).await)
}

async fn ios_rtc_close_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(cancel) = state.rtc_sessions.lock().await.remove(&id) {
        println!("[ios-rtc] close requested {id}");
        cancel.cancel();
    }
    StatusCode::NO_CONTENT
}

fn select_ios_rtc_profile(ip: &str, requested: Option<&str>) -> (String, u16) {
    match requested.unwrap_or("auto").to_ascii_lowercase().as_str() {
        "lan" => ("lan".to_string(), 7003),
        "wan" => ("wan".to_string(), 7006),
        _ if is_lan_candidate_ip(ip) => ("lan".to_string(), 7003),
        _ => ("wan".to_string(), 7006),
    }
}

fn is_lan_candidate_ip(ip: &str) -> bool {
    match ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => {
            v4.is_private() || v4.is_loopback() || v4.is_link_local()
        }
        Ok(IpAddr::V6(v6)) => {
            let first = v6.segments()[0];
            v6.is_loopback() || (first & 0xffc0) == 0xfe80
        }
        Err(_) => false,
    }
}

async fn create_ios_rtc_answer(
    ip: String,
    offer: RTCSessionDescription,
    rtc_cancel: CancellationToken,
    port: u16,
    profile: String,
    ice_config: RtcIceConfigResponse,
) -> Result<RTCSessionDescription, String> {
    let mut media_engine = MediaEngine::default();
    media_engine
        .register_default_codecs()
        .map_err(|e| format!("register codecs: {e}"))?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)
        .map_err(|e| format!("register interceptors: {e}"))?;

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .build();

    let config = to_webrtc_config(&ice_config);

    let peer_connection = Arc::new(
        api.new_peer_connection(config)
            .await
            .map_err(|e| format!("new peer connection: {e}"))?,
    );

    let video_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_H264.to_owned(),
            clock_rate: 90000,
            ..Default::default()
        },
        "ios-video".to_string(),
        "zxtouch".to_string(),
    ));

    let rtp_sender = peer_connection
        .add_track(Arc::clone(&video_track) as Arc<dyn TrackLocal + Send + Sync>)
        .await
        .map_err(|e| format!("add video track: {e}"))?;

    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        while rtp_sender.read(&mut rtcp_buf).await.is_ok() {}
    });

    let cancel_for_peer_state = rtc_cancel.clone();
    peer_connection.on_peer_connection_state_change(Box::new(move |state: RTCPeerConnectionState| {
        let cancel = cancel_for_peer_state.clone();
        Box::pin(async move {
            println!("[ios-rtc] peer state {state}");
            if matches!(state, RTCPeerConnectionState::Disconnected | RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
                cancel.cancel();
            }
        })
    }));

    let cancel_for_ice_state = rtc_cancel.clone();
    peer_connection.on_ice_connection_state_change(Box::new(move |state: RTCIceConnectionState| {
        let cancel = cancel_for_ice_state.clone();
        Box::pin(async move {
            println!("[ios-rtc] ice state {state}");
            if matches!(state, RTCIceConnectionState::Disconnected | RTCIceConnectionState::Failed | RTCIceConnectionState::Closed) {
                cancel.cancel();
            }
        })
    }));

    peer_connection
        .set_remote_description(offer)
        .await
        .map_err(|e| format!("set remote description: {e}"))?;

    let answer = peer_connection
        .create_answer(None)
        .await
        .map_err(|e| format!("create answer: {e}"))?;
    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    peer_connection
        .set_local_description(answer)
        .await
        .map_err(|e| format!("set local description: {e}"))?;
    let _ = tokio::time::timeout(Duration::from_secs(3), gather_complete.recv()).await;

    let local_desc = peer_connection
        .local_description()
        .await
        .ok_or_else(|| "missing local description".to_string())?;

    tokio::spawn(stream_ios_h264_to_rtc(
        ip,
        Arc::clone(&peer_connection),
        video_track,
        rtc_cancel,
        port,
        profile,
    ));

    Ok(local_desc)
}

async fn stream_ios_h264_to_rtc(
    ip: String,
    peer_connection: Arc<webrtc::peer_connection::RTCPeerConnection>,
    video_track: Arc<TrackLocalStaticSample>,
    cancel: CancellationToken,
    port: u16,
    profile: String,
) {
    let addr = format!("{}:{}", ip, port);
    println!("[ios-rtc] open tcp {addr} profile={profile}");
    let stream = match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => {
            println!("[ios-rtc] tcp connect failed");
            let _ = peer_connection.close().await;
            return;
        }
    };
    let _ = stream.set_nodelay(true);
    let mut reader = stream;
    let mut last_frame_at: Option<Instant> = None;
    let mut sample_duration_us = rtc_initial_sample_duration_us(&profile);

    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => break,
            result = read_zxh_frame(&mut reader) => result,
        };

        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) => break,
        };

        if frame.payload.is_empty() {
            continue;
        }

        let now = Instant::now();
        if let Some(last) = last_frame_at {
            let observed_us = now.duration_since(last).as_micros() as f64;
            // Track real source pacing smoothly. During zxtouch input, capture can dip below
            // 30fps; fixed 30fps RTP timestamps make the browser grow its jitter buffer.
            let bounded_us = observed_us.clamp(30_000.0, 60_000.0);
            sample_duration_us = (sample_duration_us * 0.80) + (bounded_us * 0.20);
        }
        last_frame_at = Some(now);

        if video_track
            .write_sample(&Sample {
                data: MediaBytes::from(frame.payload),
                duration: Duration::from_micros(sample_duration_us.round() as u64),
                ..Default::default()
            })
            .await
            .is_err()
        {
            let _ = peer_connection.close().await;
            return;
        }
    }

    println!("[ios-rtc] close tcp {port} profile={profile}");
    let _ = peer_connection.close().await;
}

fn rtc_initial_sample_duration_us(profile: &str) -> f64 {
    match profile {
        "lan" | "wan" => 33_333.0,
        _ => 33_333.0,
    }
}

struct ZxhFrame {
    timestamp_us: u64,
    payload: Vec<u8>,
}

async fn read_zxh_frame(reader: &mut TcpStream) -> Result<Option<ZxhFrame>, std::io::Error> {
    let mut magic = [0u8; 4];
    match reader.read_exact(&mut magic).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    if &magic[..3] != b"ZXH" {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad zxh magic"));
    }

    let (timestamp_us, payload_len) = match magic[3] {
        b'2' => {
            let mut rest = [0u8; 48];
            reader.read_exact(&mut rest).await?;
            let ts = u64::from_be_bytes(rest[12..20].try_into().unwrap_or([0; 8]));
            let payload_len = u32::from_be_bytes(rest[44..48].try_into().unwrap_or([0; 4])) as usize;
            (ts, payload_len)
        }
        b'1' => {
            let mut rest = [0u8; 16];
            reader.read_exact(&mut rest).await?;
            let ts = u64::from_be_bytes(rest[4..12].try_into().unwrap_or([0; 8]));
            let payload_len = u32::from_be_bytes(rest[12..16].try_into().unwrap_or([0; 4])) as usize;
            (ts, payload_len)
        }
        _ => {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad zxh version"));
        }
    };

    if payload_len == 0 || payload_len > 4 * 1024 * 1024 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad zxh payload len"));
    }

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(ZxhFrame { timestamp_us, payload }))
}

async fn ios_zxtouch_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    let registry = Arc::clone(&state.registry);
    Ok(ws.on_upgrade(move |socket| async move {
        registry.begin_ios_control();
        println!("[ios-zxtouch] control opened {id}");
        handle_ios_zxtouch(socket, device.ip).await;
        println!("[ios-zxtouch] control closed {id}");
        registry.end_ios_control();
    }))
}

async fn handle_ios_stream(mut ws: WebSocket, ip: String, port: u16) {
    let addr = format!("{}:{}", ip, port);
    let stream = match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => {
            let _ = ws.close().await;
            return;
        }
    };

    // Reduce latency for small writes.
    let _ = stream.set_nodelay(true);

    let (mut ws_writer, mut ws_reader) = ws.split();
    let (mut tcp_reader, mut tcp_writer) = stream.into_split();
    let cancel = CancellationToken::new();
    let cancel_reader = cancel.clone();
    let cancel_writer = cancel.clone();

    let ws_read_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_reader.cancelled() => break,
                message = ws_reader.next() => {
                    match message {
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(_)) => break,
                    }
                }
            }
        }
        cancel_reader.cancel();
    });

    let tcp_to_ws = tokio::spawn(async move {
        // Smaller chunks reduce end-to-end buffering latency.
        let mut buf = vec![0u8; 8 * 1024];
        let mut stat_bytes: u64 = 0;
        let mut stat_chunks: u64 = 0;
        let mut stat_send_us: u128 = 0;
        let mut stat_at = Instant::now();
        loop {
            tokio::select! {
                _ = cancel_writer.cancelled() => break,
                result = tcp_reader.read(&mut buf) => {
                    let n = match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    let send_start = Instant::now();
                    if ws_writer.send(Message::binary(buf[..n].to_vec())).await.is_err() {
                        break;
                    }
                    if port == 7003 {
                        stat_bytes += n as u64;
                        stat_chunks += 1;
                        stat_send_us += send_start.elapsed().as_micros();
                        let elapsed = stat_at.elapsed();
                        if elapsed >= Duration::from_secs(3) {
                            let secs = elapsed.as_secs_f64();
                            let kbps = (stat_bytes as f64 * 8.0 / 1000.0) / secs;
                            let avg_send_us = if stat_chunks == 0 { 0.0 } else { stat_send_us as f64 / stat_chunks as f64 };
                            println!(
                                "ios_h264_relay port=7003 kbps={:.0} chunks={} avg_ws_send_us={:.1}",
                                kbps, stat_chunks, avg_send_us
                            );
                            stat_bytes = 0;
                            stat_chunks = 0;
                            stat_send_us = 0;
                            stat_at = Instant::now();
                        }
                    }
                }
            }
        }
        cancel_writer.cancel();
    });

    let _ = tokio::join!(ws_read_task, tcp_to_ws);
    let _ = tcp_writer.shutdown().await;
}

async fn handle_ios_zxtouch(mut ws: WebSocket, ip: String) {
    let addr = format!("{}:6000", ip);
    let stream = match TcpStream::connect(addr).await {
        Ok(stream) => stream,
        Err(_) => {
            let _ = ws.close().await;
            return;
        }
    };

    let _ = stream.set_nodelay(true);

    let (mut ws_writer, mut ws_reader) = ws.split();
    let (mut tcp_reader, mut tcp_writer) = stream.into_split();
    let cancel = CancellationToken::new();
    let cancel_reader = cancel.clone();
    let cancel_writer = cancel.clone();
    let cancel_move = cancel.clone();
    let cancel_command_writer = cancel.clone();
    let (command_tx, mut command_rx) = channel::<Vec<u8>>(64);
    let latest_move = Arc::new(Mutex::new(None::<Vec<u8>>));

    let command_writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_command_writer.cancelled() => break,
                command = command_rx.recv() => {
                    let Some(command) = command else { break; };
                    if tcp_writer.write_all(&command).await.is_err() { break; }
                }
            }
        }
        cancel_command_writer.cancel();
    });

    let latest_move_for_pump = Arc::clone(&latest_move);
    let command_tx_for_pump = command_tx.clone();
    let move_pump = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = cancel_move.cancelled() => break,
                _ = tick.tick() => {
                    let command = latest_move_for_pump.lock().await.take();
                    if let Some(command) = command {
                        if command_tx_for_pump.send(command).await.is_err() { break; }
                    }
                }
            }
        }
        cancel_move.cancel();
    });

    let latest_move_for_reader = Arc::clone(&latest_move);
    let command_tx_for_reader = command_tx.clone();
    let ws_to_tcp = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_reader.cancelled() => break,
                message = ws_reader.next() => {
                    match message {
                        Some(Ok(Message::Binary(buf))) => {
                            if forward_ios_zxtouch_command(buf.to_vec(), &latest_move_for_reader, &command_tx_for_reader).await.is_err() { break; }
                        }
                        Some(Ok(Message::Text(text))) => {
                            if forward_ios_zxtouch_command(text.as_bytes().to_vec(), &latest_move_for_reader, &command_tx_for_reader).await.is_err() { break; }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(_)) => break,
                    }
                }
            }
        }
        cancel_reader.cancel();
    });

    let tcp_to_ws = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            tokio::select! {
                _ = cancel_writer.cancelled() => break,
                result = tcp_reader.read(&mut buf) => {
                    let n = match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if ws_writer.send(Message::binary(buf[..n].to_vec())).await.is_err() {
                        break;
                    }
                }
            }
        }
        cancel_writer.cancel();
    });

    let _ = tokio::join!(ws_to_tcp, command_writer, move_pump, tcp_to_ws);
}

async fn forward_ios_zxtouch_command(
    command: Vec<u8>,
    latest_move: &Arc<Mutex<Option<Vec<u8>>>>,
    command_tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<(), ()> {
    if command.starts_with(b"1012") {
        *latest_move.lock().await = Some(command);
        return Ok(());
    }

    if let Some(move_command) = latest_move.lock().await.take() {
        command_tx.send(move_command).await.map_err(|_| ())?;
    }
    command_tx.send(command).await.map_err(|_| ())
}

const ARG_AUTO_RUN: &str = "--auto-run";

#[cfg(debug_assertions)]
const PROXY_HOST: &str = "https://tangoapp.dev";
#[cfg(not(debug_assertions))]
const PROXY_HOST: &str = "https://tangoapp.dev";

static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[axum::debug_handler]
async fn proxy_request(request: Request) -> Result<Response, Response> {
    println!("proxy_request: {} {}", request.method(), request.uri());

    let url = Url::options()
        .base_url(Some(&Url::parse(PROXY_HOST).unwrap()))
        .parse(&request.uri().to_string())
        .map_err(|_| (StatusCode::BAD_REQUEST, "Bad Request").into_response())?;

    let mut headers = request.headers().clone();
    headers.insert("Host", url.host_str().unwrap().parse().unwrap());

    let (client, request) = CLIENT
        .get_or_init(|| reqwest::Client::new())
        .request(request.method().clone(), url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(
            request.into_body().into_data_stream(),
        ))
        .build_split();

    let request = request.map_err(|_| (StatusCode::BAD_REQUEST, "Bad Request").into_response())?;

    let response = client
        .execute(request)
        .await
        .map_err(|_| (StatusCode::BAD_GATEWAY, "Bad Gateway").into_response())?;

    Ok((
        response.status(),
        response.headers().clone(),
        axum::body::Body::new(reqwest::Body::from(response)),
    )
        .into_response())
}

static SINGLE_INSTANCE: OnceLock<single_instance::SingleInstance> = OnceLock::new();

#[tokio::main]
async fn main() {
    // macOS app bundle prevents re-launching by default
    #[cfg(not(target_os = "macos"))]
    {
        use single_instance::SingleInstance;

        let single_instance =
            SINGLE_INSTANCE.get_or_init(|| SingleInstance::new("tango-bridge-rs").unwrap());
        println!(
            "single_instance.is_single(): {}",
            single_instance.is_single()
        );
        if !single_instance.is_single() {
            start_browser();
            return;
        }
    }

    // Very strangely, running this in `tokio::spawn`
    // will cause `listener` to not stop on Windows
    adb::connect_or_start()
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    #[cfg(debug_assertions)]
    {
        use tracing::Level;
        use tracing_subscriber::FmtSubscriber;

        let subscriber = FmtSubscriber::builder()
            // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
            // will be written to stdout.
            .with_max_level(Level::TRACE)
            // completes the builder.
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    let app = Router::new()
        .route("/devices", get(list_devices))
        .route("/ios/{id}/stream", get(ios_stream_handler))
        .route("/ios/{id}/stream-eco", get(ios_stream_eco_handler))
        .route("/ios/{id}/h264", get(ios_h264_handler))
        .route("/ios/{id}/h264-worker", get(ios_h264_worker_handler))
        .route("/rtc/config", get(rtc_config_handler))
        .route("/ios/{id}/rtc/offer", post(ios_rtc_offer_handler))
        .route("/ios/{id}/rtc/close", post(ios_rtc_close_handler))
        .route("/ios/{id}/zxtouch", get(ios_zxtouch_handler))
        .nest(
            "/bridge",
            Router::new()
                .route("/ping", get(|| async { env!("CARGO_PKG_VERSION") }))
                .route(
                    "/",
                    get(|ws: WebSocketUpgrade| async { ws.on_upgrade(handle_websocket) }),
                )
                .route_layer(cors_layer()),
        )
        .route_layer(cors_layer())
        .fallback(proxy_request);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:15037")
        .await
        .unwrap();

    let token = CancellationToken::new();

    let registry = DeviceRegistry::new();
    let ios_provider = IosProvider::new(
        registry.clone(),
        IosLanScanner::new(Duration::from_millis(600), 64),
        Duration::from_secs(10),
    );
    let _ios_task = ios_provider.start();

    let app = app.with_state(AppState {
        registry,
        rtc_sessions: Arc::new(Mutex::new(HashMap::new())),
        rtc_ice: Arc::new(RtcIceService::from_env()),
    });

    let mut server = {
        let token = token.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(token.cancelled_owned())
                .into_future()
                .await
        });
        Some(server)
    };
    println!("server started on thread {:?}", thread::current().id());

    // Temporarily disable auto-opening the website on startup.
    // if env::args().all(|arg| arg != ARG_AUTO_RUN) {
    //     start_browser()
    // }

    let menu_open = MenuItem::new("Open", true, None);

    let auto_launch = AutoLaunchBuilder::new()
        .set_app_name("Tango")
        .set_app_path(env::current_exe().unwrap().to_str().unwrap())
        .set_args(&[ARG_AUTO_RUN])
        .set_use_launch_agent(true)
        .build()
        .unwrap();
    let menu_auto_run = CheckMenuItem::new(
        "Run at startup",
        true,
        auto_launch.is_enabled().unwrap(),
        None,
    );

    let menu_quit = MenuItem::new("Quit", true, None);

    let tray_menu = Menu::new();
    tray_menu
        .append_items(&[
            &menu_open,
            &menu_auto_run,
            &PredefinedMenuItem::separator(),
            &menu_quit,
        ])
        .unwrap();

    let menu_receiver = MenuEvent::receiver();
    let tray_receiver = TrayIconEvent::receiver();

    let mut tray_icon = None;

    #[allow(unused_mut)]
    let mut event_loop = EventLoopBuilder::new().build();

    #[cfg(target_os = "macos")]
    {
        use tao::platform::macos::EventLoopExtMacOS;

        // https://github.com/glfw/glfw/issues/1552
        event_loop.set_activation_policy(tao::platform::macos::ActivationPolicy::Accessory);
    }

    println!("before main loop");

    event_loop.run(move |event, _, control_flow| {
        *control_flow =
            tao::event_loop::ControlFlow::WaitUntil(Instant::now() + Duration::from_millis(16));

        if let tao::event::Event::Reopen { .. } = event {
            start_browser();
            return;
        }

        if let tao::event::Event::NewEvents(tao::event::StartCause::Init) = event {
            let image = image::load_from_memory_with_format(
                include_bytes!("../tango.png"),
                image::ImageFormat::Png,
            )
            .unwrap()
            .into_rgba8();
            let (width, height) = image.dimensions();
            let rgba = image.into_raw();
            let icon = tray_icon::Icon::from_rgba(rgba, width, height).unwrap();

            tray_icon = Some(
                TrayIconBuilder::new()
                    .with_tooltip("Tango (rs)")
                    .with_icon(icon)
                    .with_menu(Box::new(tray_menu.clone()))
                    .build()
                    .unwrap(),
            );

            #[cfg(target_os = "macos")]
            unsafe {
                use core_foundation::runloop::{CFRunLoopGetMain, CFRunLoopWakeUp};

                let rl = CFRunLoopGetMain();
                CFRunLoopWakeUp(rl);
            }
        }

        if let Ok(event) = menu_receiver.try_recv() {
            if event.id == menu_open.id() {
                start_browser();
                return;
            }

            if event.id == menu_auto_run.id() {
                if auto_launch.is_enabled().unwrap() {
                    auto_launch.disable().unwrap();
                } else {
                    auto_launch.enable().unwrap();
                }
                menu_auto_run.set_checked(auto_launch.is_enabled().unwrap());
                return;
            }

            if event.id == menu_quit.id() {
                tray_icon.take();

                token.cancel();
                println!("trigger token cancel");

                let server = server.take().unwrap();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(server)
                        .unwrap()
                        .unwrap();
                    println!("server exited");
                });

                println!("exiting main loop");
                *control_flow = tao::event_loop::ControlFlow::Exit;
                return;
            }
        }

        if let Ok(TrayIconEvent::Click {
            button: tray_icon::MouseButton::Left,
            button_state: tray_icon::MouseButtonState::Down,
            ..
        }) = tray_receiver.try_recv()
        {
            start_browser();
            return;
        }
    });
}

fn cors_layer() -> CorsLayer {
    // Web UI is often hosted on HTTPS but talks to a local bridge
    // (e.g. https://app.example.com -> http://localhost:15037).
    // Use permissive CORS here to avoid deployment-specific origin drift.
    CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .allow_origin(Any)
        .allow_private_network(true)
}
