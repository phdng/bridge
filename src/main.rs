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
        Path, Request, State, WebSocketUpgrade,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes as MediaBytes;
use futures_util::{SinkExt, StreamExt};
use http::{Method, StatusCode};
use reqwest::Url;
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
    ice_transport::ice_connection_state::RTCIceConnectionState,
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration,
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
    Json(offer): Json<RTCSessionDescription>,
) -> Result<Json<RTCSessionDescription>, Response> {
    let device = state
        .registry
        .get_ios_device(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "device not found").into_response())?;

    let rtc_cancel = CancellationToken::new();
    if let Some(previous) = state
        .rtc_sessions
        .lock()
        .await
        .insert(id.clone(), rtc_cancel.clone())
    {
        previous.cancel();
    }

    let answer = match create_ios_rtc_answer(device.ip, offer, rtc_cancel).await {
        Ok(answer) => answer,
        Err(e) => {
            if let Some(cancel) = state.rtc_sessions.lock().await.remove(&id) {
                cancel.cancel();
            }
            return Err((StatusCode::BAD_GATEWAY, e).into_response());
        }
    };

    Ok(Json(answer))
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

async fn create_ios_rtc_answer(
    ip: String,
    offer: RTCSessionDescription,
    rtc_cancel: CancellationToken,
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

    let config = RTCConfiguration::default();

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
    ));

    Ok(local_desc)
}

async fn stream_ios_h264_to_rtc(
    ip: String,
    peer_connection: Arc<webrtc::peer_connection::RTCPeerConnection>,
    video_track: Arc<TrackLocalStaticSample>,
    cancel: CancellationToken,
) {
    let addr = format!("{}:7003", ip);
    println!("[ios-rtc] open tcp {addr}");
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
    let mut last_pts_us: Option<u64> = None;

    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => break,
            result = read_zxh_frame(&mut reader) => result,
        };

        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) => break,
        };

        let duration_us = last_pts_us
            .map(|last| frame.timestamp_us.saturating_sub(last).clamp(1_000, 100_000))
            .unwrap_or(33_000);
        last_pts_us = Some(frame.timestamp_us);

        let nals = split_annex_b_nals(&frame.payload);
        if nals.is_empty() {
            continue;
        }

        for (index, nal) in nals.iter().enumerate() {
            let duration = if index + 1 == nals.len() {
                Duration::from_micros(duration_us)
            } else {
                Duration::from_micros(0)
            };

            if video_track
                .write_sample(&Sample {
                    data: MediaBytes::copy_from_slice(nal),
                    duration,
                    ..Default::default()
                })
                .await
                .is_err()
            {
                let _ = peer_connection.close().await;
                return;
            }
        }
    }

    println!("[ios-rtc] close tcp 7003");
    let _ = peer_connection.close().await;
}

fn split_annex_b_nals(payload: &[u8]) -> Vec<&[u8]> {
    let mut nals = Vec::new();
    let mut current_start: Option<usize> = None;
    let mut i = 0;

    while i + 3 <= payload.len() {
        let start_code_len = if i + 4 <= payload.len() && &payload[i..i + 4] == b"\0\0\0\x01" {
            4
        } else if &payload[i..i + 3] == b"\0\0\x01" {
            3
        } else {
            i += 1;
            continue;
        };

        if let Some(start) = current_start {
            if start < i {
                nals.push(&payload[start..i]);
            }
        }
        current_start = Some(i + start_code_len);
        i += start_code_len;
    }

    if let Some(start) = current_start {
        if start < payload.len() {
            nals.push(&payload[start..]);
        }
    }

    nals
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

    Ok(ws.on_upgrade(move |socket| handle_ios_zxtouch(socket, device.ip)))
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

    let ws_to_tcp = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_reader.cancelled() => break,
                message = ws_reader.next() => {
                    match message {
                        Some(Ok(Message::Binary(buf))) => {
                            if tcp_writer.write_all(&buf).await.is_err() { break; }
                        }
                        Some(Ok(Message::Text(text))) => {
                            if tcp_writer.write_all(text.as_bytes()).await.is_err() { break; }
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

    let _ = tokio::join!(ws_to_tcp, tcp_to_ws);
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
