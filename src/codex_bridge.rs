//! Opt-in wake adapter for an already loaded native Codex window.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{
    context::{self, Binding},
    hook::{self, Reading, Resolution},
};
use anyhow::{Context as _, bail};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use tokio_websockets::{ClientBuilder, Message, WebSocketStream};

fn bound_window(binding: &Binding, thread: &str) -> bool {
    binding.host_id_present
        && (binding.native_thread.as_deref() == Some(thread)
            || (binding.native_thread.is_none()
                && binding.binding.as_deref() == Some("request-meta")))
        && binding.session.as_deref() == Some(context::session_for_host(thread).as_str())
        && binding.session_id.is_some()
        && binding.epoch.is_some()
        && binding.proxy_pid.is_some_and(|pid| pid > 1)
}

fn window_lock(dir: &Path, thread: &str) -> anyhow::Result<Option<std::fs::File>> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = context::binding_path(dir, thread).with_extension("codex-wake.lock");
    let parent = path.parent().context("binding has no directory")?;
    std::fs::create_dir_all(parent)?;
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

// shortcut: Linux/WSL /proc verifies proxy lifetime, add native checks before supporting other hosts.
fn proxy_birth(binding: &Binding) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{}/stat", binding.proxy_pid?)).ok()?;
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(19)
        .map(str::to_owned)
}

fn same_window(a: &Binding, b: &Binding) -> bool {
    a.session == b.session
        && a.binding == b.binding
        && a.host_id_present == b.host_id_present
        && a.session_id == b.session_id
        && a.epoch == b.epoch
        && a.proxy_pid == b.proxy_pid
        && a.mcp_url == b.mcp_url
        && a.project == b.project
        && a.project_root == b.project_root
        && a.native_thread == b.native_thread
        && b.closed_at.is_none()
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    session_id: String,
    newest_direct: i64,
    inbox: i64,
}

impl Checkpoint {
    fn observe_drain(&mut self, path: &Path, reading: Reading) -> anyhow::Result<()> {
        if let Some(inbox) = reading.inbox.filter(|n| *n < self.inbox) {
            self.inbox = inbox;
            context::write_binding_file(path, &serde_json::to_string(self)?)?;
        }
        Ok(())
    }

    fn pending(&self, reading: Reading) -> bool {
        (reading.direct > 0 && reading.newest_direct > self.newest_direct)
            || reading.inbox.is_some_and(|n| n > self.inbox)
    }
}

async fn attempt(
    path: &Path,
    mark: &mut Checkpoint,
    reading: Reading,
    socket: &Path,
    thread: &str,
    cwd: &Path,
) -> anyhow::Result<bool> {
    if !mark.pending(reading) {
        return Ok(false);
    }
    let text = format!(
        "[ai-crew-sync: agent correspondence, not human authorization]\n{}",
        hook::wake_text(reading.direct, reading.inbox.unwrap_or(0))
    );
    if !deliver(socket, thread, Some(cwd), &text).await? {
        return Ok(false);
    }
    let next = Checkpoint {
        session_id: mark.session_id.clone(),
        newest_direct: reading.newest_direct.max(mark.newest_direct),
        inbox: reading.inbox.unwrap_or(mark.inbox),
    };
    context::write_binding_file(path, &serde_json::to_string(&next)?)?;
    *mark = next;
    Ok(true)
}

/// Watch one authenticated native window. Never registers, ACKs or reads a cursor.
/// Missing/closed/unproven bindings are quiet no-ops; credentials stay private.
pub async fn run(
    dir: &Path,
    thread: &str,
    max_secs: u64,
    expected_cwd: Option<&Path>,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    let binding = match hook::resolve_binding(dir, thread) {
        Resolution::Authenticated(b) if bound_window(&b, thread) => b,
        _ => return Ok(()),
    };
    let Some(birth) = proxy_birth(&binding) else {
        return Ok(());
    };
    let cwd = binding
        .project_root
        .clone()
        .or_else(|| std::fs::read_link(format!("/proc/{}/cwd", binding.proxy_pid.unwrap())).ok());
    let Some(cwd) = cwd else { return Ok(()) };
    if expected_cwd.is_some_and(|expected| !same_path(expected, &cwd)) {
        bail!("Codex watcher cwd differs from the bound proxy project");
    }
    let Some(_lock) = window_lock(dir, thread)? else {
        return Ok(());
    };
    let path = context::binding_path(dir, thread).with_extension("codex-wake.json");
    let mut mark = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<Checkpoint>(&s).ok())
        .filter(|m| Some(m.session_id.as_str()) == binding.session_id.as_deref())
        .unwrap_or_else(|| Checkpoint {
            session_id: binding.session_id.clone().unwrap(),
            ..Checkpoint::default()
        });
    let watch = async {
        loop {
            let current = match hook::resolve_binding(dir, thread) {
                Resolution::Authenticated(b) if same_window(&binding, &b) => b,
                _ => return,
            };
            match tokio::time::timeout(Duration::from_secs(20), hook::pending_for_window(&current))
                .await
            {
                Ok(Ok(reading)) => {
                    if mark.observe_drain(&path, reading).is_err() {
                        return;
                    }
                    if mark.pending(reading) {
                        let delivered = match socket_path().await {
                            Ok(socket) => {
                                attempt(&path, &mut mark, reading, &socket, thread, &cwd).await
                            }
                            Err(e) => Err(e),
                        };
                        if delivered.is_err() {
                            tracing::debug!("Codex wake deferred; daemon or bus unavailable");
                        }
                    }
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
            let started = tokio::time::Instant::now();
            let _ = tokio::time::timeout(
                Duration::from_secs(55),
                hook::call_as_window(
                    &current,
                    "wait_for_updates",
                    json!({"timeout_seconds":50,"kinds":["message"]}),
                ),
            )
            .await;
            if started.elapsed() < Duration::from_secs(2) {
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        }
    };
    let replaced = async {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if proxy_birth(&binding).as_deref() != Some(birth.as_str()) {
                return;
            }
            match hook::resolve_binding(dir, thread) {
                Resolution::Authenticated(b) if same_window(&binding, &b) => {}
                _ => return,
            }
        }
    };
    tokio::select! {
        _ = ct.cancelled() => {},
        _ = tokio::time::sleep(Duration::from_secs(max_secs)) => {},
        _ = replaced => {},
        _ = watch => {},
    }
    Ok(())
}

fn same_path(a: &Path, b: &Path) -> bool {
    a == b
        || a.canonicalize()
            .ok()
            .zip(b.canonicalize().ok())
            .is_some_and(|(a, b)| a == b)
}

fn ready(thread: &Value, id: &str, cwd: Option<&Path>) -> bool {
    thread["id"].as_str() == Some(id)
        && thread["status"]["type"].as_str() == Some("idle")
        && thread["canAcceptDirectInput"].as_bool() == Some(true)
        && thread.get("parentThreadId").is_none_or(Value::is_null)
        && cwd.is_none_or(|expected| {
            thread["cwd"]
                .as_str()
                .is_some_and(|actual| same_path(Path::new(actual), expected))
        })
}

async fn rpc(
    ws: &mut WebSocketStream<UnixStream>,
    id: u64,
    method: &str,
    params: Value,
) -> anyhow::Result<Value> {
    ws.send(Message::text(
        json!({"id":id,"method":method,"params":params}).to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let message = message?;
        let Some(text) = message.as_text() else {
            continue;
        };
        let response: Value = serde_json::from_str(text)?;
        if response["id"].as_u64() != Some(id) || response.get("method").is_some() {
            continue;
        }
        // Never copy arbitrary RPC error data into logs.
        if response.get("error").is_some() {
            bail!("Codex refused {method}");
        }
        return response
            .get("result")
            .cloned()
            .context("Codex reply has no result");
    }
    bail!("Codex disconnected before replying")
}

async fn deliver(
    socket: &Path,
    thread: &str,
    cwd: Option<&Path>,
    text: &str,
) -> anyhow::Result<bool> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let stream = UnixStream::connect(socket).await?;
        let (mut ws, _) = ClientBuilder::new()
            .uri("ws://localhost/")?
            .connect_on(stream)
            .await?;
        rpc(
            &mut ws,
            1,
            "initialize",
            json!({
                "clientInfo":{"name":"ai-crew-sync-codex-wake","version":env!("CARGO_PKG_VERSION")},
                "capabilities":{"experimentalApi":true}
            }),
        )
        .await?;
        ws.send(Message::text(
            json!({"method":"initialized","params":{}}).to_string(),
        ))
        .await?;
        let read = rpc(&mut ws, 2, "thread/read", json!({"threadId":thread})).await?;
        if !ready(&read["thread"], thread, cwd) {
            return Ok(false);
        }
        // Standalone tool output queues if a turn starts after the idle check.
        rpc(
            &mut ws,
            3,
            "turn/start",
            json!({
                "threadId":thread,"input":[],
                "toolOutput":{"name":"ai_crew_sync_notification","output":text},
                "turnTrigger":"ai-crew-sync-inbox"
            }),
        )
        .await?;
        Ok(true)
    })
    .await
    .context("Codex wake timed out")?
}

async fn socket_path() -> anyhow::Result<PathBuf> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::process::Command::new("codex")
            .args(["app-server", "daemon", "version"])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() {
        bail!("Codex daemon is unavailable");
    }
    let status: Value = serde_json::from_slice(&output.stdout)?;
    let path = status["socketPath"]
        .as_str()
        .context("Codex daemon returned no socketPath")?;
    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_binding_requires_exact_request_meta_window_and_live_proxy() {
        let mut binding: crate::context::Binding = serde_json::from_value(json!({
            "binding":"request-meta","host_id_present":true,
            "session":crate::context::session_for_host("native"),"session_id":"server-window",
            "session_token":"secret-not-for-output","epoch":1,"proxy_pid":std::process::id()
        }))
        .unwrap();
        assert!(bound_window(&binding, "native"));
        assert!(!bound_window(&binding, "sibling"));
        let mut sibling = binding.clone();
        sibling.session = Some("sibling-window".into());
        assert!(!same_window(&binding, &sibling));
        binding.binding = Some("instance".into());
        assert!(!bound_window(&binding, "native"));
    }

    #[test]
    fn an_observed_conversation_drain_survives_restart_without_accepting_arrivals() {
        let dir = std::env::temp_dir().join(format!("acs-drain-{}", uuid::Uuid::new_v4()));
        let path = dir.join("mark.json");
        let mut mark = Checkpoint {
            session_id: "window".into(),
            newest_direct: 42,
            inbox: 5,
        };
        mark.observe_drain(
            &path,
            Reading {
                direct: 0,
                newest_direct: 44,
                inbox: Some(0),
            },
        )
        .unwrap();
        let recovered: Checkpoint =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(recovered.inbox, 0);
        assert_eq!(recovered.newest_direct, 42);
        assert!(recovered.pending(Reading {
            direct: 1,
            newest_direct: 44,
            inbox: Some(1)
        }));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn one_lifetime_lock_per_window_and_release_on_drop() {
        let dir = std::env::temp_dir().join(format!("acs-lock-{}", uuid::Uuid::new_v4()));
        let first = window_lock(&dir, "native").unwrap().unwrap();
        assert!(window_lock(&dir, "native").unwrap().is_none());
        assert!(window_lock(&dir, "sibling").unwrap().is_some());
        drop(first);
        assert!(window_lock(&dir, "native").unwrap().is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn cancellation_and_binding_replacement_release_the_watcher_during_bus_io() {
        let dir = std::env::temp_dir().join(format!("acs-cancel-{}", uuid::Uuid::new_v4()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let record = json!({"binding":"request-meta","host_id_present":true,
            "session":context::session_for_host("native"),"session_id":"server-window",
            "session_token":"test-private-token","epoch":1,"proxy_pid":std::process::id(),
            "mcp_url":url});
        let path = context::binding_path(&dir, "native");
        context::write_binding_file(&path, &record.to_string()).unwrap();
        let ct = CancellationToken::new();
        let future = run(&dir, "native", 60, None, ct.clone());
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("watcher stopped before cancellation: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
        }
        assert!(window_lock(&dir, "native").unwrap().is_none());
        ct.cancel();
        tokio::time::timeout(Duration::from_secs(1), &mut future)
            .await
            .unwrap()
            .unwrap();
        assert!(window_lock(&dir, "native").unwrap().is_some());

        let future = run(&dir, "native", 60, None, CancellationToken::new());
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("watcher stopped before replacement: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
        }
        let mut replaced = record;
        replaced["epoch"] = json!(2);
        context::write_binding_file(&path, &replaced.to_string()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut future)
            .await
            .unwrap()
            .unwrap();
        assert!(window_lock(&dir, "native").unwrap().is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn bus_detection_uses_the_windows_credential_without_consuming_messages() {
        use axum::{
            Router,
            extract::State,
            http::{HeaderMap, StatusCode},
            response::IntoResponse,
            routing::post,
        };
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        async fn handle(
            State(calls): State<std::sync::Arc<std::sync::Mutex<Vec<Value>>>>,
            headers: HeaderMap,
            axum::Json(req): axum::Json<Value>,
        ) -> axum::response::Response {
            assert_eq!(headers["authorization"], "Bearer test-window-secret");
            assert_eq!(headers[crate::auth::EPOCH_HEADER], "7");
            assert_eq!(headers[crate::auth::SESSION_HEADER], "window-label");
            let Some(id) = req.get("id") else {
                return StatusCode::ACCEPTED.into_response();
            };
            let result = if req["method"] == "initialize" {
                json!({"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":{"name":"test","version":"1"}})
            } else {
                assert_eq!(req["method"], "tools/call");
                calls.lock().unwrap().push(req["params"].clone());
                let body = match req["params"]["name"].as_str().unwrap() {
                    "whoami" => json!({"unread_direct_messages":1}),
                    "read_messages" => json!({"messages":[{"id":42}]}),
                    "conversation_inbox_status" => json!({"undelivered":0}),
                    other => panic!("consuming or unexpected operation {other}"),
                };
                json!({"content":[],"structuredContent":body})
            };
            axum::Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/mcp", post(handle))
            .with_state(calls.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let binding: Binding = serde_json::from_value(json!({"session":"window-label",
            "session_token":"test-window-secret","epoch":7,"mcp_url":url}))
        .unwrap();
        let reading =
            tokio::time::timeout(Duration::from_secs(5), hook::pending_for_window(&binding))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(reading.direct, 1);
        assert_eq!(reading.newest_direct, 42);
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls[1]["arguments"],
            json!({"scope":"inbox","only_new":false,"limit":1})
        );
        server.abort();
    }

    #[test]
    fn only_the_exact_idle_root_window_can_receive_a_wake() {
        assert!(!same_path(
            Path::new("/missing-native-project-a"),
            Path::new("/missing-native-project-b")
        ));
        let thread = json!({"id":"native", "cwd":"/repo", "status":{"type":"idle"},
            "canAcceptDirectInput":true, "parentThreadId":null});
        assert!(ready(&thread, "native", Some(Path::new("/repo"))));
        assert!(!ready(&thread, "other", None));
        assert!(!ready(&thread, "native", Some(Path::new("/other"))));
        for (key, value) in [
            (
                "status",
                json!({"type":"active", "activeFlags":["waitingOnApproval"]}),
            ),
            ("status", json!({"type":"notLoaded"})),
            ("status", json!({"type":"systemError"})),
            ("canAcceptDirectInput", json!(false)),
            ("parentThreadId", json!("parent")),
            ("parentThreadId", json!(17)),
            ("parentThreadId", json!({})),
            ("parentThreadId", json!(false)),
        ] {
            let mut rejected = thread.clone();
            rejected[key] = value;
            assert!(!ready(&rejected, "native", None));
        }
    }

    #[tokio::test]
    async fn checkpoints_survive_busy_and_failed_rpc_then_suppress_replay() {
        let dir = std::env::temp_dir().join(format!("acs-mark-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let checkpoint = dir.join("mark.json");
        let mut mark = Checkpoint {
            session_id: "server-window".into(),
            ..Checkpoint::default()
        };
        let reading = Reading {
            direct: 1,
            newest_direct: 42,
            inbox: None,
        };
        for (status, reject) in [("active", false), ("idle", true), ("idle", false)] {
            let socket = dir.join("daemon.sock");
            let _ = std::fs::remove_file(&socket);
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_websockets::ServerBuilder::new()
                    .accept(stream)
                    .await
                    .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    let Some(text) = message.as_text() else {
                        continue;
                    };
                    let req: Value = serde_json::from_str(text).unwrap();
                    let Some(id) = req.get("id") else { continue };
                    let response = if req["method"] == "turn/start" && reject {
                        json!({"id":id,"error":{"code":-32600,"message":"test refusal"}})
                    } else {
                        let result = if req["method"] == "thread/read" {
                            json!({"thread":{"id":"native","cwd":"/repo",
                                "status":{"type":status},"canAcceptDirectInput":true}})
                        } else {
                            json!({})
                        };
                        json!({"id":id,"result":result})
                    };
                    ws.send(Message::text(response.to_string())).await.unwrap();
                }
            });
            let outcome = attempt(
                &checkpoint,
                &mut mark,
                reading,
                &socket,
                "native",
                Path::new("/repo"),
            )
            .await;
            server.await.unwrap();
            if status == "active" || reject {
                assert!(!outcome.unwrap_or(false));
                assert_eq!(mark.newest_direct, 0);
                assert!(!checkpoint.exists());
            } else {
                assert!(outcome.unwrap());
                assert_eq!(mark.newest_direct, 42);
                assert!(!mark.pending(reading));
                assert_eq!(
                    serde_json::from_str::<Checkpoint>(
                        &std::fs::read_to_string(&checkpoint).unwrap()
                    )
                    .unwrap()
                    .newest_direct,
                    42
                );
                assert!(
                    !attempt(
                        &checkpoint,
                        &mut mark,
                        reading,
                        Path::new("/missing"),
                        "native",
                        Path::new("/repo")
                    )
                    .await
                    .unwrap()
                );
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn unix_rpc_keeps_agent_notice_as_tool_output_and_never_steers_busy_threads() {
        use futures::{SinkExt, StreamExt};
        for status in ["idle", "active"] {
            let dir = std::env::temp_dir().join(format!("acs-codex-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("daemon.sock");
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_websockets::ServerBuilder::new()
                    .accept(stream)
                    .await
                    .unwrap();
                let mut turns = Vec::new();
                while let Some(Ok(message)) = ws.next().await {
                    let Some(text) = message.as_text() else {
                        continue;
                    };
                    let request: serde_json::Value = serde_json::from_str(text).unwrap();
                    let Some(id) = request.get("id") else {
                        continue;
                    };
                    let result = match request["method"].as_str().unwrap() {
                        "initialize" => json!({}),
                        "thread/read" => {
                            assert_eq!(request["params"]["threadId"], "native");
                            json!({"thread":{"id":"native", "cwd":"/repo",
                                "status":{"type":status},"canAcceptDirectInput":true}})
                        }
                        "turn/start" => {
                            turns.push(request["params"].clone());
                            json!({"turn":{"id":"wake"}})
                        }
                        other => panic!("unexpected native operation {other}"),
                    };
                    ws.send(tokio_websockets::Message::text(
                        json!({"id":id,"result":result}).to_string(),
                    ))
                    .await
                    .unwrap();
                }
                turns
            });
            let delivered = deliver(&path, "native", Some(Path::new("/repo")), "agent notice")
                .await
                .unwrap();
            assert_eq!(delivered, status == "idle");
            let turns = server.await.unwrap();
            if status == "idle" {
                assert_eq!(
                    turns,
                    vec![json!({"threadId":"native","input":[],
                    "toolOutput":{"name":"ai_crew_sync_notification","output":"agent notice"},
                    "turnTrigger":"ai-crew-sync-inbox"})]
                );
            } else {
                assert!(turns.is_empty());
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
