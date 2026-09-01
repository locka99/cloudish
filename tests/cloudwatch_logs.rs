//! Integration tests for the CloudWatch Logs service.
//!
//! Run with: `cargo test --test cloudwatch_logs -- --nocapture`

use aws_sdk_cloudwatchlogs::{
    Client as LogsClient,
    config::{BehaviorVersion, Credentials, Region},
    types::InputLogEvent,
};
use uuid::Uuid;

// ── Server startup ────────────────────────────────────────────────────────────

fn start_server_sync() -> u16 {
    use std::net::TcpListener as StdListener;

    let std_listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();

    let data_dir = format!("data/test_{port}");

    if std::path::Path::new(&data_dir).exists() {
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            let state = std::sync::Arc::new(
                cloudish::AppState::new_with_data_dir(&data_dir)
                    .await
                    .unwrap(),
            );
            let app = cloudish::build_app(state).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });

    for _ in 0..20 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    port
}

async fn port() -> u16 {
    static INIT: std::sync::Once = std::sync::Once::new();
    static PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

    let p = PORT.load(std::sync::atomic::Ordering::Acquire);
    if p != 0 {
        return p;
    }

    tokio::task::spawn_blocking(|| {
        INIT.call_once(|| {
            let port = start_server_sync();
            PORT.store(port, std::sync::atomic::Ordering::Release);
        });
        PORT.load(std::sync::atomic::Ordering::Acquire)
    })
    .await
    .unwrap()
}

fn logs_client(port: u16) -> LogsClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_cloudwatchlogs::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    LogsClient::from_conf(conf)
}

// ── Drop guards ───────────────────────────────────────────────────────────────

struct LogGroupGuard {
    port: u16,
    name: String,
}

impl Drop for LogGroupGuard {
    fn drop(&mut self) {
        use std::io::{Read, Write};
        let body = format!(r#"{{"logGroupName":"{}"}}"#, self.name);
        let req = format!(
            "POST / HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: Logs_20140328.DeleteLogGroup\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(format!("127.0.0.1:{}", self.port)) {
            let _ = stream.write_all(req.as_bytes());
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_describe_log_group() {
    let p = port().await;
    let client = logs_client(p);

    let group_name = format!("/test/group/{}", Uuid::new_v4());
    let _guard = LogGroupGuard { port: p, name: group_name.clone() };

    client
        .create_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("CreateLogGroup failed");

    let resp = client
        .describe_log_groups()
        .log_group_name_prefix(&group_name)
        .send()
        .await
        .expect("DescribeLogGroups failed");

    let groups = resp.log_groups();
    assert!(
        groups.iter().any(|g| g.log_group_name().unwrap_or("") == group_name),
        "expected log group to appear in describe"
    );
}

#[tokio::test]
async fn test_create_log_stream() {
    let p = port().await;
    let client = logs_client(p);

    let group_name = format!("/test/stream-group/{}", Uuid::new_v4());
    let stream_name = format!("my-stream-{}", Uuid::new_v4());
    let _guard = LogGroupGuard { port: p, name: group_name.clone() };

    client
        .create_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("CreateLogGroup failed");

    client
        .create_log_stream()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("CreateLogStream failed");

    let resp = client
        .describe_log_streams()
        .log_group_name(&group_name)
        .log_stream_name_prefix(&stream_name)
        .send()
        .await
        .expect("DescribeLogStreams failed");

    let streams = resp.log_streams();
    assert!(
        streams.iter().any(|s| s.log_stream_name().unwrap_or("") == stream_name),
        "expected log stream to appear in describe"
    );
}

#[tokio::test]
async fn test_put_and_get_log_events() {
    let p = port().await;
    let client = logs_client(p);

    let group_name = format!("/test/events-group/{}", Uuid::new_v4());
    let stream_name = format!("events-stream-{}", Uuid::new_v4());
    let _guard = LogGroupGuard { port: p, name: group_name.clone() };

    client
        .create_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("CreateLogGroup failed");

    client
        .create_log_stream()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("CreateLogStream failed");

    let now_ms = chrono::Utc::now().timestamp_millis();
    let messages = ["first event", "second event", "third event"];

    client
        .put_log_events()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms)
                .message(messages[0])
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms + 1)
                .message(messages[1])
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms + 2)
                .message(messages[2])
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("PutLogEvents failed");

    let resp = client
        .get_log_events()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("GetLogEvents failed");

    let events = resp.events();
    assert_eq!(events.len(), 3, "expected 3 events");
    let returned_msgs: Vec<&str> = events.iter().map(|e| e.message().unwrap_or("")).collect();
    for msg in &messages {
        assert!(returned_msgs.contains(msg), "missing message: {msg}");
    }
}

#[tokio::test]
async fn test_filter_log_events() {
    let p = port().await;
    let client = logs_client(p);

    let group_name = format!("/test/filter-group/{}", Uuid::new_v4());
    let stream_name = format!("filter-stream-{}", Uuid::new_v4());
    let _guard = LogGroupGuard { port: p, name: group_name.clone() };

    client
        .create_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("CreateLogGroup failed");

    client
        .create_log_stream()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("CreateLogStream failed");

    let now_ms = chrono::Utc::now().timestamp_millis();

    client
        .put_log_events()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms)
                .message("ERROR: something went wrong")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms + 1)
                .message("INFO: all good here")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms + 2)
                .message("ERROR: another problem")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("PutLogEvents failed");

    let resp = client
        .filter_log_events()
        .log_group_name(&group_name)
        .filter_pattern("ERROR")
        .send()
        .await
        .expect("FilterLogEvents failed");

    let events = resp.events();
    assert_eq!(events.len(), 2, "expected 2 ERROR events, got {}", events.len());
    for event in events {
        assert!(
            event.message().unwrap_or("").contains("ERROR"),
            "non-ERROR event returned: {}",
            event.message().unwrap_or("")
        );
    }
}

#[tokio::test]
async fn test_delete_log_group() {
    let p = port().await;
    let client = logs_client(p);

    let group_name = format!("/test/delete-group/{}", Uuid::new_v4());
    let stream_name = format!("delete-stream-{}", Uuid::new_v4());
    // No guard - we delete manually

    client
        .create_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("CreateLogGroup failed");

    client
        .create_log_stream()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("CreateLogStream failed");

    let now_ms = chrono::Utc::now().timestamp_millis();
    client
        .put_log_events()
        .log_group_name(&group_name)
        .log_stream_name(&stream_name)
        .log_events(
            InputLogEvent::builder()
                .timestamp(now_ms)
                .message("a log message")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("PutLogEvents failed");

    client
        .delete_log_group()
        .log_group_name(&group_name)
        .send()
        .await
        .expect("DeleteLogGroup failed");

    let resp = client
        .describe_log_groups()
        .log_group_name_prefix(&group_name)
        .send()
        .await
        .expect("DescribeLogGroups after delete failed");

    assert!(
        resp.log_groups().is_empty(),
        "expected no log groups after delete"
    );
}
