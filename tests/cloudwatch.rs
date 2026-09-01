//! Integration tests for the CloudWatch service (metrics and alarms).
//!
//! Run with: `cargo test --test cloudwatch -- --nocapture`

use aws_sdk_cloudwatch::{
    Client as CwClient,
    config::{BehaviorVersion, Credentials, Region},
    types::{MetricDatum, StandardUnit, StateValue},
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

fn cw_client(port: u16) -> CwClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_cloudwatch::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    CwClient::from_conf(conf)
}

// ── Drop guards ───────────────────────────────────────────────────────────────

struct AlarmGuard {
    port: u16,
    name: String,
}

impl Drop for AlarmGuard {
    fn drop(&mut self) {
        use std::io::{Read, Write};
        let addr = format!("127.0.0.1:{}", self.port);
        let body = format!(
            "Action=DeleteAlarms&AlarmNames.member.1={}",
            urlencoding::encode(&self.name)
        );
        let req = format!(
            "POST / HTTP/1.0\r\nHost: {addr}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(req.as_bytes());
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_put_and_list_metrics() {
    let p = port().await;
    let client = cw_client(p);

    let namespace = format!("Test/Namespace/{}", Uuid::new_v4());
    let metric_name = format!("MyMetric-{}", Uuid::new_v4());

    client
        .put_metric_data()
        .namespace(&namespace)
        .metric_data(
            MetricDatum::builder()
                .metric_name(&metric_name)
                .value(42.0)
                .unit(StandardUnit::Count)
                .build(),
        )
        .send()
        .await
        .expect("PutMetricData failed");

    let resp = client
        .list_metrics()
        .namespace(&namespace)
        .send()
        .await
        .expect("ListMetrics failed");

    let metrics = resp.metrics();
    assert!(
        metrics.iter().any(|m| m.metric_name().unwrap_or("") == metric_name
            && m.namespace().unwrap_or("") == namespace),
        "expected metric to appear in list"
    );
}

#[tokio::test]
async fn test_get_metric_statistics() {
    let p = port().await;
    let client = cw_client(p);

    let namespace = format!("Test/Stats/{}", Uuid::new_v4());
    let metric_name = format!("StatMetric-{}", Uuid::new_v4());

    // Put three data points
    for val in [10.0_f64, 20.0, 30.0] {
        client
            .put_metric_data()
            .namespace(&namespace)
            .metric_data(
                MetricDatum::builder()
                    .metric_name(&metric_name)
                    .value(val)
                    .unit(StandardUnit::Count)
                    .build(),
            )
            .send()
            .await
            .expect("PutMetricData failed");
    }

    let now = chrono::Utc::now();
    let start = (now - chrono::Duration::hours(1)).to_rfc3339();
    let end = (now + chrono::Duration::hours(1)).to_rfc3339();

    let resp = client
        .get_metric_statistics()
        .namespace(&namespace)
        .metric_name(&metric_name)
        .start_time(aws_sdk_cloudwatch::primitives::DateTime::from_str(&start, aws_sdk_cloudwatch::primitives::DateTimeFormat::DateTime).unwrap())
        .end_time(aws_sdk_cloudwatch::primitives::DateTime::from_str(&end, aws_sdk_cloudwatch::primitives::DateTimeFormat::DateTime).unwrap())
        .period(60)
        .statistics(aws_sdk_cloudwatch::types::Statistic::Sum)
        .statistics(aws_sdk_cloudwatch::types::Statistic::Average)
        .statistics(aws_sdk_cloudwatch::types::Statistic::SampleCount)
        .send()
        .await
        .expect("GetMetricStatistics failed");

    let datapoints = resp.datapoints();
    assert!(!datapoints.is_empty(), "expected at least one datapoint");
    let dp = &datapoints[0];
    assert_eq!(dp.sample_count(), Some(3.0), "expected SampleCount=3");
    assert_eq!(dp.sum(), Some(60.0), "expected Sum=60");
    assert_eq!(dp.average(), Some(20.0), "expected Average=20");
}

#[tokio::test]
async fn test_put_and_describe_alarm() {
    let p = port().await;
    let client = cw_client(p);

    let alarm_name = format!("test-alarm-{}", Uuid::new_v4());
    let _guard = AlarmGuard { port: p, name: alarm_name.clone() };

    client
        .put_metric_alarm()
        .alarm_name(&alarm_name)
        .alarm_description("A test alarm")
        .namespace("AWS/EC2")
        .metric_name("CPUUtilization")
        .statistic(aws_sdk_cloudwatch::types::Statistic::Average)
        .period(60)
        .evaluation_periods(1)
        .threshold(80.0)
        .comparison_operator(aws_sdk_cloudwatch::types::ComparisonOperator::GreaterThanThreshold)
        .send()
        .await
        .expect("PutMetricAlarm failed");

    let resp = client
        .describe_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("DescribeAlarms failed");

    let alarms = resp.metric_alarms();
    assert_eq!(alarms.len(), 1, "expected one alarm");
    let alarm = &alarms[0];
    assert_eq!(alarm.alarm_name().unwrap_or(""), alarm_name);
    assert_eq!(alarm.alarm_description().unwrap_or(""), "A test alarm");
    assert_eq!(alarm.namespace().unwrap_or(""), "AWS/EC2");
    assert_eq!(alarm.metric_name().unwrap_or(""), "CPUUtilization");
    assert_eq!(
        alarm.state_value().cloned(),
        Some(aws_sdk_cloudwatch::types::StateValue::InsufficientData)
    );
}

#[tokio::test]
async fn test_set_alarm_state() {
    let p = port().await;
    let client = cw_client(p);

    let alarm_name = format!("test-alarm-state-{}", Uuid::new_v4());
    let _guard = AlarmGuard { port: p, name: alarm_name.clone() };

    client
        .put_metric_alarm()
        .alarm_name(&alarm_name)
        .namespace("AWS/EC2")
        .metric_name("CPUUtilization")
        .statistic(aws_sdk_cloudwatch::types::Statistic::Average)
        .period(60)
        .evaluation_periods(1)
        .threshold(80.0)
        .comparison_operator(aws_sdk_cloudwatch::types::ComparisonOperator::GreaterThanThreshold)
        .send()
        .await
        .expect("PutMetricAlarm failed");

    client
        .set_alarm_state()
        .alarm_name(&alarm_name)
        .state_value(StateValue::Alarm)
        .state_reason("Manual override for testing")
        .send()
        .await
        .expect("SetAlarmState failed");

    let resp = client
        .describe_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("DescribeAlarms failed");

    let alarms = resp.metric_alarms();
    assert_eq!(alarms.len(), 1);
    assert_eq!(
        alarms[0].state_value().cloned(),
        Some(StateValue::Alarm)
    );
    assert_eq!(
        alarms[0].state_reason().unwrap_or(""),
        "Manual override for testing"
    );
}

#[tokio::test]
async fn test_delete_alarm() {
    let p = port().await;
    let client = cw_client(p);

    let alarm_name = format!("test-alarm-delete-{}", Uuid::new_v4());
    // No guard — we delete manually and verify

    client
        .put_metric_alarm()
        .alarm_name(&alarm_name)
        .namespace("AWS/EC2")
        .metric_name("CPUUtilization")
        .statistic(aws_sdk_cloudwatch::types::Statistic::Average)
        .period(60)
        .evaluation_periods(1)
        .threshold(80.0)
        .comparison_operator(aws_sdk_cloudwatch::types::ComparisonOperator::GreaterThanThreshold)
        .send()
        .await
        .expect("PutMetricAlarm failed");

    client
        .delete_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("DeleteAlarms failed");

    let resp = client
        .describe_alarms()
        .alarm_names(alarm_name)
        .send()
        .await
        .expect("DescribeAlarms after delete failed");

    assert!(
        resp.metric_alarms().is_empty(),
        "expected no alarms after delete"
    );
}
