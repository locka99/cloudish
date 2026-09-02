//! CloudWatch service emulator — metrics and alarms.
//!
//! Wire format: Smithy RPCv2 CBOR protocol.
//! Routes: POST /service/GraniteServiceVersion20100801/operation/{OperationName}
//! SigV4 credential scope: service = "monitoring"
//!
//! Storage layout (under data/cloudwatch/):
//!   metrics/{encoded_namespace}/{encoded_metric_name}.json   — array of data points
//!   alarms/{encoded_alarm_name}.json

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use chrono::DateTime;
use ciborium::value::Value as CborValue;
use serde::{Deserialize, Serialize};

use crate::{services::AppState, storage::Storage};

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricDataPoint {
    timestamp: String,
    value: f64,
    unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMetric {
    namespace: String,
    metric_name: String,
    points: Vec<MetricDataPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAlarm {
    alarm_name: String,
    alarm_description: String,
    namespace: String,
    metric_name: String,
    statistic: String,
    period: i64,
    evaluation_periods: i64,
    threshold: f64,
    comparison_operator: String,
    state_value: String,
    state_reason: String,
    alarm_arn: String,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type CborResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 2], Vec<u8>);

fn cbor_response(status: StatusCode, value: CborValue) -> CborResponse {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&value, &mut buf).unwrap_or_default();
    (
        status,
        [
            (header::CONTENT_TYPE, "application/cbor"),
            (
                axum::http::header::HeaderName::from_static("smithy-protocol"),
                "rpc-v2-cbor",
            ),
        ],
        buf,
    )
}

fn cbor_ok(value: CborValue) -> CborResponse {
    cbor_response(StatusCode::OK, value)
}

fn cbor_error(status: StatusCode, code: &str, message: &str) -> CborResponse {
    let val = CborValue::Map(vec![
        (CborValue::Text("code".to_string()), CborValue::Text(code.to_string())),
        (CborValue::Text("message".to_string()), CborValue::Text(message.to_string())),
    ]);
    cbor_response(status, val)
}

fn metric_key(namespace: &str, metric_name: &str) -> String {
    format!(
        "metrics/{}/{}.json",
        urlencoding::encode(namespace),
        urlencoding::encode(metric_name)
    )
}

fn alarm_key(name: &str) -> String {
    format!("alarms/{}.json", urlencoding::encode(name))
}

fn alarm_arn(name: &str) -> String {
    format!("arn:aws:cloudwatch:{REGION}:{ACCOUNT_ID}:alarm:{name}")
}

fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Extract a string value from a CBOR map by key.
fn cbor_get_str<'a>(map: &'a [(CborValue, CborValue)], key: &str) -> Option<&'a str> {
    map.iter().find(|(k, _)| {
        matches!(k, CborValue::Text(s) if s == key)
    }).and_then(|(_, v)| {
        if let CborValue::Text(s) = v { Some(s.as_str()) } else { None }
    })
}

/// Extract a float64 value from a CBOR map by key.
fn cbor_get_f64(map: &[(CborValue, CborValue)], key: &str) -> Option<f64> {
    map.iter().find(|(k, _)| {
        matches!(k, CborValue::Text(s) if s == key)
    }).and_then(|(_, v)| {
        match v {
            CborValue::Float(f) => Some(*f),
            CborValue::Integer(i) => Some(i128::from(*i) as f64),
            _ => None,
        }
    })
}

/// Extract an i64 value from a CBOR map by key.
fn cbor_get_i64(map: &[(CborValue, CborValue)], key: &str) -> Option<i64> {
    map.iter().find(|(k, _)| {
        matches!(k, CborValue::Text(s) if s == key)
    }).and_then(|(_, v)| {
        match v {
            CborValue::Integer(i) => Some(i128::from(*i) as i64),
            _ => None,
        }
    })
}

/// Extract an array from a CBOR map by key.
fn cbor_get_array<'a>(map: &'a [(CborValue, CborValue)], key: &str) -> Option<&'a Vec<CborValue>> {
    map.iter().find(|(k, _)| {
        matches!(k, CborValue::Text(s) if s == key)
    }).and_then(|(_, v)| {
        if let CborValue::Array(arr) = v { Some(arr) } else { None }
    })
}

/// Decode CBOR bytes into a map.
fn decode_cbor_map(bytes: &[u8]) -> Option<Vec<(CborValue, CborValue)>> {
    let value: ciborium::value::Value = ciborium::de::from_reader(bytes).ok()?;
    if let CborValue::Map(map) = value {
        Some(map)
    } else {
        None
    }
}

/// Get timestamp from CBOR value (Tag 1 = epoch float OR text string).
fn cbor_timestamp_to_iso8601(v: &CborValue) -> String {
    match v {
        CborValue::Tag(1, inner) => {
            match inner.as_ref() {
                CborValue::Float(f) => {
                    let secs = *f as i64;
                    let dt = chrono::DateTime::from_timestamp(secs, 0)
                        .unwrap_or_else(chrono::Utc::now);
                    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                }
                CborValue::Integer(i) => {
                    let secs = i128::from(*i) as i64;
                    let dt = chrono::DateTime::from_timestamp(secs, 0)
                        .unwrap_or_else(chrono::Utc::now);
                    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                }
                _ => now_iso8601(),
            }
        }
        CborValue::Text(s) => s.clone(),
        _ => now_iso8601(),
    }
}

/// Convert an ISO-8601 string to a CBOR timestamp tag.
fn iso8601_to_cbor_timestamp(s: &str) -> CborValue {
    let secs = DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp() as f64)
        .unwrap_or(0.0);
    CborValue::Tag(1, Box::new(CborValue::Float(secs)))
}

// ── Operation handlers ────────────────────────────────────────────────────────

async fn put_metric_data(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let Some(map) = decode_cbor_map(body) else {
        return cbor_error(StatusCode::BAD_REQUEST, "InvalidInput", "invalid CBOR body");
    };

    let Some(namespace) = cbor_get_str(&map, "Namespace") else {
        return cbor_error(StatusCode::BAD_REQUEST, "MissingParameter", "Namespace is required");
    };
    let namespace = namespace.to_string();

    let Some(metric_data_arr) = cbor_get_array(&map, "MetricData") else {
        return cbor_error(StatusCode::BAD_REQUEST, "MissingParameter", "MetricData is required");
    };

    for item in metric_data_arr {
        let CborValue::Map(datum_map) = item else { continue };

        let metric_name = match cbor_get_str(datum_map, "MetricName") {
            Some(n) => n.to_string(),
            None => continue,
        };
        let value = cbor_get_f64(datum_map, "Value").unwrap_or(0.0);
        let unit = cbor_get_str(datum_map, "Unit")
            .unwrap_or("None")
            .to_string();

        // Timestamp can be a tag(1, epoch) or missing
        let timestamp = datum_map
            .iter()
            .find(|(k, _)| matches!(k, CborValue::Text(s) if s == "Timestamp"))
            .map(|(_, v)| cbor_timestamp_to_iso8601(v))
            .unwrap_or_else(now_iso8601);

        let key = metric_key(&namespace, &metric_name);
        let mut stored: StoredMetric = match state.cloudwatch.get(&key).await {
            Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or(StoredMetric {
                namespace: namespace.clone(),
                metric_name: metric_name.clone(),
                points: Vec::new(),
            }),
            _ => StoredMetric {
                namespace: namespace.clone(),
                metric_name: metric_name.clone(),
                points: Vec::new(),
            },
        };
        stored.points.push(MetricDataPoint { timestamp, value, unit });

        if let Ok(data) = serde_json::to_vec(&stored) {
            let _ = state.cloudwatch.put(&key, data).await;
        }
    }

    // PutMetricData returns empty body on success
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/cbor"),
            (
                axum::http::header::HeaderName::from_static("smithy-protocol"),
                "rpc-v2-cbor",
            ),
        ],
        Vec::new(),
    )
}

async fn list_metrics(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let ns_filter = if body.is_empty() {
        None
    } else {
        decode_cbor_map(body)
            .as_ref()
            .and_then(|m| cbor_get_str(m, "Namespace").map(|s| s.to_string()))
    };

    let keys = match state.cloudwatch.list("metrics/").await {
        Ok(k) => k,
        Err(e) => return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string()),
    };

    let mut metrics: Vec<CborValue> = Vec::new();
    for key in &keys {
        let data = match state.cloudwatch.get(key).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let stored: StoredMetric = match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(_) => continue,
        };

        if let Some(ref ns) = ns_filter {
            if &stored.namespace != ns {
                continue;
            }
        }

        metrics.push(CborValue::Map(vec![
            (CborValue::Text("Namespace".to_string()), CborValue::Text(stored.namespace)),
            (CborValue::Text("MetricName".to_string()), CborValue::Text(stored.metric_name)),
            (CborValue::Text("Dimensions".to_string()), CborValue::Array(vec![])),
        ]));
    }

    cbor_ok(CborValue::Map(vec![
        (CborValue::Text("Metrics".to_string()), CborValue::Array(metrics)),
    ]))
}

async fn get_metric_statistics(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let Some(map) = decode_cbor_map(body) else {
        return cbor_error(StatusCode::BAD_REQUEST, "InvalidInput", "invalid CBOR body");
    };

    let namespace = cbor_get_str(&map, "Namespace").unwrap_or("").to_string();
    let metric_name = cbor_get_str(&map, "MetricName").unwrap_or("").to_string();

    // StartTime and EndTime are CBOR timestamps (tag 1)
    let start_time = map.iter()
        .find(|(k, _)| matches!(k, CborValue::Text(s) if s == "StartTime"))
        .map(|(_, v)| cbor_timestamp_to_iso8601(v));
    let end_time = map.iter()
        .find(|(k, _)| matches!(k, CborValue::Text(s) if s == "EndTime"))
        .map(|(_, v)| cbor_timestamp_to_iso8601(v));

    let key = metric_key(&namespace, &metric_name);
    let stored: StoredMetric = match state.cloudwatch.get(&key).await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or(StoredMetric {
            namespace: namespace.clone(),
            metric_name: metric_name.clone(),
            points: Vec::new(),
        }),
        _ => StoredMetric {
            namespace: namespace.clone(),
            metric_name: metric_name.clone(),
            points: Vec::new(),
        },
    };

    let start_dt = start_time.as_deref().and_then(|s| DateTime::parse_from_rfc3339(s).ok());
    let end_dt = end_time.as_deref().and_then(|s| DateTime::parse_from_rfc3339(s).ok());

    let matching: Vec<&MetricDataPoint> = stored
        .points
        .iter()
        .filter(|p| {
            let ts = DateTime::parse_from_rfc3339(&p.timestamp).ok();
            let after_start = start_dt
                .as_ref()
                .zip(ts.as_ref())
                .map(|(s, t)| t >= s)
                .unwrap_or(true);
            let before_end = end_dt
                .as_ref()
                .zip(ts.as_ref())
                .map(|(e, t)| t <= e)
                .unwrap_or(true);
            after_start && before_end
        })
        .collect();

    let datapoints = if matching.is_empty() {
        vec![]
    } else {
        let count = matching.len() as f64;
        let sum: f64 = matching.iter().map(|p| p.value).sum();
        let min = matching.iter().map(|p| p.value).fold(f64::INFINITY, f64::min);
        let max = matching.iter().map(|p| p.value).fold(f64::NEG_INFINITY, f64::max);
        let avg = sum / count;
        let unit = matching.first().map(|p| p.unit.as_str()).unwrap_or("None");
        let ts_iso = matching.first().map(|p| p.timestamp.as_str()).unwrap_or("");
        let ts_cbor = iso8601_to_cbor_timestamp(ts_iso);

        vec![CborValue::Map(vec![
            (CborValue::Text("Timestamp".to_string()), ts_cbor),
            (CborValue::Text("SampleCount".to_string()), CborValue::Float(count)),
            (CborValue::Text("Sum".to_string()), CborValue::Float(sum)),
            (CborValue::Text("Average".to_string()), CborValue::Float(avg)),
            (CborValue::Text("Minimum".to_string()), CborValue::Float(min)),
            (CborValue::Text("Maximum".to_string()), CborValue::Float(max)),
            (CborValue::Text("Unit".to_string()), CborValue::Text(unit.to_string())),
        ])]
    };

    cbor_ok(CborValue::Map(vec![
        (CborValue::Text("Label".to_string()), CborValue::Text(metric_name)),
        (CborValue::Text("Datapoints".to_string()), CborValue::Array(datapoints)),
    ]))
}

async fn put_metric_alarm(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let Some(map) = decode_cbor_map(body) else {
        return cbor_error(StatusCode::BAD_REQUEST, "InvalidInput", "invalid CBOR body");
    };

    let Some(alarm_name) = cbor_get_str(&map, "AlarmName").map(|s| s.to_string()) else {
        return cbor_error(StatusCode::BAD_REQUEST, "MissingParameter", "AlarmName is required");
    };

    let alarm = StoredAlarm {
        alarm_name: alarm_name.clone(),
        alarm_description: cbor_get_str(&map, "AlarmDescription").unwrap_or("").to_string(),
        namespace: cbor_get_str(&map, "Namespace").unwrap_or("").to_string(),
        metric_name: cbor_get_str(&map, "MetricName").unwrap_or("").to_string(),
        statistic: cbor_get_str(&map, "Statistic").unwrap_or("Average").to_string(),
        period: cbor_get_i64(&map, "Period").unwrap_or(60),
        evaluation_periods: cbor_get_i64(&map, "EvaluationPeriods").unwrap_or(1),
        threshold: cbor_get_f64(&map, "Threshold").unwrap_or(0.0),
        comparison_operator: cbor_get_str(&map, "ComparisonOperator")
            .unwrap_or("GreaterThanThreshold")
            .to_string(),
        state_value: "INSUFFICIENT_DATA".to_string(),
        state_reason: "Unchecked: Initial alarm creation".to_string(),
        alarm_arn: alarm_arn(&alarm_name),
    };

    if let Ok(data) = serde_json::to_vec(&alarm) {
        if let Err(e) = state.cloudwatch.put(&alarm_key(&alarm_name), data).await {
            return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string());
        }
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/cbor"),
            (
                axum::http::header::HeaderName::from_static("smithy-protocol"),
                "rpc-v2-cbor",
            ),
        ],
        Vec::new(),
    )
}

async fn describe_alarms(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    // Parse optional AlarmNames filter
    let name_filter: Vec<String> = if body.is_empty() {
        vec![]
    } else {
        decode_cbor_map(body)
            .as_ref()
            .and_then(|m| cbor_get_array(m, "AlarmNames"))
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        if let CborValue::Text(s) = v { Some(s.clone()) } else { None }
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let keys = match state.cloudwatch.list("alarms/").await {
        Ok(k) => k,
        Err(e) => return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string()),
    };

    let mut alarm_items: Vec<CborValue> = Vec::new();
    for key in &keys {
        let data = match state.cloudwatch.get(key).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let alarm: StoredAlarm = match serde_json::from_slice(&data) {
            Ok(a) => a,
            Err(_) => continue,
        };

        if !name_filter.is_empty() && !name_filter.contains(&alarm.alarm_name) {
            continue;
        }

        alarm_items.push(CborValue::Map(vec![
            (CborValue::Text("AlarmName".to_string()), CborValue::Text(alarm.alarm_name)),
            (CborValue::Text("AlarmArn".to_string()), CborValue::Text(alarm.alarm_arn)),
            (CborValue::Text("AlarmDescription".to_string()), CborValue::Text(alarm.alarm_description)),
            (CborValue::Text("Namespace".to_string()), CborValue::Text(alarm.namespace)),
            (CborValue::Text("MetricName".to_string()), CborValue::Text(alarm.metric_name)),
            (CborValue::Text("Statistic".to_string()), CborValue::Text(alarm.statistic)),
            (CborValue::Text("Period".to_string()), CborValue::Integer(alarm.period.into())),
            (CborValue::Text("EvaluationPeriods".to_string()), CborValue::Integer(alarm.evaluation_periods.into())),
            (CborValue::Text("Threshold".to_string()), CborValue::Float(alarm.threshold)),
            (CborValue::Text("ComparisonOperator".to_string()), CborValue::Text(alarm.comparison_operator)),
            (CborValue::Text("StateValue".to_string()), CborValue::Text(alarm.state_value)),
            (CborValue::Text("StateReason".to_string()), CborValue::Text(alarm.state_reason)),
            (CborValue::Text("Dimensions".to_string()), CborValue::Array(vec![])),
        ]));
    }

    cbor_ok(CborValue::Map(vec![
        (CborValue::Text("MetricAlarms".to_string()), CborValue::Array(alarm_items)),
        (CborValue::Text("CompositeAlarms".to_string()), CborValue::Array(vec![])),
    ]))
}

async fn delete_alarms(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let names: Vec<String> = if body.is_empty() {
        vec![]
    } else {
        decode_cbor_map(body)
            .as_ref()
            .and_then(|m| cbor_get_array(m, "AlarmNames"))
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        if let CborValue::Text(s) = v { Some(s.clone()) } else { None }
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    for name in &names {
        let _ = state.cloudwatch.delete(&alarm_key(name)).await;
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/cbor"),
            (
                axum::http::header::HeaderName::from_static("smithy-protocol"),
                "rpc-v2-cbor",
            ),
        ],
        Vec::new(),
    )
}

async fn set_alarm_state(state: &Arc<AppState>, body: &[u8]) -> CborResponse {
    let Some(map) = decode_cbor_map(body) else {
        return cbor_error(StatusCode::BAD_REQUEST, "InvalidInput", "invalid CBOR body");
    };

    let Some(alarm_name) = cbor_get_str(&map, "AlarmName").map(|s| s.to_string()) else {
        return cbor_error(StatusCode::BAD_REQUEST, "MissingParameter", "AlarmName is required");
    };
    let Some(state_value) = cbor_get_str(&map, "StateValue").map(|s| s.to_string()) else {
        return cbor_error(StatusCode::BAD_REQUEST, "MissingParameter", "StateValue is required");
    };
    let state_reason = cbor_get_str(&map, "StateReason").unwrap_or("").to_string();

    let key = alarm_key(&alarm_name);
    let mut alarm: StoredAlarm = match state.cloudwatch.get(&key).await {
        Ok(Some(data)) => match serde_json::from_slice(&data) {
            Ok(a) => a,
            Err(e) => return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string()),
        },
        Ok(None) => return cbor_error(StatusCode::NOT_FOUND, "ResourceNotFound", &format!("Alarm {alarm_name} not found")),
        Err(e) => return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string()),
    };

    alarm.state_value = state_value;
    alarm.state_reason = state_reason;

    if let Ok(data) = serde_json::to_vec(&alarm) {
        if let Err(e) = state.cloudwatch.put(&key, data).await {
            return cbor_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServiceError", &e.to_string());
        }
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/cbor"),
            (
                axum::http::header::HeaderName::from_static("smithy-protocol"),
                "rpc-v2-cbor",
            ),
        ],
        Vec::new(),
    )
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/service/GraniteServiceVersion20100801/operation/{operation}",
            post(handle_operation),
        )
        .route("/cloudwatch/", post(query_dispatch))
}

async fn handle_operation(
    State(state): State<Arc<AppState>>,
    Path(operation): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return cbor_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                &format!("failed to read body: {e}"),
            )
            .into_response()
        }
    };

    tracing::debug!("CloudWatch operation={operation}");

    let resp = match operation.as_str() {
        "PutMetricData" => put_metric_data(&state, &body_bytes).await,
        "ListMetrics" => list_metrics(&state, &body_bytes).await,
        "GetMetricStatistics" => get_metric_statistics(&state, &body_bytes).await,
        "PutMetricAlarm" => put_metric_alarm(&state, &body_bytes).await,
        "DescribeAlarms" => describe_alarms(&state, &body_bytes).await,
        "DeleteAlarms" => delete_alarms(&state, &body_bytes).await,
        "SetAlarmState" => set_alarm_state(&state, &body_bytes).await,
        other => {
            tracing::warn!("unknown CloudWatch operation: {other}");
            cbor_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                &format!("unknown operation: {other}"),
            )
        }
    };

    // Add request ID header
    let (status, headers, body) = resp;
    let mut response = axum::response::Response::builder()
        .status(status);
    for (name, value) in &headers {
        response = response.header(name, *value);
    }
    response
        .header("x-amzn-requestid", REQUEST_ID)
        .body(axum::body::Body::from(body))
        .unwrap()
        .into_response()
}

/// Fallback Query-protocol handler for /cloudwatch/ direct path.
/// The SDK no longer uses this, but it's kept for curl-based testing.
async fn query_dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(State(state), request).await
}

/// Legacy Query-protocol dispatcher — called from top_level_dispatch when service=monitoring.
/// This handles old-style requests; the SDK v1 uses the CBOR routes instead.
pub async fn dispatch(State(state): State<Arc<AppState>>, request: Request) -> impl IntoResponse {
    use axum::http::header as hdr;

    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                [(hdr::CONTENT_TYPE, "text/xml")],
                format!("<Error><Message>{e}</Message></Error>"),
            )
                .into_response()
        }
    };

    let params: HashMap<String, String> = if body_bytes.is_empty() {
        HashMap::new()
    } else {
        serde_urlencoded::from_bytes(&body_bytes).unwrap_or_default()
    };

    let action = params.get("Action").cloned().unwrap_or_default();

    // For the Query protocol, we respond with XML (for manual/curl testing)
    let xml_resp = match action.as_str() {
        "ListMetrics" => {
            let keys = state.cloudwatch.list("metrics/").await.unwrap_or_default();
            let mut members = String::new();
            for key in &keys {
                if let Ok(Some(data)) = state.cloudwatch.get(key).await {
                    if let Ok(stored) = serde_json::from_slice::<StoredMetric>(&data) {
                        members.push_str(&format!(
                            "<member><Namespace>{}</Namespace><MetricName>{}</MetricName><Dimensions/></member>",
                            stored.namespace, stored.metric_name
                        ));
                    }
                }
            }
            format!(
                r#"<ListMetricsResponse xmlns="https://monitoring.amazonaws.com/doc/2010-08-01/"><ListMetricsResult><Metrics>{members}</Metrics></ListMetricsResult><ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata></ListMetricsResponse>"#
            )
        }
        _ => format!(
            r#"<ErrorResponse xmlns="https://monitoring.amazonaws.com/doc/2010-08-01/"><Error><Code>InvalidAction</Code><Message>unknown action: {action}</Message></Error></ErrorResponse>"#
        ),
    };

    (StatusCode::OK, [(hdr::CONTENT_TYPE, "text/xml")], xml_resp).into_response()
}
