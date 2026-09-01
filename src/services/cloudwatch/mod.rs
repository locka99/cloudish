//! CloudWatch service emulator — metrics and alarms.
//!
//! Wire format: AWS Query protocol — POST with `application/x-www-form-urlencoded` body,
//! `Action=` field selects the operation. Responses are XML.
//! SigV4 credential scope: service = "monitoring"
//!
//! Routing:
//!   POST /cloudwatch/   — convenience path for direct calls
//!   POST /              — routed here via top_level_dispatch when SigV4 service = "monitoring"
//!
//! Storage layout (under data/cloudwatch/):
//!   metrics/{encoded_namespace}/{encoded_metric_name}.json   — array of data points
//!   alarms/{encoded_alarm_name}.json

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::{services::AppState, storage::Storage};

// ── Constants ────────────────────────────────────────────────────────────────

const NS: &str = "https://monitoring.amazonaws.com/doc/2010-08-01/";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";
const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";

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

type XmlResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn response_metadata() -> String {
    format!("<ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>")
}

fn xml_response(op: &str, inner: &str) -> XmlResponse {
    let body = format!(
        r#"<{op}Response xmlns="{NS}">{inner}{meta}</{op}Response>"#,
        meta = response_metadata()
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_error(code: StatusCode, error_code: &str, message: &str) -> XmlResponse {
    let body = format!(
        r#"<ErrorResponse xmlns="{NS}"><Error><Code>{error_code}</Code><Message>{message}</Message></Error>{meta}</ErrorResponse>"#,
        meta = response_metadata()
    );
    (code, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn member_list(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        match params.get(&format!("{prefix}.{i}")) {
            Some(v) => {
                out.push(v.clone());
                i += 1;
            }
            None => break,
        }
    }
    out
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

// ── Operation handlers ────────────────────────────────────────────────────────

async fn put_metric_data(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(namespace) = params.get("Namespace") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "Namespace is required");
    };

    // Parse numbered MetricData.member.N.{MetricName,Value,Unit,Timestamp}
    let mut i = 1usize;
    loop {
        let prefix = format!("MetricData.member.{i}");
        let Some(metric_name) = params.get(&format!("{prefix}.MetricName")) else {
            break;
        };

        let value: f64 = params
            .get(&format!("{prefix}.Value"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let unit = params
            .get(&format!("{prefix}.Unit"))
            .cloned()
            .unwrap_or_else(|| "None".to_string());
        let timestamp = params
            .get(&format!("{prefix}.Timestamp"))
            .cloned()
            .unwrap_or_else(now_iso8601);

        let key = metric_key(namespace, metric_name);

        // Load existing metric or create new
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

        let data = match serde_json::to_vec(&stored) {
            Ok(d) => d,
            Err(e) => {
                return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
            }
        };
        if let Err(e) = state.cloudwatch.put(&key, data).await {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
        }

        i += 1;
    }

    xml_response("PutMetricData", "")
}

async fn list_metrics(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let ns_filter = params.get("Namespace").cloned();

    let keys = match state.cloudwatch.list("metrics/").await {
        Ok(k) => k,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };

    let mut members = String::new();
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

        members.push_str(&format!(
            "<member>\
                <Namespace>{}</Namespace>\
                <MetricName>{}</MetricName>\
                <Dimensions/>\
            </member>",
            xml_escape(&stored.namespace),
            xml_escape(&stored.metric_name)
        ));
    }

    xml_response(
        "ListMetrics",
        &format!("<ListMetricsResult><Metrics>{members}</Metrics></ListMetricsResult>"),
    )
}

async fn get_metric_statistics(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(namespace) = params.get("Namespace") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "Namespace is required");
    };
    let Some(metric_name) = params.get("MetricName") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "MetricName is required");
    };
    let start_time = params.get("StartTime").cloned().unwrap_or_default();
    let end_time = params.get("EndTime").cloned().unwrap_or_default();

    let key = metric_key(namespace, metric_name);
    let stored: StoredMetric = match state.cloudwatch.get(&key).await {
        Ok(Some(data)) => match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(e) => {
                return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
            }
        },
        Ok(None) => StoredMetric {
            namespace: namespace.clone(),
            metric_name: metric_name.clone(),
            points: Vec::new(),
        },
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };

    // Parse start/end time for filtering
    let start_dt = DateTime::parse_from_rfc3339(&start_time).ok();
    let end_dt = DateTime::parse_from_rfc3339(&end_time).ok();

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
        String::new()
    } else {
        let count = matching.len() as f64;
        let sum: f64 = matching.iter().map(|p| p.value).sum();
        let min = matching.iter().map(|p| p.value).fold(f64::INFINITY, f64::min);
        let max = matching.iter().map(|p| p.value).fold(f64::NEG_INFINITY, f64::max);
        let avg = sum / count;
        let unit = matching.first().map(|p| p.unit.as_str()).unwrap_or("None");
        let ts = matching.first().map(|p| p.timestamp.as_str()).unwrap_or("");

        format!(
            "<member>\
                <Timestamp>{ts}</Timestamp>\
                <SampleCount>{count}</SampleCount>\
                <Sum>{sum}</Sum>\
                <Average>{avg}</Average>\
                <Minimum>{min}</Minimum>\
                <Maximum>{max}</Maximum>\
                <Unit>{unit}</Unit>\
            </member>"
        )
    };

    xml_response(
        "GetMetricStatistics",
        &format!(
            "<GetMetricStatisticsResult>\
                <Datapoints>{datapoints}</Datapoints>\
                <Label>{}</Label>\
            </GetMetricStatisticsResult>",
            xml_escape(metric_name)
        ),
    )
}

async fn put_metric_alarm(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(alarm_name) = params.get("AlarmName") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "AlarmName is required");
    };

    let alarm = StoredAlarm {
        alarm_name: alarm_name.clone(),
        alarm_description: params.get("AlarmDescription").cloned().unwrap_or_default(),
        namespace: params.get("Namespace").cloned().unwrap_or_default(),
        metric_name: params.get("MetricName").cloned().unwrap_or_default(),
        statistic: params.get("Statistic").cloned().unwrap_or_else(|| "Average".to_string()),
        period: params.get("Period").and_then(|v| v.parse().ok()).unwrap_or(60),
        evaluation_periods: params
            .get("EvaluationPeriods")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
        threshold: params.get("Threshold").and_then(|v| v.parse().ok()).unwrap_or(0.0),
        comparison_operator: params
            .get("ComparisonOperator")
            .cloned()
            .unwrap_or_else(|| "GreaterThanThreshold".to_string()),
        state_value: "INSUFFICIENT_DATA".to_string(),
        state_reason: "Unchecked: Initial alarm creation".to_string(),
        alarm_arn: alarm_arn(alarm_name),
    };

    let data = match serde_json::to_vec(&alarm) {
        Ok(d) => d,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };
    if let Err(e) = state.cloudwatch.put(&alarm_key(alarm_name), data).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    xml_response("PutMetricAlarm", "")
}

async fn describe_alarms(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let name_filter: Vec<String> = member_list(params, "AlarmNames.member");

    let keys = match state.cloudwatch.list("alarms/").await {
        Ok(k) => k,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };

    let mut members = String::new();
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

        members.push_str(&format!(
            "<member>\
                <AlarmName>{}</AlarmName>\
                <AlarmArn>{}</AlarmArn>\
                <AlarmDescription>{}</AlarmDescription>\
                <Namespace>{}</Namespace>\
                <MetricName>{}</MetricName>\
                <Statistic>{}</Statistic>\
                <Period>{}</Period>\
                <EvaluationPeriods>{}</EvaluationPeriods>\
                <Threshold>{}</Threshold>\
                <ComparisonOperator>{}</ComparisonOperator>\
                <StateValue>{}</StateValue>\
                <StateReason>{}</StateReason>\
            </member>",
            xml_escape(&alarm.alarm_name),
            xml_escape(&alarm.alarm_arn),
            xml_escape(&alarm.alarm_description),
            xml_escape(&alarm.namespace),
            xml_escape(&alarm.metric_name),
            xml_escape(&alarm.statistic),
            alarm.period,
            alarm.evaluation_periods,
            alarm.threshold,
            xml_escape(&alarm.comparison_operator),
            xml_escape(&alarm.state_value),
            xml_escape(&alarm.state_reason),
        ));
    }

    xml_response(
        "DescribeAlarms",
        &format!(
            "<DescribeAlarmsResult><MetricAlarms>{members}</MetricAlarms></DescribeAlarmsResult>"
        ),
    )
}

async fn delete_alarms(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let names = member_list(params, "AlarmNames.member");
    for name in &names {
        if let Err(e) = state.cloudwatch.delete(&alarm_key(name)).await {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
        }
    }
    xml_response("DeleteAlarms", "")
}

async fn set_alarm_state(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(alarm_name) = params.get("AlarmName") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "AlarmName is required");
    };
    let Some(state_value) = params.get("StateValue") else {
        return xml_error(StatusCode::BAD_REQUEST, "MissingParameter", "StateValue is required");
    };
    let state_reason = params.get("StateReason").cloned().unwrap_or_default();

    let key = alarm_key(alarm_name);
    let mut alarm: StoredAlarm = match state.cloudwatch.get(&key).await {
        Ok(Some(data)) => match serde_json::from_slice(&data) {
            Ok(a) => a,
            Err(e) => {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    &e.to_string(),
                )
            }
        },
        Ok(None) => {
            return xml_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                &format!("Alarm {alarm_name} not found"),
            )
        }
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };

    alarm.state_value = state_value.clone();
    alarm.state_reason = state_reason;

    let data = match serde_json::to_vec(&alarm) {
        Ok(d) => d,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };
    if let Err(e) = state.cloudwatch.put(&key, data).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    xml_response("SetAlarmState", "")
}

// ── Router and dispatcher ─────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/cloudwatch/", post(dispatch_handler))
}

async fn dispatch_handler(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(State(state), request).await
}

pub async fn dispatch(State(state): State<Arc<AppState>>, request: Request) -> impl IntoResponse {
    let query_action = request
        .uri()
        .query()
        .and_then(|q| serde_urlencoded::from_str::<HashMap<String, String>>(q).ok())
        .and_then(|m| m.get("Action").cloned());

    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                &format!("failed to read body: {e}"),
            )
            .into_response()
        }
    };

    let params: HashMap<String, String> = if body_bytes.is_empty() {
        HashMap::new()
    } else {
        match serde_urlencoded::from_bytes(&body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return xml_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    &format!("failed to parse body: {e}"),
                )
                .into_response()
            }
        }
    };

    let action = params.get("Action").cloned().or(query_action).unwrap_or_default();
    tracing::debug!("CloudWatch action={action}");

    match action.as_str() {
        "PutMetricData" => put_metric_data(&state, &params).await.into_response(),
        "ListMetrics" => list_metrics(&state, &params).await.into_response(),
        "GetMetricStatistics" => get_metric_statistics(&state, &params).await.into_response(),
        "PutMetricAlarm" => put_metric_alarm(&state, &params).await.into_response(),
        "DescribeAlarms" => describe_alarms(&state, &params).await.into_response(),
        "DeleteAlarms" => delete_alarms(&state, &params).await.into_response(),
        "SetAlarmState" => set_alarm_state(&state, &params).await.into_response(),
        other => {
            tracing::warn!("unknown CloudWatch action: {other}");
            xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                &format!("unknown action: {other}"),
            )
            .into_response()
        }
    }
}
