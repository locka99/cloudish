//! CloudWatch Logs service emulator.
//!
//! Wire format: JSON API with `X-Amz-Target: Logs_20140328.{Operation}` header.
//! Content-Type: application/x-amz-json-1.1. Responses are JSON.
//! SigV4 credential scope: service = "logs"
//!
//! Routing: dispatched from top_level_dispatch when target starts with "Logs_"
//!
//! Storage layout (under data/cloudwatch_logs/):
//!   groups/{encoded_group_name}.json
//!   streams/{encoded_group}/{encoded_stream}.json
//!   events/{encoded_group}/{encoded_stream}.json

use std::sync::Arc;

use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{services::AppState, storage::Storage};

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLogGroup {
    log_group_name: String,
    arn: String,
    creation_time: i64,
    retention_in_days: Option<i64>,
    stored_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLogStream {
    log_stream_name: String,
    log_group_name: String,
    arn: String,
    creation_time: i64,
    first_event_timestamp: Option<i64>,
    last_event_timestamp: Option<i64>,
    last_ingestion_time: Option<i64>,
    upload_sequence_token: String,
    stored_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLogEvent {
    timestamp: i64,
    message: String,
    ingestion_time: i64,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn group_key(name: &str) -> String {
    format!("groups/{}.json", urlencoding::encode(name))
}

fn stream_key(group: &str, stream: &str) -> String {
    format!(
        "streams/{}/{}.json",
        urlencoding::encode(group),
        urlencoding::encode(stream)
    )
}

fn events_key(group: &str, stream: &str) -> String {
    format!(
        "events/{}/{}.json",
        urlencoding::encode(group),
        urlencoding::encode(stream)
    )
}

fn log_group_arn(name: &str) -> String {
    format!("arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{name}")
}

fn log_stream_arn(group: &str, stream: &str) -> String {
    format!("arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{group}:log-stream:{stream}")
}

type JsonResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn json_ok(body: Value) -> JsonResponse {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/x-amz-json-1.1")],
        body.to_string(),
    )
}

fn json_error(status: StatusCode, error_type: &str, message: &str) -> JsonResponse {
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/x-amz-json-1.1")],
        json!({"__type": error_type, "message": message}).to_string(),
    )
}

// ── Operation handlers ────────────────────────────────────────────────────────

async fn create_log_group(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };

    let key = group_key(name);
    if let Ok(Some(_)) = state.cloudwatch_logs.get(&key).await {
        return json_error(
            StatusCode::BAD_REQUEST,
            "ResourceAlreadyExistsException",
            "The specified log group already exists",
        );
    }

    let group = StoredLogGroup {
        log_group_name: name.to_string(),
        arn: log_group_arn(name),
        creation_time: now_ms(),
        retention_in_days: None,
        stored_bytes: 0,
    };

    let data = match serde_json::to_vec(&group) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };
    if let Err(e) = state.cloudwatch_logs.put(&key, data).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string());
    }

    json_ok(json!({}))
}

async fn delete_log_group(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };

    // Delete all streams and events for this group
    let stream_prefix = format!("streams/{}/", urlencoding::encode(name));
    let events_prefix = format!("events/{}/", urlencoding::encode(name));

    if let Ok(keys) = state.cloudwatch_logs.list(&stream_prefix).await {
        for key in keys {
            let _ = state.cloudwatch_logs.delete(&key).await;
        }
    }
    if let Ok(keys) = state.cloudwatch_logs.list(&events_prefix).await {
        for key in keys {
            let _ = state.cloudwatch_logs.delete(&key).await;
        }
    }

    let _ = state.cloudwatch_logs.delete(&group_key(name)).await;

    json_ok(json!({}))
}

async fn describe_log_groups(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let prefix_filter = body["logGroupNamePrefix"].as_str().unwrap_or("");

    let keys = match state.cloudwatch_logs.list("groups/").await {
        Ok(k) => k,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };

    let mut groups: Vec<StoredLogGroup> = Vec::new();
    for key in &keys {
        let data = match state.cloudwatch_logs.get(key).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let group: StoredLogGroup = match serde_json::from_slice(&data) {
            Ok(g) => g,
            Err(_) => continue,
        };
        if !prefix_filter.is_empty() && !group.log_group_name.starts_with(prefix_filter) {
            continue;
        }
        groups.push(group);
    }

    groups.sort_by(|a, b| a.log_group_name.cmp(&b.log_group_name));

    let log_groups: Vec<Value> = groups
        .iter()
        .map(|g| {
            json!({
                "logGroupName": g.log_group_name,
                "arn": g.arn,
                "creationTime": g.creation_time,
                "retentionInDays": g.retention_in_days,
                "storedBytes": g.stored_bytes,
            })
        })
        .collect();

    json_ok(json!({"logGroups": log_groups}))
}

async fn create_log_stream(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };
    let Some(stream_name) = body["logStreamName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logStreamName is required");
    };

    // Check group exists
    if let Ok(None) = state.cloudwatch_logs.get(&group_key(group_name)).await {
        return json_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            "The specified log group does not exist",
        );
    }

    let key = stream_key(group_name, stream_name);
    if let Ok(Some(_)) = state.cloudwatch_logs.get(&key).await {
        return json_error(
            StatusCode::BAD_REQUEST,
            "ResourceAlreadyExistsException",
            "The specified log stream already exists",
        );
    }

    let stream = StoredLogStream {
        log_stream_name: stream_name.to_string(),
        log_group_name: group_name.to_string(),
        arn: log_stream_arn(group_name, stream_name),
        creation_time: now_ms(),
        first_event_timestamp: None,
        last_event_timestamp: None,
        last_ingestion_time: None,
        upload_sequence_token: Uuid::new_v4().to_string(),
        stored_bytes: 0,
    };

    let data = match serde_json::to_vec(&stream) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };
    if let Err(e) = state.cloudwatch_logs.put(&key, data).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string());
    }

    json_ok(json!({}))
}

async fn delete_log_stream(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };
    let Some(stream_name) = body["logStreamName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logStreamName is required");
    };

    let _ = state.cloudwatch_logs.delete(&stream_key(group_name, stream_name)).await;
    let _ = state.cloudwatch_logs.delete(&events_key(group_name, stream_name)).await;

    json_ok(json!({}))
}

async fn describe_log_streams(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };
    let prefix_filter = body["logStreamNamePrefix"].as_str().unwrap_or("");

    let stream_prefix = format!("streams/{}/", urlencoding::encode(group_name));
    let keys = match state.cloudwatch_logs.list(&stream_prefix).await {
        Ok(k) => k,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };

    let mut streams: Vec<StoredLogStream> = Vec::new();
    for key in &keys {
        let data = match state.cloudwatch_logs.get(key).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let stream: StoredLogStream = match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if !prefix_filter.is_empty() && !stream.log_stream_name.starts_with(prefix_filter) {
            continue;
        }
        streams.push(stream);
    }

    streams.sort_by(|a, b| a.log_stream_name.cmp(&b.log_stream_name));

    let log_streams: Vec<Value> = streams
        .iter()
        .map(|s| {
            json!({
                "logStreamName": s.log_stream_name,
                "logGroupName": s.log_group_name,
                "arn": s.arn,
                "creationTime": s.creation_time,
                "firstEventTimestamp": s.first_event_timestamp,
                "lastEventTimestamp": s.last_event_timestamp,
                "lastIngestionTime": s.last_ingestion_time,
                "uploadSequenceToken": s.upload_sequence_token,
                "storedBytes": s.stored_bytes,
            })
        })
        .collect();

    json_ok(json!({"logStreams": log_streams}))
}

async fn put_log_events(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };
    let Some(stream_name) = body["logStreamName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logStreamName is required");
    };

    // Auto-create group if missing
    let group_k = group_key(group_name);
    if let Ok(None) = state.cloudwatch_logs.get(&group_k).await {
        let group = StoredLogGroup {
            log_group_name: group_name.to_string(),
            arn: log_group_arn(group_name),
            creation_time: now_ms(),
            retention_in_days: None,
            stored_bytes: 0,
        };
        if let Ok(d) = serde_json::to_vec(&group) {
            let _ = state.cloudwatch_logs.put(&group_k, d).await;
        }
    }

    // Auto-create stream if missing
    let stream_k = stream_key(group_name, stream_name);
    if let Ok(None) = state.cloudwatch_logs.get(&stream_k).await {
        let stream = StoredLogStream {
            log_stream_name: stream_name.to_string(),
            log_group_name: group_name.to_string(),
            arn: log_stream_arn(group_name, stream_name),
            creation_time: now_ms(),
            first_event_timestamp: None,
            last_event_timestamp: None,
            last_ingestion_time: None,
            upload_sequence_token: Uuid::new_v4().to_string(),
            stored_bytes: 0,
        };
        if let Ok(d) = serde_json::to_vec(&stream) {
            let _ = state.cloudwatch_logs.put(&stream_k, d).await;
        }
    }

    let ingestion_time = now_ms();
    let events_k = events_key(group_name, stream_name);

    // Load existing events
    let mut stored_events: Vec<StoredLogEvent> = match state.cloudwatch_logs.get(&events_k).await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
        _ => Vec::new(),
    };

    // Parse and append new events
    let new_events = body["logEvents"].as_array().cloned().unwrap_or_default();
    let mut first_ts: Option<i64> = None;
    let mut last_ts: Option<i64> = None;

    for ev in &new_events {
        let ts = ev["timestamp"].as_i64().unwrap_or(ingestion_time);
        let msg = ev["message"].as_str().unwrap_or("").to_string();

        if first_ts.is_none() || ts < first_ts.unwrap() {
            first_ts = Some(ts);
        }
        if last_ts.is_none() || ts > last_ts.unwrap() {
            last_ts = Some(ts);
        }

        stored_events.push(StoredLogEvent {
            timestamp: ts,
            message: msg,
            ingestion_time,
        });
    }

    // Sort events by timestamp
    stored_events.sort_by_key(|e| e.timestamp);

    let data = match serde_json::to_vec(&stored_events) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };
    if let Err(e) = state.cloudwatch_logs.put(&events_k, data).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string());
    }

    // Update stream metadata
    if let Ok(Some(stream_data)) = state.cloudwatch_logs.get(&stream_k).await {
        if let Ok(mut stream) = serde_json::from_slice::<StoredLogStream>(&stream_data) {
            if let Some(ft) = first_ts {
                stream.first_event_timestamp = Some(
                    stream.first_event_timestamp.map(|e| e.min(ft)).unwrap_or(ft),
                );
            }
            if let Some(lt) = last_ts {
                stream.last_event_timestamp = Some(
                    stream.last_event_timestamp.map(|e| e.max(lt)).unwrap_or(lt),
                );
            }
            stream.last_ingestion_time = Some(ingestion_time);
            stream.upload_sequence_token = Uuid::new_v4().to_string();
            if let Ok(d) = serde_json::to_vec(&stream) {
                let _ = state.cloudwatch_logs.put(&stream_k, d).await;
            }
        }
    }

    let next_token = Uuid::new_v4().to_string();
    json_ok(json!({"nextSequenceToken": next_token}))
}

async fn get_log_events(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };
    let Some(stream_name) = body["logStreamName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logStreamName is required");
    };

    let start_time = body["startTime"].as_i64();
    let end_time = body["endTime"].as_i64();
    let limit = body["limit"].as_u64().unwrap_or(10000) as usize;

    let events_k = events_key(group_name, stream_name);
    let stored_events: Vec<StoredLogEvent> = match state.cloudwatch_logs.get(&events_k).await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
        _ => Vec::new(),
    };

    let filtered: Vec<&StoredLogEvent> = stored_events
        .iter()
        .filter(|e| {
            let after_start = start_time.map(|s| e.timestamp >= s).unwrap_or(true);
            let before_end = end_time.map(|en| e.timestamp <= en).unwrap_or(true);
            after_start && before_end
        })
        .take(limit)
        .collect();

    let events: Vec<Value> = filtered
        .iter()
        .map(|e| {
            json!({
                "timestamp": e.timestamp,
                "message": e.message,
                "ingestionTime": e.ingestion_time,
            })
        })
        .collect();

    let forward_token = Uuid::new_v4().to_string();
    let backward_token = Uuid::new_v4().to_string();

    json_ok(json!({
        "events": events,
        "nextForwardToken": forward_token,
        "nextBackwardToken": backward_token,
    }))
}

async fn filter_log_events(state: &Arc<AppState>, body: &Value) -> JsonResponse {
    let Some(group_name) = body["logGroupName"].as_str() else {
        return json_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "logGroupName is required");
    };

    let filter_pattern = body["filterPattern"].as_str().unwrap_or("");
    let start_time = body["startTime"].as_i64();
    let end_time = body["endTime"].as_i64();

    // Determine which streams to search
    let stream_names: Vec<String> = body["logStreamNames"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();

    // List all streams in this group
    let stream_prefix = format!("streams/{}/", urlencoding::encode(group_name));
    let stream_keys = match state.cloudwatch_logs.list(&stream_prefix).await {
        Ok(k) => k,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "ServiceException", &e.to_string()),
    };

    let mut all_events: Vec<Value> = Vec::new();

    for sk in &stream_keys {
        let stream_data = match state.cloudwatch_logs.get(sk).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let stream: StoredLogStream = match serde_json::from_slice(&stream_data) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Filter by stream names if specified
        if !stream_names.is_empty() && !stream_names.contains(&stream.log_stream_name) {
            continue;
        }

        let events_k = events_key(group_name, &stream.log_stream_name);
        let stored_events: Vec<StoredLogEvent> = match state.cloudwatch_logs.get(&events_k).await {
            Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
            _ => continue,
        };

        for (idx, event) in stored_events.iter().enumerate() {
            let after_start = start_time.map(|s| event.timestamp >= s).unwrap_or(true);
            let before_end = end_time.map(|en| event.timestamp <= en).unwrap_or(true);
            if !after_start || !before_end {
                continue;
            }

            // Substring match (case-sensitive); empty pattern matches all
            if !filter_pattern.is_empty() && !event.message.contains(filter_pattern) {
                continue;
            }

            let event_id = format!("{}-{}-{}", urlencoding::encode(group_name), urlencoding::encode(&stream.log_stream_name), idx);
            all_events.push(json!({
                "logStreamName": stream.log_stream_name,
                "timestamp": event.timestamp,
                "message": event.message,
                "ingestionTime": event.ingestion_time,
                "eventId": event_id,
            }));
        }
    }

    // Sort by timestamp
    all_events.sort_by_key(|e| e["timestamp"].as_i64().unwrap_or(0));

    json_ok(json!({"events": all_events}))
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub async fn dispatch(State(state): State<Arc<AppState>>, request: Request) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let operation = target.strip_prefix("Logs_20140328.").unwrap_or("").to_string();

    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                &format!("failed to read body: {e}"),
            )
            .into_response()
        }
    };

    let body: Value = if body_bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    &format!("failed to parse body: {e}"),
                )
                .into_response()
            }
        }
    };

    tracing::debug!("CloudWatch Logs operation={operation}");

    match operation.as_str() {
        "CreateLogGroup" => create_log_group(&state, &body).await.into_response(),
        "DeleteLogGroup" => delete_log_group(&state, &body).await.into_response(),
        "DescribeLogGroups" => describe_log_groups(&state, &body).await.into_response(),
        "CreateLogStream" => create_log_stream(&state, &body).await.into_response(),
        "DeleteLogStream" => delete_log_stream(&state, &body).await.into_response(),
        "DescribeLogStreams" => describe_log_streams(&state, &body).await.into_response(),
        "PutLogEvents" => put_log_events(&state, &body).await.into_response(),
        "GetLogEvents" => get_log_events(&state, &body).await.into_response(),
        "FilterLogEvents" => filter_log_events(&state, &body).await.into_response(),
        other => {
            tracing::warn!("unknown CloudWatch Logs operation: {other}");
            json_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                &format!("unknown operation: {other}"),
            )
            .into_response()
        }
    }
}
