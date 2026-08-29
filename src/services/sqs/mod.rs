//! SQS service emulator.
//!
//! Wire format: POST with JSON body, X-Amz-Target header selects the operation.
//! Responses are JSON. Uses `application/x-amz-json-1.0` protocol.
//!
//! Routing:
//!   POST /sqs/                        — service-level operations (convenience)
//!   POST /000000000000/{queue_name}   — queue-specific operations
//!   POST /                            — routed here via top_level_dispatch when service=sqs

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::services::AppState;
use crate::storage::Storage;

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueMeta {
    name: String,
    url: String,
    arn: String,
    created_timestamp: u64,
    last_modified_timestamp: u64,
    visibility_timeout: u64,
    max_message_size: u64,
    message_retention_seconds: u64,
    delay_seconds: u64,
    receive_wait_time_seconds: u64,
    fifo: bool,
    content_based_deduplication: bool,
    dlq_arn: Option<String>,
    max_receive_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MsgAttribute {
    data_type: String,
    string_value: Option<String>,
    binary_value: Option<String>, // base64 encoded
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMessage {
    message_id: String,
    receipt_handle: String,
    body: String,
    md5_of_body: String,
    attributes: HashMap<String, String>,
    message_attributes: HashMap<String, MsgAttribute>,
    sent_at: u64,
    delay_until: u64,
    visible_after: u64,
    receive_count: u32,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    sequence_number: Option<String>,
}

// ── Long-poll notification ───────────────────────────────────────────────────

static QUEUE_NOTIFY: OnceLock<Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>> =
    OnceLock::new();

fn queue_notify() -> &'static Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Notify>>>> {
    QUEUE_NOTIFY.get_or_init(|| Arc::new(std::sync::Mutex::new(HashMap::new())))
}

fn get_or_create_notify(queue_name: &str) -> Arc<tokio::sync::Notify> {
    let mut map = queue_notify().lock().unwrap();
    map.entry(queue_name.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
        .clone()
}

// ── JSON response helpers ────────────────────────────────────────────────────

type JsonResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn json_ok(body: serde_json::Value) -> JsonResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-amz-json-1.0")],
        body.to_string(),
    )
}

fn json_empty_ok() -> JsonResponse {
    json_ok(serde_json::json!({}))
}

fn json_error(code: StatusCode, error_code: &str, message: &str) -> JsonResponse {
    let body = serde_json::json!({
        "__type": error_code,
        "message": message
    });
    (
        code,
        [(header::CONTENT_TYPE, "application/x-amz-json-1.0")],
        body.to_string(),
    )
}

fn queue_not_found(queue_name: &str) -> JsonResponse {
    json_error(
        StatusCode::BAD_REQUEST,
        "AWS.SimpleQueueService.NonExistentQueue",
        &format!("The specified queue '{queue_name}' does not exist."),
    )
}

// ── Utility functions ────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn md5_hex(data: &str) -> String {
    format!("{:x}", Md5::digest(data.as_bytes()))
}

fn new_message_id() -> String {
    Uuid::new_v4().to_string()
}

fn new_receipt_handle() -> String {
    Uuid::new_v4().to_string()
}

fn queue_url_from_host(host: &str, queue_name: &str) -> String {
    format!("http://{host}/{ACCOUNT_ID}/{queue_name}")
}

fn queue_arn(queue_name: &str) -> String {
    format!("arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}")
}

fn queue_name_from_url(url: &str) -> Option<String> {
    url.trim_end_matches('/')
        .split('/')
        .last()
        .map(|s| s.to_string())
}

// ── Storage path helpers ─────────────────────────────────────────────────────

fn meta_path(queue_name: &str) -> String {
    format!("queues/{queue_name}/_meta.json")
}

fn msg_path(queue_name: &str, msg_id: &str) -> String {
    format!("queues/{queue_name}/messages/{msg_id}.json")
}

fn dedup_path(queue_name: &str, dedup_id: &str) -> String {
    format!("queues/{queue_name}/dedup/{dedup_id}")
}

fn seq_path(queue_name: &str) -> String {
    format!("queues/{queue_name}/_seq")
}

fn receipt_index_path(queue_name: &str, receipt_handle: &str) -> String {
    format!("queues/{queue_name}/receipts/{receipt_handle}")
}

// ── Storage helpers ──────────────────────────────────────────────────────────

async fn load_meta(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
) -> anyhow::Result<Option<QueueMeta>> {
    match sqs.get(&meta_path(queue_name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_meta(
    sqs: &Arc<crate::storage::file::FileStorage>,
    meta: &QueueMeta,
) -> anyhow::Result<()> {
    sqs.put(&meta_path(&meta.name), serde_json::to_vec(meta)?)
        .await
}

async fn load_message(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
    msg_id: &str,
) -> anyhow::Result<Option<StoredMessage>> {
    match sqs.get(&msg_path(queue_name, msg_id)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_message(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
    msg: &StoredMessage,
) -> anyhow::Result<()> {
    sqs.put(
        &msg_path(queue_name, &msg.message_id),
        serde_json::to_vec(msg)?,
    )
    .await
}

/// Next FIFO sequence number (monotonically increasing, padded to 20 digits).
async fn next_sequence_number(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
) -> anyhow::Result<String> {
    let path = seq_path(queue_name);
    let current: u64 = match sqs.get(&path).await? {
        Some(b) => String::from_utf8_lossy(&b).trim().parse().unwrap_or(0),
        None => 0,
    };
    let next = current + 1;
    sqs.put(&path, next.to_string().into_bytes()).await?;
    Ok(format!("{:020}", next))
}

/// Pick up to `max` visible messages.
async fn pick_visible_messages(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
    max: usize,
    visibility_timeout: u64,
    fifo: bool,
) -> anyhow::Result<Vec<StoredMessage>> {
    let prefix = format!("queues/{queue_name}/messages/");
    let paths = sqs.list(&prefix).await?;
    let now = now_secs();

    let mut candidates: Vec<StoredMessage> = Vec::new();
    for path in &paths {
        if !path.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = sqs.get(path).await {
            if let Ok(msg) = serde_json::from_slice::<StoredMessage>(&b) {
                let is_visible = msg.delay_until <= now
                    && (msg.visible_after == 0 || msg.visible_after <= now);
                if is_visible {
                    candidates.push(msg);
                }
            }
        }
    }

    candidates.sort_by_key(|m| m.sent_at);

    if fifo && !candidates.is_empty() {
        let first_group = candidates[0].group_id.clone();
        candidates.retain(|m| m.group_id == first_group);
    }

    candidates.truncate(max);

    let mut result = Vec::new();
    for mut msg in candidates {
        msg.receipt_handle = new_receipt_handle();
        msg.visible_after = now + visibility_timeout;
        msg.receive_count += 1;
        if msg.receive_count == 1 {
            msg.attributes
                .insert("ApproximateFirstReceiveTimestamp".into(), now_millis().to_string());
        }
        msg.attributes
            .insert("ApproximateReceiveCount".into(), msg.receive_count.to_string());
        // Store receipt index
        sqs.put(
            &receipt_index_path(queue_name, &msg.receipt_handle),
            msg.message_id.as_bytes().to_vec(),
        )
        .await?;
        save_message(sqs, queue_name, &msg).await?;
        result.push(msg);
    }

    Ok(result)
}

/// Move a message to the DLQ.
async fn move_to_dlq(
    sqs: &Arc<crate::storage::file::FileStorage>,
    source_queue: &str,
    original: &StoredMessage,
    dlq_name: &str,
) -> anyhow::Result<()> {
    if load_meta(sqs, dlq_name).await?.is_none() {
        return Ok(());
    }

    let mut new_msg = original.clone();
    new_msg.message_id = new_message_id();
    new_msg.receipt_handle = new_receipt_handle();
    new_msg.receive_count = 0;
    new_msg.visible_after = 0;
    new_msg.attributes.remove("ApproximateReceiveCount");
    new_msg.attributes.remove("ApproximateFirstReceiveTimestamp");

    save_message(sqs, dlq_name, &new_msg).await?;
    sqs.delete(&msg_path(source_queue, &original.message_id)).await?;

    Ok(())
}

/// Find message_id by receipt_handle (using index or scan).
async fn find_msg_id_by_receipt(
    sqs: &Arc<crate::storage::file::FileStorage>,
    queue_name: &str,
    receipt_handle: &str,
) -> anyhow::Result<Option<String>> {
    let idx_path = receipt_index_path(queue_name, receipt_handle);
    match sqs.get(&idx_path).await? {
        Some(b) => return Ok(Some(String::from_utf8_lossy(&b).to_string())),
        None => {}
    }
    // Fallback: scan
    let prefix = format!("queues/{queue_name}/messages/");
    let paths = sqs.list(&prefix).await?;
    for path in &paths {
        if !path.ends_with(".json") { continue; }
        if let Ok(Some(b)) = sqs.get(path).await {
            if let Ok(msg) = serde_json::from_slice::<StoredMessage>(&b) {
                if msg.receipt_handle == receipt_handle {
                    return Ok(Some(msg.message_id));
                }
            }
        }
    }
    Ok(None)
}

// ── JSON helpers for building responses ─────────────────────────────────────

fn msg_attribute_to_json(attr: &MsgAttribute) -> serde_json::Value {
    let mut obj = serde_json::json!({"DataType": attr.data_type});
    if let Some(sv) = &attr.string_value {
        obj["StringValue"] = serde_json::Value::String(sv.clone());
    }
    if let Some(bv) = &attr.binary_value {
        obj["BinaryValue"] = serde_json::Value::String(bv.clone());
    }
    obj
}

fn message_to_json(
    msg: &StoredMessage,
    req_sys_attrs: &[String],
    req_msg_attrs: &[String],
) -> serde_json::Value {
    let want_all_sys = req_sys_attrs.contains(&"All".to_string());
    let want_all_msg = req_msg_attrs.contains(&"All".to_string());

    // System attributes
    let mut attrs = serde_json::Map::new();
    let system_attr_names = [
        "SenderId",
        "SentTimestamp",
        "ApproximateReceiveCount",
        "ApproximateFirstReceiveTimestamp",
    ];
    for name in &system_attr_names {
        if want_all_sys || req_sys_attrs.contains(&name.to_string()) {
            if let Some(val) = msg.attributes.get(*name) {
                attrs.insert(name.to_string(), serde_json::Value::String(val.clone()));
            } else {
                // Provide defaults
                let val = match *name {
                    "ApproximateReceiveCount" => msg.receive_count.to_string(),
                    "SentTimestamp" => (msg.sent_at * 1000).to_string(),
                    _ => continue,
                };
                attrs.insert(name.to_string(), serde_json::Value::String(val));
            }
        }
    }

    // User message attributes
    let mut msg_attrs = serde_json::Map::new();
    for (name, attr) in &msg.message_attributes {
        if want_all_msg || req_msg_attrs.contains(name) {
            msg_attrs.insert(name.clone(), msg_attribute_to_json(attr));
        }
    }

    let mut obj = serde_json::json!({
        "MessageId": msg.message_id,
        "ReceiptHandle": msg.receipt_handle,
        "MD5OfBody": msg.md5_of_body,
        "Body": msg.body,
    });

    if !attrs.is_empty() {
        obj["Attributes"] = serde_json::Value::Object(attrs);
    }
    if !msg_attrs.is_empty() {
        obj["MessageAttributes"] = serde_json::Value::Object(msg_attrs);
    }

    obj
}

// ── Request parsing helpers ──────────────────────────────────────────────────

fn get_str<'a>(body: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(|v| v.as_str())
}

fn get_string(body: &serde_json::Value, key: &str) -> Option<String> {
    get_str(body, key).map(|s| s.to_string())
}

fn get_u64(body: &serde_json::Value, key: &str) -> Option<u64> {
    body.get(key).and_then(|v| v.as_u64())
}

fn get_i64(body: &serde_json::Value, key: &str) -> Option<i64> {
    body.get(key).and_then(|v| v.as_i64())
}

fn get_str_array(body: &serde_json::Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_msg_attributes(body: &serde_json::Value, key: &str) -> HashMap<String, MsgAttribute> {
    let mut result = HashMap::new();
    if let Some(obj) = body.get(key).and_then(|v| v.as_object()) {
        for (name, attr_val) in obj {
            let data_type = attr_val
                .get("DataType")
                .and_then(|v| v.as_str())
                .unwrap_or("String")
                .to_string();
            let string_value = attr_val.get("StringValue").and_then(|v| v.as_str()).map(|s| s.to_string());
            let binary_value = attr_val.get("BinaryValue").and_then(|v| v.as_str()).map(|s| s.to_string());
            result.insert(
                name.clone(),
                MsgAttribute { data_type, string_value, binary_value },
            );
        }
    }
    result
}

fn parse_attributes_map(body: &serde_json::Value) -> HashMap<String, String> {
    let mut result = HashMap::new();
    if let Some(obj) = body.get("Attributes").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                result.insert(k.clone(), s.to_string());
            }
        }
    }
    result
}

fn apply_meta_attributes(meta: &mut QueueMeta, attrs: &HashMap<String, String>) {
    if let Some(v) = attrs.get("VisibilityTimeout").and_then(|s| s.parse().ok()) {
        meta.visibility_timeout = v;
    }
    if let Some(v) = attrs.get("MaximumMessageSize").and_then(|s| s.parse().ok()) {
        meta.max_message_size = v;
    }
    if let Some(v) = attrs.get("MessageRetentionPeriod").and_then(|s| s.parse().ok()) {
        meta.message_retention_seconds = v;
    }
    if let Some(v) = attrs.get("DelaySeconds").and_then(|s| s.parse().ok()) {
        meta.delay_seconds = v;
    }
    if let Some(v) = attrs.get("ReceiveMessageWaitTimeSeconds").and_then(|s| s.parse().ok()) {
        meta.receive_wait_time_seconds = v;
    }
    if let Some(v) = attrs.get("FifoQueue") {
        meta.fifo = v == "true";
    }
    if let Some(v) = attrs.get("ContentBasedDeduplication") {
        meta.content_based_deduplication = v == "true";
    }
    if let Some(v) = attrs.get("RedrivePolicy") {
        if let Ok(rd) = serde_json::from_str::<serde_json::Value>(v) {
            if let Some(arn) = rd.get("deadLetterTargetArn").and_then(|a| a.as_str()) {
                meta.dlq_arn = Some(arn.to_string());
            }
            if let Some(mrc) = rd.get("maxReceiveCount").and_then(|a| a.as_u64()) {
                meta.max_receive_count = Some(mrc as u32);
            }
        }
    }
}

fn meta_matches_attrs(meta: &QueueMeta, attrs: &HashMap<String, String>) -> bool {
    let mut test = meta.clone();
    apply_meta_attributes(&mut test, attrs);
    test.visibility_timeout == meta.visibility_timeout
        && test.max_message_size == meta.max_message_size
        && test.message_retention_seconds == meta.message_retention_seconds
        && test.delay_seconds == meta.delay_seconds
        && test.receive_wait_time_seconds == meta.receive_wait_time_seconds
        && test.fifo == meta.fifo
        && test.content_based_deduplication == meta.content_based_deduplication
        && test.dlq_arn == meta.dlq_arn
        && test.max_receive_count == meta.max_receive_count
}

// ── Operation handlers ───────────────────────────────────────────────────────

async fn handle_create_queue(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    host: &str,
) -> JsonResponse {
    let name = match get_string(body, "QueueName") {
        Some(n) => n,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueName is required"),
    };

    let attrs = parse_attributes_map(body);
    let now = now_secs();

    let mut meta = QueueMeta {
        name: name.clone(),
        url: queue_url_from_host(host, &name),
        arn: queue_arn(&name),
        created_timestamp: now,
        last_modified_timestamp: now,
        visibility_timeout: 30,
        max_message_size: 262144,
        message_retention_seconds: 345600,
        delay_seconds: 0,
        receive_wait_time_seconds: 0,
        fifo: name.ends_with(".fifo"),
        content_based_deduplication: false,
        dlq_arn: None,
        max_receive_count: None,
    };

    apply_meta_attributes(&mut meta, &attrs);

    match load_meta(&state.sqs, &name).await {
        Ok(Some(existing)) => {
            if !meta_matches_attrs(&existing, &attrs) {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "QueueAlreadyExists",
                    &format!("Queue '{name}' already exists with different attributes."),
                );
            }
            return json_ok(serde_json::json!({"QueueUrl": existing.url}));
        }
        Err(e) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string());
        }
        Ok(None) => {}
    }

    if let Err(e) = save_meta(&state.sqs, &meta).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string());
    }

    json_ok(serde_json::json!({"QueueUrl": meta.url}))
}

async fn handle_delete_queue(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    match load_meta(&state.sqs, &queue_name).await {
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
        Ok(Some(_)) => {}
    }

    let prefix = format!("queues/{queue_name}/");
    if let Ok(paths) = state.sqs.list(&prefix).await {
        for path in paths {
            let _ = state.sqs.delete(&path).await;
        }
    }

    json_empty_ok()
}

async fn handle_get_queue_url(
    state: &Arc<AppState>,
    body: &serde_json::Value,
) -> JsonResponse {
    let name = match get_string(body, "QueueName") {
        Some(n) => n,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueName is required"),
    };

    match load_meta(&state.sqs, &name).await {
        Ok(Some(meta)) => json_ok(serde_json::json!({"QueueUrl": meta.url})),
        Ok(None) => queue_not_found(&name),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    }
}

async fn handle_list_queues(
    state: &Arc<AppState>,
    body: &serde_json::Value,
) -> JsonResponse {
    let prefix_filter = get_string(body, "QueueNamePrefix").unwrap_or_default();
    let max_results: usize = get_u64(body, "MaxResults").unwrap_or(1000) as usize;

    let paths = match state.sqs.list("queues/").await {
        Ok(p) => p,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let mut urls = Vec::new();
    for path in &paths {
        if urls.len() >= max_results { break; }
        if !path.ends_with("/_meta.json") { continue; }
        if let Ok(Some(b)) = state.sqs.get(path).await {
            if let Ok(meta) = serde_json::from_slice::<QueueMeta>(&b) {
                if meta.name.starts_with(&prefix_filter) {
                    urls.push(serde_json::Value::String(meta.url));
                }
            }
        }
    }

    json_ok(serde_json::json!({"QueueUrls": urls}))
}

async fn handle_list_dlq_sources(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let target_arn = queue_arn(&queue_name);

    let paths = match state.sqs.list("queues/").await {
        Ok(p) => p,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let mut urls = Vec::new();
    for path in &paths {
        if !path.ends_with("/_meta.json") { continue; }
        if let Ok(Some(b)) = state.sqs.get(path).await {
            if let Ok(meta) = serde_json::from_slice::<QueueMeta>(&b) {
                if meta.dlq_arn.as_deref() == Some(&target_arn) {
                    urls.push(serde_json::Value::String(meta.url));
                }
            }
        }
    }

    json_ok(serde_json::json!({"queueUrls": urls}))
}

async fn handle_get_queue_attributes(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let meta = match load_meta(&state.sqs, &queue_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let requested = get_str_array(body, "AttributeNames");
    let want_all = requested.contains(&"All".to_string()) || requested.is_empty();
    let want = |name: &str| want_all || requested.contains(&name.to_string());

    // Count messages
    let msg_prefix = format!("queues/{queue_name}/messages/");
    let msg_paths = state.sqs.list(&msg_prefix).await.unwrap_or_default();
    let now = now_secs();
    let mut approx_visible = 0usize;
    let mut approx_not_visible = 0usize;
    let mut approx_delayed = 0usize;

    for path in &msg_paths {
        if !path.ends_with(".json") { continue; }
        if let Ok(Some(b)) = state.sqs.get(path).await {
            if let Ok(msg) = serde_json::from_slice::<StoredMessage>(&b) {
                let delayed = msg.delay_until > now;
                let in_flight = msg.visible_after > 0 && msg.visible_after > now;
                if delayed && !in_flight {
                    approx_delayed += 1;
                } else if in_flight {
                    approx_not_visible += 1;
                } else {
                    approx_visible += 1;
                }
            }
        }
    }

    let mut attrs = serde_json::Map::new();

    macro_rules! add_attr {
        ($name:expr, $val:expr) => {
            if want($name) {
                attrs.insert($name.to_string(), serde_json::Value::String($val));
            }
        };
    }

    add_attr!("QueueArn", meta.arn.clone());
    add_attr!("ApproximateNumberOfMessages", approx_visible.to_string());
    add_attr!("ApproximateNumberOfMessagesNotVisible", approx_not_visible.to_string());
    add_attr!("ApproximateNumberOfMessagesDelayed", approx_delayed.to_string());
    add_attr!("VisibilityTimeout", meta.visibility_timeout.to_string());
    add_attr!("CreatedTimestamp", meta.created_timestamp.to_string());
    add_attr!("LastModifiedTimestamp", meta.last_modified_timestamp.to_string());
    add_attr!("MaximumMessageSize", meta.max_message_size.to_string());
    add_attr!("MessageRetentionPeriod", meta.message_retention_seconds.to_string());
    add_attr!("DelaySeconds", meta.delay_seconds.to_string());
    add_attr!("ReceiveMessageWaitTimeSeconds", meta.receive_wait_time_seconds.to_string());

    if meta.fifo && want("FifoQueue") {
        attrs.insert("FifoQueue".to_string(), serde_json::Value::String("true".to_string()));
    }
    if meta.content_based_deduplication && want("ContentBasedDeduplication") {
        attrs.insert("ContentBasedDeduplication".to_string(), serde_json::Value::String("true".to_string()));
    }

    if let (Some(dlq_arn), Some(mrc)) = (&meta.dlq_arn, meta.max_receive_count) {
        if want("RedrivePolicy") {
            let rdp = serde_json::json!({
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": mrc
            })
            .to_string();
            attrs.insert("RedrivePolicy".to_string(), serde_json::Value::String(rdp));
        }
    }

    json_ok(serde_json::json!({"Attributes": attrs}))
}

async fn handle_set_queue_attributes(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let mut meta = match load_meta(&state.sqs, &queue_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let attrs = parse_attributes_map(body);
    apply_meta_attributes(&mut meta, &attrs);
    meta.last_modified_timestamp = now_secs();

    if let Err(e) = save_meta(&state.sqs, &meta).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string());
    }

    json_empty_ok()
}

async fn do_send_message(
    state: &Arc<AppState>,
    queue_name: &str,
    meta: &QueueMeta,
    body_str: &str,
    delay_secs_opt: Option<u64>,
    msg_attrs: HashMap<String, MsgAttribute>,
    group_id: Option<String>,
    dedup_id_param: Option<String>,
) -> JsonResponse {
    let now = now_secs();
    let delay_secs = delay_secs_opt.unwrap_or(meta.delay_seconds);
    let delay_until = if delay_secs > 0 { now + delay_secs } else { 0 };

    // FIFO dedup
    let dedup_id: Option<String> = if meta.fifo {
        Some(match &dedup_id_param {
            Some(id) => id.clone(),
            None if meta.content_based_deduplication => md5_hex(body_str),
            None => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "MessageDeduplicationId required for FIFO queues without ContentBasedDeduplication",
                )
            }
        })
    } else {
        None
    };

    // Check dedup window (5 minutes)
    if let Some(ref did) = dedup_id {
        let dpath = dedup_path(queue_name, did);
        if let Ok(Some(ts_bytes)) = state.sqs.get(&dpath).await {
            let content = String::from_utf8_lossy(&ts_bytes);
            let parts: Vec<&str> = content.trim().splitn(2, ':').collect();
            if parts.len() == 2 {
                let ts: u64 = parts[0].parse().unwrap_or(0);
                if now.saturating_sub(ts) < 300 {
                    let msg_id = parts[1].to_string();
                    return json_ok(serde_json::json!({
                        "MD5OfMessageBody": md5_hex(body_str),
                        "MessageId": msg_id
                    }));
                }
            }
        }
    }

    let message_id = new_message_id();
    let receipt_handle = new_receipt_handle();

    let seq_num = if meta.fifo {
        match next_sequence_number(&state.sqs, queue_name).await {
            Ok(s) => Some(s),
            Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
        }
    } else {
        None
    };

    let mut attributes = HashMap::new();
    attributes.insert("SenderId".into(), "000000000000".into());
    attributes.insert("SentTimestamp".into(), (now * 1000).to_string());
    attributes.insert("ApproximateReceiveCount".into(), "0".into());

    let msg = StoredMessage {
        message_id: message_id.clone(),
        receipt_handle,
        body: body_str.to_string(),
        md5_of_body: md5_hex(body_str),
        attributes,
        message_attributes: msg_attrs,
        sent_at: now,
        delay_until,
        visible_after: 0,
        receive_count: 0,
        group_id,
        deduplication_id: dedup_id.clone(),
        sequence_number: seq_num.clone(),
    };

    if let Err(e) = save_message(&state.sqs, queue_name, &msg).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string());
    }

    if let Some(ref did) = dedup_id {
        let dpath = dedup_path(queue_name, did);
        let _ = state.sqs.put(&dpath, format!("{now}:{message_id}").into_bytes()).await;
    }

    get_or_create_notify(queue_name).notify_waiters();

    let mut resp = serde_json::json!({
        "MD5OfMessageBody": md5_hex(body_str),
        "MessageId": message_id
    });
    if let Some(seq) = seq_num {
        resp["SequenceNumber"] = serde_json::Value::String(seq);
    }
    json_ok(resp)
}

async fn handle_send_message(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let meta = match load_meta(&state.sqs, &queue_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let body_str = match get_string(body, "MessageBody") {
        Some(b) => b,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "MessageBody is required"),
    };

    let delay_secs = get_i64(body, "DelaySeconds").map(|v| v.max(0) as u64);
    let msg_attrs = parse_msg_attributes(body, "MessageAttributes");
    let group_id = get_string(body, "MessageGroupId");
    let dedup_id_param = get_string(body, "MessageDeduplicationId");

    do_send_message(state, &queue_name, &meta, &body_str, delay_secs, msg_attrs, group_id, dedup_id_param).await
}

async fn handle_send_message_batch(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let meta = match load_meta(&state.sqs, &queue_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let entries = match body.get("Entries").and_then(|v| v.as_array()) {
        Some(e) => e.clone(),
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "Entries is required"),
    };

    // Check for distinct IDs
    let mut seen_ids = std::collections::HashSet::new();
    for entry in &entries {
        if let Some(id) = get_str(entry, "Id") {
            if !seen_ids.insert(id.to_string()) {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                    "Two or more batch entries have the same Id.",
                );
            }
        }
    }

    let now = now_secs();
    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for entry in &entries {
        let entry_id = get_string(entry, "Id").unwrap_or_default();
        let body_str = get_string(entry, "MessageBody").unwrap_or_default();
        let delay_secs = get_i64(entry, "DelaySeconds").map(|v| v.max(0) as u64).unwrap_or(meta.delay_seconds);
        let delay_until = if delay_secs > 0 { now + delay_secs } else { 0 };
        let group_id = get_string(entry, "MessageGroupId");
        let dedup_id_param = get_string(entry, "MessageDeduplicationId");
        let msg_attrs = parse_msg_attributes(entry, "MessageAttributes");

        let dedup_id: Option<String> = if meta.fifo {
            match &dedup_id_param {
                Some(id) => Some(id.clone()),
                None if meta.content_based_deduplication => Some(md5_hex(&body_str)),
                None => {
                    failed.push(serde_json::json!({
                        "Id": entry_id,
                        "SenderFault": true,
                        "Code": "InvalidParameterValue",
                        "Message": "MessageDeduplicationId required for FIFO"
                    }));
                    continue;
                }
            }
        } else {
            None
        };

        let message_id = new_message_id();
        let receipt_handle = new_receipt_handle();

        let seq_num = if meta.fifo {
            match next_sequence_number(&state.sqs, &queue_name).await {
                Ok(s) => Some(s),
                Err(_) => None,
            }
        } else {
            None
        };

        let mut attributes = HashMap::new();
        attributes.insert("SenderId".into(), "000000000000".into());
        attributes.insert("SentTimestamp".into(), (now * 1000).to_string());
        attributes.insert("ApproximateReceiveCount".into(), "0".into());

        let msg = StoredMessage {
            message_id: message_id.clone(),
            receipt_handle,
            body: body_str.clone(),
            md5_of_body: md5_hex(&body_str),
            attributes,
            message_attributes: msg_attrs,
            sent_at: now,
            delay_until,
            visible_after: 0,
            receive_count: 0,
            group_id,
            deduplication_id: dedup_id.clone(),
            sequence_number: seq_num.clone(),
        };

        match save_message(&state.sqs, &queue_name, &msg).await {
            Ok(_) => {
                if let Some(ref did) = dedup_id {
                    let dpath = dedup_path(&queue_name, did);
                    let _ = state.sqs.put(&dpath, format!("{now}:{message_id}").into_bytes()).await;
                }
                let mut entry_resp = serde_json::json!({
                    "Id": entry_id,
                    "MessageId": message_id,
                    "MD5OfMessageBody": md5_hex(&body_str)
                });
                if let Some(seq) = seq_num {
                    entry_resp["SequenceNumber"] = serde_json::Value::String(seq);
                }
                successful.push(entry_resp);
            }
            Err(e) => {
                failed.push(serde_json::json!({
                    "Id": entry_id,
                    "SenderFault": false,
                    "Code": "InternalError",
                    "Message": e.to_string()
                }));
            }
        }
    }

    if !entries.is_empty() {
        get_or_create_notify(&queue_name).notify_waiters();
    }

    json_ok(serde_json::json!({
        "Successful": successful,
        "Failed": failed
    }))
}

async fn handle_receive_message(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let meta = match load_meta(&state.sqs, &queue_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let max_msgs: usize = get_i64(body, "MaxNumberOfMessages")
        .map(|v| v.max(1).min(10) as usize)
        .unwrap_or(1);

    let vis_timeout: u64 = get_i64(body, "VisibilityTimeout")
        .map(|v| v.max(0) as u64)
        .unwrap_or(meta.visibility_timeout);

    let wait_secs: u64 = get_i64(body, "WaitTimeSeconds")
        .map(|v| v.max(0).min(20) as u64)
        .unwrap_or(meta.receive_wait_time_seconds);

    // The SDK sends AttributeNames/MessageSystemAttributeNames for system attrs
    // and MessageAttributeNames for user-defined message attrs
    let req_sys_attrs = {
        let mut v = get_str_array(body, "AttributeNames");
        v.extend(get_str_array(body, "MessageSystemAttributeNames"));
        v
    };
    let req_msg_attrs = get_str_array(body, "MessageAttributeNames");

    let dlq_name: Option<String> = meta
        .dlq_arn
        .as_deref()
        .and_then(|arn| arn.split(':').last())
        .map(|s| s.to_string());

    let should_move_to_dlq = |msg: &StoredMessage| -> bool {
        if let (Some(_), Some(mrc)) = (&meta.dlq_arn, meta.max_receive_count) {
            msg.receive_count > mrc
        } else {
            false
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    let notify = get_or_create_notify(&queue_name);

    loop {
        let candidates = match pick_visible_messages(
            &state.sqs,
            &queue_name,
            max_msgs,
            vis_timeout,
            meta.fifo,
        ).await {
            Ok(c) => c,
            Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
        };

        let mut msgs_to_return = Vec::new();
        for msg in candidates {
            if should_move_to_dlq(&msg) {
                if let Some(ref dlq) = dlq_name {
                    let _ = move_to_dlq(&state.sqs, &queue_name, &msg, dlq).await;
                }
            } else {
                msgs_to_return.push(msg);
            }
        }

        if !msgs_to_return.is_empty() {
            let messages_json: Vec<serde_json::Value> = msgs_to_return
                .iter()
                .map(|m| message_to_json(m, &req_sys_attrs, &req_msg_attrs))
                .collect();
            return json_ok(serde_json::json!({"Messages": messages_json}));
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let sleep_dur = remaining.min(Duration::from_millis(200));
        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => {}
            _ = notify.notified() => {}
        }
    }

    json_ok(serde_json::json!({"Messages": []}))
}

async fn handle_delete_message(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let receipt_handle = match get_string(body, "ReceiptHandle") {
        Some(r) => r,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "ReceiptHandle is required"),
    };

    let msg_id = match find_msg_id_by_receipt(&state.sqs, &queue_name, &receipt_handle).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return json_error(StatusCode::BAD_REQUEST, "ReceiptHandleIsInvalid", "The receipt handle is invalid.")
        }
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let _ = state.sqs.delete(&msg_path(&queue_name, &msg_id)).await;
    let _ = state.sqs.delete(&receipt_index_path(&queue_name, &receipt_handle)).await;

    json_empty_ok()
}

async fn handle_delete_message_batch(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let entries = match body.get("Entries").and_then(|v| v.as_array()) {
        Some(e) => e.clone(),
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "Entries is required"),
    };

    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for entry in &entries {
        let entry_id = get_string(entry, "Id").unwrap_or_default();
        let receipt_handle = get_string(entry, "ReceiptHandle").unwrap_or_default();

        match find_msg_id_by_receipt(&state.sqs, &queue_name, &receipt_handle).await {
            Ok(Some(msg_id)) => {
                let _ = state.sqs.delete(&msg_path(&queue_name, &msg_id)).await;
                let _ = state.sqs.delete(&receipt_index_path(&queue_name, &receipt_handle)).await;
                successful.push(serde_json::json!({"Id": entry_id}));
            }
            Ok(None) => {
                failed.push(serde_json::json!({
                    "Id": entry_id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The receipt handle is invalid."
                }));
            }
            Err(e) => {
                failed.push(serde_json::json!({
                    "Id": entry_id,
                    "SenderFault": false,
                    "Code": "InternalError",
                    "Message": e.to_string()
                }));
            }
        }
    }

    json_ok(serde_json::json!({
        "Successful": successful,
        "Failed": failed
    }))
}

async fn handle_change_message_visibility(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let receipt_handle = match get_string(body, "ReceiptHandle") {
        Some(r) => r,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "ReceiptHandle is required"),
    };

    let new_timeout: u64 = match get_i64(body, "VisibilityTimeout").map(|v| v.max(0) as u64) {
        Some(t) => t,
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "VisibilityTimeout is required"),
    };

    let msg_id = match find_msg_id_by_receipt(&state.sqs, &queue_name, &receipt_handle).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return json_error(StatusCode::BAD_REQUEST, "ReceiptHandleIsInvalid", "The receipt handle is invalid.")
        }
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let mut msg = match load_message(&state.sqs, &queue_name, &msg_id).await {
        Ok(Some(m)) => m,
        Ok(None) => return json_error(StatusCode::BAD_REQUEST, "ReceiptHandleIsInvalid", "Message not found."),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
    };

    let now = now_secs();
    msg.visible_after = if new_timeout == 0 { 0 } else { now + new_timeout };

    if let Err(e) = save_message(&state.sqs, &queue_name, &msg).await {
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string());
    }

    if new_timeout == 0 {
        get_or_create_notify(&queue_name).notify_waiters();
    }

    json_empty_ok()
}

async fn handle_change_message_visibility_batch(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    let entries = match body.get("Entries").and_then(|v| v.as_array()) {
        Some(e) => e.clone(),
        None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "Entries is required"),
    };

    let now = now_secs();
    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for entry in &entries {
        let entry_id = get_string(entry, "Id").unwrap_or_default();
        let receipt_handle = get_string(entry, "ReceiptHandle").unwrap_or_default();
        let new_timeout: u64 = get_i64(entry, "VisibilityTimeout")
            .map(|v| v.max(0) as u64)
            .unwrap_or(0);

        match find_msg_id_by_receipt(&state.sqs, &queue_name, &receipt_handle).await {
            Ok(Some(msg_id)) => {
                if let Ok(Some(mut msg)) = load_message(&state.sqs, &queue_name, &msg_id).await {
                    msg.visible_after = if new_timeout == 0 { 0 } else { now + new_timeout };
                    let _ = save_message(&state.sqs, &queue_name, &msg).await;
                    successful.push(serde_json::json!({"Id": entry_id}));
                }
            }
            Ok(None) => {
                failed.push(serde_json::json!({
                    "Id": entry_id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The receipt handle is invalid."
                }));
            }
            Err(e) => {
                failed.push(serde_json::json!({
                    "Id": entry_id,
                    "SenderFault": false,
                    "Code": "InternalError",
                    "Message": e.to_string()
                }));
            }
        }
    }

    json_ok(serde_json::json!({
        "Successful": successful,
        "Failed": failed
    }))
}

async fn handle_purge_queue(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    let queue_name = if let Some(n) = queue_name_from_path {
        n.to_string()
    } else {
        match get_string(body, "QueueUrl").and_then(|u| queue_name_from_url(&u)) {
            Some(n) => n,
            None => return json_error(StatusCode::BAD_REQUEST, "MissingParameter", "QueueUrl is required"),
        }
    };

    match load_meta(&state.sqs, &queue_name).await {
        Ok(None) => return queue_not_found(&queue_name),
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalFailure", &e.to_string()),
        Ok(Some(_)) => {}
    }

    for sub in &["messages", "receipts", "dedup"] {
        let prefix = format!("queues/{queue_name}/{sub}/");
        if let Ok(paths) = state.sqs.list(&prefix).await {
            for path in paths {
                let _ = state.sqs.delete(&path).await;
            }
        }
    }

    json_empty_ok()
}

// ── Dispatch ─────────────────────────────────────────────────────────────────

async fn dispatch_action(
    state: &Arc<AppState>,
    body: &serde_json::Value,
    host: &str,
    action: &str,
    queue_name_from_path: Option<&str>,
) -> JsonResponse {
    match action {
        "CreateQueue" => handle_create_queue(state, body, host).await,
        "DeleteQueue" => handle_delete_queue(state, body, queue_name_from_path).await,
        "GetQueueUrl" => handle_get_queue_url(state, body).await,
        "ListQueues" => handle_list_queues(state, body).await,
        "ListDeadLetterSourceQueues" => handle_list_dlq_sources(state, body, queue_name_from_path).await,
        "GetQueueAttributes" => handle_get_queue_attributes(state, body, queue_name_from_path).await,
        "SetQueueAttributes" => handle_set_queue_attributes(state, body, queue_name_from_path).await,
        "SendMessage" => handle_send_message(state, body, queue_name_from_path).await,
        "SendMessageBatch" => handle_send_message_batch(state, body, queue_name_from_path).await,
        "ReceiveMessage" => handle_receive_message(state, body, queue_name_from_path).await,
        "DeleteMessage" => handle_delete_message(state, body, queue_name_from_path).await,
        "DeleteMessageBatch" => handle_delete_message_batch(state, body, queue_name_from_path).await,
        "ChangeMessageVisibility" => handle_change_message_visibility(state, body, queue_name_from_path).await,
        "ChangeMessageVisibilityBatch" => handle_change_message_visibility_batch(state, body, queue_name_from_path).await,
        "PurgeQueue" => handle_purge_queue(state, body, queue_name_from_path).await,
        other => {
            tracing::warn!("unknown SQS action: {other}");
            json_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                &format!("unknown action: {other}"),
            )
        }
    }
}

/// Parse request: extract host, action from X-Amz-Target, and JSON body.
async fn parse_request(
    request: Request,
) -> Result<(serde_json::Value, String, String), JsonResponse> {
    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:4566")
        .to_string();

    // Action from X-Amz-Target header: e.g. "AmazonSQS.CreateQueue"
    let action_from_target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split('.')
        .last()
        .unwrap_or("")
        .to_string();

    let bytes = to_bytes(request.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| {
            json_error(StatusCode::BAD_REQUEST, "InvalidInput", &format!("failed to read body: {e}"))
        })?;

    let mut body: serde_json::Value = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).map_err(|e| {
            json_error(StatusCode::BAD_REQUEST, "InvalidInput", &format!("failed to parse body: {e}"))
        })?
    };

    // Action: prefer body's "Action" key (for fallback compatibility), else X-Amz-Target
    let action = body
        .get("Action")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or(action_from_target);

    Ok((body, host, action))
}

/// Handler for service-level POST /sqs/ and POST / (via top-level dispatch)
pub(crate) async fn service_dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let (body, host, action) = match parse_request(request).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    tracing::debug!("SQS service_dispatch action={action}");
    dispatch_action(&state, &body, &host, &action, None)
        .await
        .into_response()
}

/// Handler for queue-specific POST /000000000000/{queue_name}
async fn queue_dispatch(
    State(state): State<Arc<AppState>>,
    Path(queue_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let (body, host, action) = match parse_request(request).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    tracing::debug!("SQS queue_dispatch queue={queue_name} action={action}");
    dispatch_action(&state, &body, &host, &action, Some(&queue_name))
        .await
        .into_response()
}

// ── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/sqs/", post(service_dispatch))
        .route("/000000000000/{queue_name}", post(queue_dispatch))
}
