//! Lambda service implementation.
//!
//! Wire format: REST/JSON over paths rooted at /2015-03-31/.
//! Routing: path-based; SigV4 credential scope service = "lambda".

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use base64::Engine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::Digest as Sha2Digest;
use uuid::Uuid;

use crate::services::AppState;
use crate::storage::Storage;

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFunction {
    name: String,
    arn: String,
    description: String,
    runtime: String,
    handler: String,
    role: String,
    timeout: u32,
    memory_size: u32,
    environment: HashMap<String, String>,
    package_type: String,
    image_uri: Option<String>,
    code_sha256: String,
    code_size: i64,
    last_modified: String,
    state: String,
    version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAlias {
    name: String,
    function_name: String,
    function_version: String,
    description: String,
    arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEventSourceMapping {
    uuid: String,
    event_source_arn: String,
    function_arn: String,
    function_name: String,
    batch_size: i64,
    starting_position: String,
    state: String,
    last_modified: f64,
    maximum_retry_attempts: i64,
    bisect_batch_on_function_error: bool,
}

// ── Storage path helpers ─────────────────────────────────────────────────────

fn fn_path(name: &str) -> String {
    format!("functions/{name}.json")
}

fn fn_code_path(name: &str) -> String {
    format!("functions/{name}/code.zip")
}

fn alias_path(fn_name: &str, alias_name: &str) -> String {
    format!("aliases/{fn_name}/{alias_name}.json")
}

fn esm_path(uuid: &str) -> String {
    format!("esms/{uuid}.json")
}

fn policy_path(fn_name: &str) -> String {
    format!("policies/{fn_name}.json")
}

// ── Storage helpers ──────────────────────────────────────────────────────────

async fn load_function(
    lambda: &Arc<crate::storage::file::FileStorage>,
    name: &str,
) -> anyhow::Result<Option<StoredFunction>> {
    match lambda.get(&fn_path(name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_function(
    lambda: &Arc<crate::storage::file::FileStorage>,
    f: &StoredFunction,
) -> anyhow::Result<()> {
    lambda.put(&fn_path(&f.name), serde_json::to_vec(f)?).await
}

async fn delete_function_data(
    lambda: &Arc<crate::storage::file::FileStorage>,
    name: &str,
) -> anyhow::Result<()> {
    let _ = lambda.delete(&fn_path(name)).await;
    let _ = lambda.delete(&fn_code_path(name)).await;
    Ok(())
}

async fn list_functions(
    lambda: &Arc<crate::storage::file::FileStorage>,
) -> anyhow::Result<Vec<StoredFunction>> {
    let keys = lambda.list("functions/").await?;
    let mut result = Vec::new();
    for key in keys {
        // Only top-level function JSONs, not code.zip files inside subdirs
        if !key.ends_with(".json") {
            continue;
        }
        // functions/{name}.json — must be at depth 1 (no extra '/' after functions/)
        let rest = key.strip_prefix("functions/").unwrap_or(&key);
        if rest.contains('/') {
            continue;
        }
        if let Ok(Some(b)) = lambda.get(&key).await {
            if let Ok(f) = serde_json::from_slice::<StoredFunction>(&b) {
                result.push(f);
            }
        }
    }
    Ok(result)
}

async fn load_alias(
    lambda: &Arc<crate::storage::file::FileStorage>,
    fn_name: &str,
    alias_name: &str,
) -> anyhow::Result<Option<StoredAlias>> {
    match lambda.get(&alias_path(fn_name, alias_name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_alias(
    lambda: &Arc<crate::storage::file::FileStorage>,
    a: &StoredAlias,
) -> anyhow::Result<()> {
    lambda
        .put(&alias_path(&a.function_name, &a.name), serde_json::to_vec(a)?)
        .await
}

async fn list_aliases_for_fn(
    lambda: &Arc<crate::storage::file::FileStorage>,
    fn_name: &str,
) -> anyhow::Result<Vec<StoredAlias>> {
    let prefix = format!("aliases/{fn_name}/");
    let keys = lambda.list(&prefix).await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = lambda.get(&key).await {
            if let Ok(a) = serde_json::from_slice::<StoredAlias>(&b) {
                result.push(a);
            }
        }
    }
    Ok(result)
}

async fn load_esm(
    lambda: &Arc<crate::storage::file::FileStorage>,
    uuid: &str,
) -> anyhow::Result<Option<StoredEventSourceMapping>> {
    match lambda.get(&esm_path(uuid)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_esm(
    lambda: &Arc<crate::storage::file::FileStorage>,
    esm: &StoredEventSourceMapping,
) -> anyhow::Result<()> {
    lambda.put(&esm_path(&esm.uuid), serde_json::to_vec(esm)?).await
}

async fn list_esms(
    lambda: &Arc<crate::storage::file::FileStorage>,
) -> anyhow::Result<Vec<StoredEventSourceMapping>> {
    let keys = lambda.list("esms/").await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = lambda.get(&key).await {
            if let Ok(e) = serde_json::from_slice::<StoredEventSourceMapping>(&b) {
                result.push(e);
            }
        }
    }
    Ok(result)
}

// ── Utility helpers ──────────────────────────────────────────────────────────

fn function_arn(name: &str) -> String {
    format!("arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}")
}

fn alias_arn(fn_name: &str, alias_name: &str) -> String {
    format!("{}:{}", function_arn(fn_name), alias_name)
}

fn now_iso8601() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn now_unix_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn function_to_json(f: &StoredFunction) -> serde_json::Value {
    serde_json::json!({
        "FunctionName": f.name,
        "FunctionArn": f.arn,
        "Runtime": f.runtime,
        "Role": f.role,
        "Handler": f.handler,
        "Description": f.description,
        "Timeout": f.timeout,
        "MemorySize": f.memory_size,
        "CodeSize": f.code_size,
        "CodeSha256": f.code_sha256,
        "LastModified": f.last_modified,
        "State": f.state,
        "Version": f.version,
        "PackageType": f.package_type,
        "ImageUri": f.image_uri,
        "Environment": { "Variables": f.environment },
        "TracingConfig": { "Mode": "PassThrough" }
    })
}

fn esm_to_json(e: &StoredEventSourceMapping) -> serde_json::Value {
    serde_json::json!({
        "UUID": e.uuid,
        "EventSourceArn": e.event_source_arn,
        "FunctionArn": e.function_arn,
        "BatchSize": e.batch_size,
        "StartingPosition": e.starting_position,
        "State": e.state,
        "LastModified": e.last_modified,
        "MaximumRetryAttempts": e.maximum_retry_attempts,
        "BisectBatchOnFunctionError": e.bisect_batch_on_function_error
    })
}

fn alias_to_json(a: &StoredAlias) -> serde_json::Value {
    serde_json::json!({
        "Name": a.name,
        "FunctionVersion": a.function_version,
        "Description": a.description,
        "AliasArn": a.arn
    })
}

/// Resolve a FunctionName that may be a name, partial ARN, or full ARN.
fn resolve_function_name(function_name_or_arn: &str) -> String {
    // ARN: arn:aws:lambda:...:function:{name}
    if function_name_or_arn.starts_with("arn:") {
        function_name_or_arn
            .split(':')
            .last()
            .unwrap_or(function_name_or_arn)
            .to_string()
    } else {
        function_name_or_arn.to_string()
    }
}

// ── JSON response helpers ────────────────────────────────────────────────────

fn json_response(status: StatusCode, body: serde_json::Value) -> impl IntoResponse {
    (
        status,
        [(
            axum::http::header::CONTENT_TYPE,
            "application/json",
        )],
        body.to_string(),
    )
}

fn not_found(resource: &str, name: &str) -> impl IntoResponse {
    json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({
            "Type": "ResourceNotFoundException",
            "message": format!("{resource} not found: {name}")
        }),
    )
}

fn internal_error(msg: &str) -> impl IntoResponse {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "Type": "ServiceException",
            "message": msg
        }),
    )
}

// ── Docker helpers ───────────────────────────────────────────────────────────

/// Start a Docker container for image-based function invocation.
/// Returns (container_id, host_port) or an error string.
async fn start_container(
    image_uri: &str,
    function_name: &str,
    env: &HashMap<String, String>,
) -> Result<(String, u16), String> {
    let mut cmd = tokio::process::Command::new("docker");
    cmd.arg("run")
        .arg("--rm")
        .arg("-d")
        .arg("-p")
        .arg("0:8080")
        .arg("--name")
        .arg(format!("cloudish-lambda-{function_name}"));

    for (k, v) in env {
        cmd.arg("-e").arg(format!("{k}={v}"));
    }

    cmd.arg(image_uri);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("docker run failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("docker run error: {stderr}"));
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // Get the mapped host port
    let port_output = tokio::process::Command::new("docker")
        .arg("port")
        .arg(&container_id)
        .arg("8080")
        .output()
        .await
        .map_err(|e| format!("docker port failed: {e}"))?;

    if !port_output.status.success() {
        let _ = stop_container(&container_id).await;
        return Err("docker port returned error".to_string());
    }

    let port_str = String::from_utf8_lossy(&port_output.stdout);
    // Output: "0.0.0.0:PORT\n" or ":::PORT\n"
    let host_port: u16 = port_str
        .trim()
        .split(':')
        .last()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("failed to parse port from: {port_str}"))?;

    // Wait up to 10s for RIE to be ready
    let url = format!("http://127.0.0.1:{host_port}/2015-03-31/functions/function/invocations");
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match client.get(&url).send().await {
            Ok(_) => break,
            Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    let _ = stop_container(&container_id).await;
                    return Err("RIE did not become ready within 10s".to_string());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    Ok((container_id, host_port))
}

/// Stop and remove a Docker container.
async fn stop_container(container_id: &str) -> () {
    let _ = tokio::process::Command::new("docker")
        .arg("rm")
        .arg("-f")
        .arg(container_id)
        .output()
        .await;
}

/// Invoke a function via Docker RIE.
/// Returns (response_body, Option<x-amz-function-error header value>).
async fn invoke_via_rie(
    host_port: u16,
    event_json: &str,
) -> Result<(String, Option<String>), String> {
    let url = format!(
        "http://127.0.0.1:{host_port}/2015-03-31/functions/function/invocations"
    );
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(event_json.to_string())
        .send()
        .await
        .map_err(|e| format!("RIE POST failed: {e}"))?;

    let error_header = response
        .headers()
        .get("x-amz-function-error")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let body = response
        .text()
        .await
        .map_err(|e| format!("reading RIE response body: {e}"))?;

    Ok((body, error_header))
}

// ── Internal invoke ──────────────────────────────────────────────────────────

/// Find or start a container for the function, then invoke it with event_json.
pub async fn invoke_function_internal(
    state: &Arc<AppState>,
    function_name: &str,
    event_json: &str,
) -> Result<(String, Option<String>), (StatusCode, String)> {
    let f = match load_function(&state.lambda, function_name).await {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("Function not found: {function_name}"),
            ))
        }
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    if f.package_type != "Image" || f.image_uri.is_none() {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "Zip-based invocation not supported; use PackageType=Image with an ImageUri"
                .to_string(),
        ));
    }

    let image_uri = f.image_uri.as_deref().unwrap().to_string();

    // Check for existing container
    let existing = {
        let containers = state.lambda_containers.lock().await;
        containers.get(function_name).cloned()
    };

    let host_port = if let Some((container_id, port)) = existing {
        // Ping to verify it's still alive
        let url = format!(
            "http://127.0.0.1:{port}/2015-03-31/functions/function/invocations"
        );
        let alive = reqwest::Client::new().get(&url).send().await.is_ok();
        if alive {
            port
        } else {
            // Container died — remove from map and restart
            {
                let mut containers = state.lambda_containers.lock().await;
                containers.remove(function_name);
            }
            match start_container(&image_uri, function_name, &f.environment).await {
                Ok((cid, p)) => {
                    let mut containers = state.lambda_containers.lock().await;
                    containers.insert(function_name.to_string(), (cid, p));
                    p
                }
                Err(e) => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to start container: {e}"),
                    ))
                }
            }
        }
    } else {
        match start_container(&image_uri, function_name, &f.environment).await {
            Ok((cid, p)) => {
                let mut containers = state.lambda_containers.lock().await;
                containers.insert(function_name.to_string(), (cid, p));
                p
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to start container: {e}"),
                ))
            }
        }
    };

    invoke_via_rie(host_port, event_json)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

// ── ESM task management ──────────────────────────────────────────────────────

/// Spawn a background task for an event source mapping.
pub(crate) fn spawn_esm_task(state: &Arc<AppState>, esm: &StoredEventSourceMapping) {
    let handle = if esm.event_source_arn.contains(":sqs:") {
        let state_clone = state.clone();
        let esm_clone = esm.clone();
        let join = tokio::spawn(sqs_poller(state_clone, esm_clone));
        join.abort_handle()
    } else if esm.event_source_arn.contains(":dynamodb:") {
        let state_clone = state.clone();
        let esm_clone = esm.clone();
        let join = tokio::spawn(dynamo_streams_poller(state_clone, esm_clone));
        join.abort_handle()
    } else {
        tracing::warn!(
            "ESM {}: unknown event source type for ARN: {}",
            esm.uuid,
            esm.event_source_arn
        );
        return;
    };

    // Store abort handle (non-blocking — use try_lock, or block briefly)
    let esm_tasks = state.esm_tasks.clone();
    let uuid = esm.uuid.clone();
    tokio::spawn(async move {
        let mut tasks = esm_tasks.lock().await;
        tasks.insert(uuid, handle);
    });
}

/// Start all ESM tasks from persisted storage (called at startup).
pub async fn start_esm_tasks(state: &Arc<AppState>) {
    let esms = match list_esms(&state.lambda).await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("Failed to load ESMs at startup: {e}");
            return;
        }
    };
    for esm in &esms {
        if esm.state == "Enabled" {
            spawn_esm_task(state, esm);
        }
    }
}

/// SQS event source mapping poller.
async fn sqs_poller(state: Arc<AppState>, esm: StoredEventSourceMapping) {
    let queue_name = esm
        .event_source_arn
        .split(':')
        .last()
        .unwrap_or_default()
        .to_string();

    loop {
        let records =
            crate::services::sqs::receive_batch_for_esm(&state, &queue_name, esm.batch_size as usize)
                .await;

        if records.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let event_records: Vec<serde_json::Value> = records
            .iter()
            .map(|r| {
                serde_json::json!({
                    "messageId": r.message_id,
                    "receiptHandle": r.receipt_handle,
                    "body": r.body,
                    "attributes": r.attributes,
                    "messageAttributes": {},
                    "md5OfBody": r.md5_of_body,
                    "eventSource": "aws:sqs",
                    "eventSourceARN": r.queue_arn,
                    "awsRegion": REGION
                })
            })
            .collect();

        let event = serde_json::json!({"Records": event_records}).to_string();
        let fn_name = esm.function_arn.split(':').last().unwrap_or("").to_string();

        match invoke_function_internal(&state, &fn_name, &event).await {
            Ok((_, None)) => {
                // Success — delete all processed messages
                for r in &records {
                    crate::services::sqs::delete_message_for_esm(
                        &state,
                        &queue_name,
                        &r.receipt_handle,
                    )
                    .await;
                }
            }
            Ok((body, Some(err_type))) => {
                tracing::warn!(
                    "Lambda ESM SQS function returned error: {err_type} — {body}"
                );
                // Leave messages visible again (will be retried)
            }
            Err((status, msg)) => {
                tracing::warn!("Lambda ESM SQS invoke failed: {status} {msg}");
            }
        }
    }
}

/// DynamoDB Streams event source mapping poller.
async fn dynamo_streams_poller(state: Arc<AppState>, esm: StoredEventSourceMapping) {
    let table_name = esm
        .event_source_arn
        .split('/')
        .nth(1)
        .unwrap_or_default()
        .to_string();

    let token_str = format!("{table_name}:0");
    let mut current_iterator = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(token_str.as_bytes());

    loop {
        let (records, next_iterator) =
            get_stream_records(&state, &current_iterator).await;
        current_iterator = next_iterator;

        if !records.is_empty() {
            let fn_name = esm.function_arn.split(':').last().unwrap_or("").to_string();
            let event = serde_json::json!({
                "Records": records.iter().map(|r| {
                    serde_json::json!({
                        "eventSource": "aws:dynamodb",
                        "eventSourceARN": esm.event_source_arn,
                        "awsRegion": REGION,
                        "dynamodb": r
                    })
                }).collect::<Vec<_>>()
            })
            .to_string();

            if let Err((status, msg)) =
                invoke_function_internal(&state, &fn_name, &event).await
            {
                tracing::warn!("Lambda ESM DynamoDB invoke failed: {status} {msg}");
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn get_stream_records(
    state: &Arc<AppState>,
    iterator: &str,
) -> (Vec<serde_json::Value>, String) {
    let Ok(token_bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(iterator)
    else {
        return (vec![], iterator.to_string());
    };
    let token_str = String::from_utf8_lossy(&token_bytes);
    let Some((table_name, seq_str)) = token_str.split_once(':') else {
        return (vec![], iterator.to_string());
    };
    let start_seq: u64 = seq_str.parse().unwrap_or(0);

    let prefix = format!("streams/{table_name}");
    let all_keys = match state.dynamodb.list(&prefix).await {
        Ok(k) => k,
        Err(_) => return (vec![], iterator.to_string()),
    };

    let mut record_keys: Vec<(u64, String)> = all_keys
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .filter_map(|k| {
            let filename = k.split('/').last()?;
            let seq: u64 = filename.strip_suffix(".json")?.parse().ok()?;
            if seq >= start_seq { Some((seq, k)) } else { None }
        })
        .collect();
    record_keys.sort_by_key(|(seq, _)| *seq);
    record_keys.truncate(100);

    let mut records = vec![];
    let mut last_seq = start_seq;
    for (seq, key) in &record_keys {
        if let Ok(Some(bytes)) = state.dynamodb.get(key).await {
            if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                records.push(record);
                last_seq = *seq + 1;
            }
        }
    }

    let next_str = format!("{table_name}:{last_seq}");
    let next_iterator =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(next_str.as_bytes());
    (records, next_iterator)
}

// ── Request body helper ──────────────────────────────────────────────────────

async fn read_body(request: Request) -> serde_json::Value {
    let bytes = to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({}))
    }
}

// ── Function handlers ────────────────────────────────────────────────────────

async fn create_function(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let name = match body.get("FunctionName").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"Type": "InvalidParameterValueException", "message": "FunctionName is required"}),
            )
            .into_response()
        }
    };

    let arn = function_arn(&name);

    // Check if already exists
    match load_function(&state.lambda, &name).await {
        Ok(Some(_)) => {
            return json_response(
                StatusCode::CONFLICT,
                serde_json::json!({
                    "Type": "ResourceConflictException",
                    "message": format!("Function already exists: {arn}")
                }),
            )
            .into_response()
        }
        Err(e) => return internal_error(&e.to_string()).into_response(),
        Ok(None) => {}
    }

    let description = body
        .get("Description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let runtime = body
        .get("Runtime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let handler = body
        .get("Handler")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let role = body
        .get("Role")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let timeout: u32 = body
        .get("Timeout")
        .and_then(|v| v.as_u64())
        .unwrap_or(3) as u32;
    let memory_size: u32 = body
        .get("MemorySize")
        .and_then(|v| v.as_u64())
        .unwrap_or(128) as u32;
    let package_type = body
        .get("PackageType")
        .and_then(|v| v.as_str())
        .unwrap_or("Zip")
        .to_string();

    let environment: HashMap<String, String> = body
        .get("Environment")
        .and_then(|e| e.get("Variables"))
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // Image URI: Code.ImageUri or top-level ImageUri
    let image_uri: Option<String> = body
        .get("Code")
        .and_then(|c| c.get("ImageUri"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            body.get("ImageUri")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });

    // Zip code
    let (code_size, code_sha256) = if let Some(zip_b64) = body
        .get("Code")
        .and_then(|c| c.get("ZipFile"))
        .and_then(|v| v.as_str())
    {
        match base64::engine::general_purpose::STANDARD.decode(zip_b64) {
            Ok(zip_bytes) => {
                let size = zip_bytes.len() as i64;
                let sha = format!("{:x}", sha2::Sha256::digest(&zip_bytes));
                if let Err(e) = state.lambda.put(&fn_code_path(&name), zip_bytes).await {
                    return internal_error(&e.to_string()).into_response();
                }
                (size, sha)
            }
            Err(_) => (0, "".to_string()),
        }
    } else {
        (0, "".to_string())
    };

    let f = StoredFunction {
        name: name.clone(),
        arn: arn.clone(),
        description,
        runtime,
        handler,
        role,
        timeout,
        memory_size,
        environment,
        package_type,
        image_uri,
        code_sha256,
        code_size,
        last_modified: now_iso8601(),
        state: "Active".to_string(),
        version: "$LATEST".to_string(),
    };

    if let Err(e) = save_function(&state.lambda, &f).await {
        return internal_error(&e.to_string()).into_response();
    }

    json_response(StatusCode::CREATED, function_to_json(&f)).into_response()
}

async fn list_functions_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match list_functions(&state.lambda).await {
        Ok(fns) => {
            let arr: Vec<_> = fns.iter().map(function_to_json).collect();
            json_response(
                StatusCode::OK,
                serde_json::json!({"Functions": arr, "NextMarker": null}),
            )
            .into_response()
        }
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn get_function_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_function(&state.lambda, &name).await {
        Ok(Some(f)) => {
            let repo_type = if f.package_type == "Image" { "ECR" } else { "S3" };
            json_response(
                StatusCode::OK,
                serde_json::json!({
                    "Configuration": function_to_json(&f),
                    "Code": {"RepositoryType": repo_type},
                    "Tags": {}
                }),
            )
            .into_response()
        }
        Ok(None) => not_found("Function", &name).into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn delete_function_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_function(&state.lambda, &name).await {
        Ok(None) => return not_found("Function", &name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    // Stop container if running
    let container = {
        let mut containers = state.lambda_containers.lock().await;
        containers.remove(&name)
    };
    if let Some((container_id, _)) = container {
        stop_container(&container_id).await;
    }

    if let Err(e) = delete_function_data(&state.lambda, &name).await {
        return internal_error(&e.to_string()).into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn update_function_code(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut f = match load_function(&state.lambda, &name).await {
        Ok(Some(f)) => f,
        Ok(None) => return not_found("Function", &name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    if let Some(image_uri) = body.get("ImageUri").and_then(|v| v.as_str()) {
        f.image_uri = Some(image_uri.to_string());
        f.code_sha256 = "".to_string();
        f.code_size = 0;
    } else if let Some(zip_b64) = body.get("ZipFile").and_then(|v| v.as_str()) {
        match base64::engine::general_purpose::STANDARD.decode(zip_b64) {
            Ok(zip_bytes) => {
                f.code_size = zip_bytes.len() as i64;
                f.code_sha256 = format!("{:x}", sha2::Digest::finalize(sha2::Sha256::new_with_prefix(&zip_bytes)));
                if let Err(e) = state.lambda.put(&fn_code_path(&name), zip_bytes).await {
                    return internal_error(&e.to_string()).into_response();
                }
            }
            Err(_) => {}
        }
    }

    f.last_modified = now_iso8601();

    // Stop old container so it restarts fresh on next invoke
    let container = {
        let mut containers = state.lambda_containers.lock().await;
        containers.remove(&name)
    };
    if let Some((container_id, _)) = container {
        stop_container(&container_id).await;
    }

    if let Err(e) = save_function(&state.lambda, &f).await {
        return internal_error(&e.to_string()).into_response();
    }

    json_response(StatusCode::OK, function_to_json(&f)).into_response()
}

async fn get_function_configuration(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_function(&state.lambda, &name).await {
        Ok(Some(f)) => json_response(StatusCode::OK, function_to_json(&f)).into_response(),
        Ok(None) => not_found("Function", &name).into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn update_function_configuration(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut f = match load_function(&state.lambda, &name).await {
        Ok(Some(f)) => f,
        Ok(None) => return not_found("Function", &name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    if let Some(v) = body.get("Description").and_then(|v| v.as_str()) {
        f.description = v.to_string();
    }
    if let Some(v) = body.get("Runtime").and_then(|v| v.as_str()) {
        f.runtime = v.to_string();
    }
    if let Some(v) = body.get("Handler").and_then(|v| v.as_str()) {
        f.handler = v.to_string();
    }
    if let Some(v) = body.get("Role").and_then(|v| v.as_str()) {
        f.role = v.to_string();
    }
    if let Some(v) = body.get("Timeout").and_then(|v| v.as_u64()) {
        f.timeout = v as u32;
    }
    if let Some(v) = body.get("MemorySize").and_then(|v| v.as_u64()) {
        f.memory_size = v as u32;
    }
    if let Some(env_vars) = body
        .get("Environment")
        .and_then(|e| e.get("Variables"))
        .and_then(|v| v.as_object())
    {
        f.environment = env_vars
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
    }

    f.last_modified = now_iso8601();

    if let Err(e) = save_function(&state.lambda, &f).await {
        return internal_error(&e.to_string()).into_response();
    }

    json_response(StatusCode::OK, function_to_json(&f)).into_response()
}

async fn invoke_function(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let bytes = to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let event_json = String::from_utf8_lossy(&bytes).to_string();
    let event_json = if event_json.is_empty() { "{}".to_string() } else { event_json };

    match invoke_function_internal(&state, &name, &event_json).await {
        Ok((body, error_header)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            if let Some(err) = error_header {
                if let Ok(val) = HeaderValue::from_str(&err) {
                    headers.insert(
                        HeaderName::from_static("x-amz-function-error"),
                        val,
                    );
                }
            }
            (StatusCode::OK, headers, body).into_response()
        }
        Err((status, msg)) => json_response(
            status,
            serde_json::json!({"Type": "ServiceException", "message": msg}),
        )
        .into_response(),
    }
}

// ── Alias handlers ───────────────────────────────────────────────────────────

async fn create_alias(
    State(state): State<Arc<AppState>>,
    Path(fn_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    // Verify function exists
    match load_function(&state.lambda, &fn_name).await {
        Ok(None) => return not_found("Function", &fn_name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    let alias_name = match body.get("Name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"Type": "InvalidParameterValueException", "message": "Name is required"}),
            )
            .into_response()
        }
    };

    let function_version = body
        .get("FunctionVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("$LATEST")
        .to_string();
    let description = body
        .get("Description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let a = StoredAlias {
        name: alias_name.clone(),
        function_name: fn_name.clone(),
        function_version,
        description,
        arn: alias_arn(&fn_name, &alias_name),
    };

    if let Err(e) = save_alias(&state.lambda, &a).await {
        return internal_error(&e.to_string()).into_response();
    }

    json_response(StatusCode::CREATED, alias_to_json(&a)).into_response()
}

async fn get_alias(
    State(state): State<Arc<AppState>>,
    Path((fn_name, alias_name)): Path<(String, String)>,
) -> impl IntoResponse {
    match load_alias(&state.lambda, &fn_name, &alias_name).await {
        Ok(Some(a)) => json_response(StatusCode::OK, alias_to_json(&a)).into_response(),
        Ok(None) => not_found("Alias", &alias_name).into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn list_aliases_handler(
    State(state): State<Arc<AppState>>,
    Path(fn_name): Path<String>,
) -> impl IntoResponse {
    match list_aliases_for_fn(&state.lambda, &fn_name).await {
        Ok(aliases) => {
            let arr: Vec<_> = aliases.iter().map(alias_to_json).collect();
            json_response(StatusCode::OK, serde_json::json!({"Aliases": arr})).into_response()
        }
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn update_alias(
    State(state): State<Arc<AppState>>,
    Path((fn_name, alias_name)): Path<(String, String)>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut a = match load_alias(&state.lambda, &fn_name, &alias_name).await {
        Ok(Some(a)) => a,
        Ok(None) => return not_found("Alias", &alias_name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    if let Some(v) = body.get("FunctionVersion").and_then(|v| v.as_str()) {
        a.function_version = v.to_string();
    }
    if let Some(v) = body.get("Description").and_then(|v| v.as_str()) {
        a.description = v.to_string();
    }

    if let Err(e) = save_alias(&state.lambda, &a).await {
        return internal_error(&e.to_string()).into_response();
    }

    json_response(StatusCode::OK, alias_to_json(&a)).into_response()
}

async fn delete_alias(
    State(state): State<Arc<AppState>>,
    Path((fn_name, alias_name)): Path<(String, String)>,
) -> impl IntoResponse {
    match load_alias(&state.lambda, &fn_name, &alias_name).await {
        Ok(None) => return not_found("Alias", &alias_name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    let _ = state.lambda.delete(&alias_path(&fn_name, &alias_name)).await;
    StatusCode::NO_CONTENT.into_response()
}

// ── Policy handlers ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LambdaPolicy {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Statement")]
    statement: Vec<serde_json::Value>,
}

async fn load_policy(
    lambda: &Arc<crate::storage::file::FileStorage>,
    fn_name: &str,
) -> anyhow::Result<Option<LambdaPolicy>> {
    match lambda.get(&policy_path(fn_name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_policy(
    lambda: &Arc<crate::storage::file::FileStorage>,
    fn_name: &str,
    policy: &LambdaPolicy,
) -> anyhow::Result<()> {
    lambda
        .put(&policy_path(fn_name), serde_json::to_vec(policy)?)
        .await
}

async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(fn_name): Path<String>,
) -> impl IntoResponse {
    match load_policy(&state.lambda, &fn_name).await {
        Ok(Some(p)) => {
            let policy_str = serde_json::to_string(&p).unwrap_or_default();
            json_response(
                StatusCode::OK,
                serde_json::json!({
                    "Policy": policy_str,
                    "RevisionId": "1"
                }),
            )
            .into_response()
        }
        Ok(None) => not_found("Policy", &fn_name).into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn add_permission(
    State(state): State<Arc<AppState>>,
    Path(fn_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    // Verify function exists
    match load_function(&state.lambda, &fn_name).await {
        Ok(None) => return not_found("Function", &fn_name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    let mut policy = load_policy(&state.lambda, &fn_name)
        .await
        .unwrap_or_default()
        .unwrap_or_else(|| LambdaPolicy {
            version: "2012-10-17".to_string(),
            id: format!("default-{fn_name}"),
            statement: vec![],
        });

    let statement_id = body
        .get("StatementId")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    let stmt = serde_json::json!({
        "Sid": statement_id,
        "Effect": body.get("Effect").and_then(|v| v.as_str()).unwrap_or("Allow"),
        "Principal": body.get("Principal").cloned().unwrap_or(serde_json::Value::String("*".to_string())),
        "Action": body.get("Action").and_then(|v| v.as_str()).unwrap_or("lambda:InvokeFunction"),
        "Resource": function_arn(&fn_name),
        "Condition": body.get("Condition").cloned().unwrap_or(serde_json::json!({}))
    });

    policy.statement.push(stmt.clone());

    if let Err(e) = save_policy(&state.lambda, &fn_name, &policy).await {
        return internal_error(&e.to_string()).into_response();
    }

    let stmt_str = serde_json::to_string(&stmt).unwrap_or_default();
    json_response(
        StatusCode::CREATED,
        serde_json::json!({"Statement": stmt_str}),
    )
    .into_response()
}

async fn remove_permission(
    State(state): State<Arc<AppState>>,
    Path((fn_name, statement_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut policy = match load_policy(&state.lambda, &fn_name).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found("Policy", &fn_name).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    let before = policy.statement.len();
    policy.statement.retain(|s| {
        s.get("Sid").and_then(|v| v.as_str()) != Some(&statement_id)
    });

    if policy.statement.len() == before {
        return not_found("Statement", &statement_id).into_response();
    }

    if let Err(e) = save_policy(&state.lambda, &fn_name, &policy).await {
        return internal_error(&e.to_string()).into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

// ── Event source mapping handlers ────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct EsmQueryParams {
    #[serde(rename = "FunctionName")]
    function_name: Option<String>,
}

async fn create_event_source_mapping(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let event_source_arn = match body.get("EventSourceArn").and_then(|v| v.as_str()) {
        Some(a) => a.to_string(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"Type": "InvalidParameterValueException", "message": "EventSourceArn is required"}),
            )
            .into_response()
        }
    };

    let function_name_raw = match body.get("FunctionName").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"Type": "InvalidParameterValueException", "message": "FunctionName is required"}),
            )
            .into_response()
        }
    };

    let function_name = resolve_function_name(&function_name_raw);
    let fn_arn = if function_name_raw.starts_with("arn:") {
        function_name_raw.clone()
    } else {
        function_arn(&function_name)
    };

    // Default batch size depends on source type
    let default_batch_size: i64 = if event_source_arn.contains(":sqs:") { 10 } else { 100 };
    let batch_size: i64 = body
        .get("BatchSize")
        .and_then(|v| v.as_i64())
        .unwrap_or(default_batch_size);

    let starting_position = body
        .get("StartingPosition")
        .and_then(|v| v.as_str())
        .unwrap_or("TRIM_HORIZON")
        .to_string();

    let enabled = body
        .get("Enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let maximum_retry_attempts: i64 = body
        .get("MaximumRetryAttempts")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    let bisect_batch_on_function_error = body
        .get("BisectBatchOnFunctionError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let uuid = Uuid::new_v4().to_string();
    let esm = StoredEventSourceMapping {
        uuid: uuid.clone(),
        event_source_arn,
        function_arn: fn_arn,
        function_name,
        batch_size,
        starting_position,
        state: if enabled { "Enabled".to_string() } else { "Disabled".to_string() },
        last_modified: now_unix_f64(),
        maximum_retry_attempts,
        bisect_batch_on_function_error,
    };

    if let Err(e) = save_esm(&state.lambda, &esm).await {
        return internal_error(&e.to_string()).into_response();
    }

    if enabled {
        spawn_esm_task(&state, &esm);
    }

    json_response(StatusCode::ACCEPTED, esm_to_json(&esm)).into_response()
}

async fn get_event_source_mapping(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    match load_esm(&state.lambda, &uuid).await {
        Ok(Some(esm)) => json_response(StatusCode::OK, esm_to_json(&esm)).into_response(),
        Ok(None) => not_found("EventSourceMapping", &uuid).into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn list_event_source_mappings(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EsmQueryParams>,
) -> impl IntoResponse {
    match list_esms(&state.lambda).await {
        Ok(esms) => {
            let filtered: Vec<_> = esms
                .iter()
                .filter(|e| {
                    if let Some(ref fn_name) = params.function_name {
                        &e.function_name == fn_name || &e.function_arn == fn_name
                    } else {
                        true
                    }
                })
                .map(esm_to_json)
                .collect();
            json_response(
                StatusCode::OK,
                serde_json::json!({"EventSourceMappings": filtered}),
            )
            .into_response()
        }
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

async fn update_event_source_mapping(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut esm = match load_esm(&state.lambda, &uuid).await {
        Ok(Some(e)) => e,
        Ok(None) => return not_found("EventSourceMapping", &uuid).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    let old_state = esm.state.clone();

    if let Some(v) = body.get("BatchSize").and_then(|v| v.as_i64()) {
        esm.batch_size = v;
    }
    if let Some(v) = body.get("MaximumRetryAttempts").and_then(|v| v.as_i64()) {
        esm.maximum_retry_attempts = v;
    }
    if let Some(enabled) = body.get("Enabled").and_then(|v| v.as_bool()) {
        esm.state = if enabled { "Enabled".to_string() } else { "Disabled".to_string() };
    }
    esm.last_modified = now_unix_f64();

    if let Err(e) = save_esm(&state.lambda, &esm).await {
        return internal_error(&e.to_string()).into_response();
    }

    // Handle state transitions
    if old_state != esm.state {
        if esm.state == "Disabled" {
            // Abort existing task
            let mut tasks = state.esm_tasks.lock().await;
            if let Some(handle) = tasks.remove(&uuid) {
                handle.abort();
            }
        } else if esm.state == "Enabled" {
            spawn_esm_task(&state, &esm);
        }
    }

    json_response(StatusCode::OK, esm_to_json(&esm)).into_response()
}

async fn delete_event_source_mapping(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let esm = match load_esm(&state.lambda, &uuid).await {
        Ok(Some(e)) => e,
        Ok(None) => return not_found("EventSourceMapping", &uuid).into_response(),
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    // Abort task if running
    {
        let mut tasks = state.esm_tasks.lock().await;
        if let Some(handle) = tasks.remove(&uuid) {
            handle.abort();
        }
    }

    let _ = state.lambda.delete(&esm_path(&uuid)).await;

    json_response(StatusCode::ACCEPTED, esm_to_json(&esm)).into_response()
}

// ── Layer handlers (stubs) ───────────────────────────────────────────────────

async fn list_layers(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    json_response(StatusCode::OK, serde_json::json!({"Layers": []}))
}

async fn list_layer_versions(
    State(_): State<Arc<AppState>>,
    Path(_layer_name): Path<String>,
) -> impl IntoResponse {
    json_response(StatusCode::OK, serde_json::json!({"LayerVersions": []}))
}

async fn publish_layer_version(
    State(_): State<Arc<AppState>>,
    Path(_layer_name): Path<String>,
) -> impl IntoResponse {
    json_response(
        StatusCode::NOT_IMPLEMENTED,
        serde_json::json!({"Type": "NotImplementedException", "message": "Layer publishing not supported"}),
    )
}

async fn get_layer_version(
    State(_): State<Arc<AppState>>,
    Path((_layer_name, _version)): Path<(String, u64)>,
) -> impl IntoResponse {
    json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"Type": "ResourceNotFoundException", "message": "Layer version not found"}),
    )
}

async fn delete_layer_version(
    State(_): State<Arc<AppState>>,
    Path((_layer_name, _version)): Path<(String, u64)>,
) -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

// ── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Functions
        .route("/2015-03-31/functions", post(create_function))
        .route("/2015-03-31/functions", get(list_functions_handler))
        .route("/2015-03-31/functions/{name}", get(get_function_handler))
        .route("/2015-03-31/functions/{name}", delete(delete_function_handler))
        .route("/2015-03-31/functions/{name}/code", put(update_function_code))
        .route(
            "/2015-03-31/functions/{name}/configuration",
            get(get_function_configuration),
        )
        .route(
            "/2015-03-31/functions/{name}/configuration",
            put(update_function_configuration),
        )
        .route(
            "/2015-03-31/functions/{name}/invocations",
            post(invoke_function),
        )
        // Aliases
        .route("/2015-03-31/functions/{name}/aliases", get(list_aliases_handler))
        .route("/2015-03-31/functions/{name}/aliases", post(create_alias))
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            get(get_alias),
        )
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            put(update_alias),
        )
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            delete(delete_alias),
        )
        // Resource-based policy
        .route("/2015-03-31/functions/{name}/policy", get(get_policy))
        .route(
            "/2015-03-31/functions/{name}/policy",
            post(add_permission),
        )
        .route(
            "/2015-03-31/functions/{name}/policy/{statement_id}",
            delete(remove_permission),
        )
        // Event source mappings
        .route(
            "/2015-03-31/event-source-mappings",
            get(list_event_source_mappings),
        )
        .route(
            "/2015-03-31/event-source-mappings",
            post(create_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            get(get_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            put(update_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            delete(delete_event_source_mapping),
        )
        // Layers
        .route("/2015-03-31/layers", get(list_layers))
        .route(
            "/2015-03-31/layers/{layer_name}/versions",
            get(list_layer_versions),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions",
            post(publish_layer_version),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions/{version}",
            get(get_layer_version),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions/{version}",
            delete(delete_layer_version),
        )
}
