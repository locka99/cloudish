
//! DynamoDB service emulator.
//!
//! Routing: all requests POST to `/dynamodb/`, dispatched by `X-Amz-Target` header.
//! Target format: `DynamoDB_20120810.<Operation>` or `DynamoDBStreams_20120810.<Operation>`

pub mod expressions;
pub mod store;
pub mod types;

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use base64::Engine;
use chrono::Utc;
use serde_json::{Value, json};

use crate::storage::Storage;
use crate::services::AppState;
use crate::services::dynamodb::expressions::{
    apply_projection, apply_update_expression, eval_condition, parse_expr_names, parse_expr_values,
    parse_key_condition,
};
use crate::services::dynamodb::store::{
    create_table_meta, current_stream_seq, delete_table_data, extract_key, item_key,
    list_all_items, list_tables, load_item, load_table_meta, save_item,
    save_table_meta, write_stream_record,
};
use crate::services::dynamodb::types::{AttributeValue, Item, TableMeta};


pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    // Spawn TTL sweep task
    tokio::spawn(ttl_sweep_task(state));
    // Keep /dynamodb/ for direct HTTP testing.
    // The top-level POST / dispatcher in services/mod.rs routes SDK traffic here.
    Router::new()
        .route("/dynamodb/", post(dispatch_inner))
}

/// Public dispatch function called from the top-level POST / handler.
pub(crate) async fn dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch_inner(State(state), request).await
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn ddb_error(code: StatusCode, error_type: &str, message: &str) -> (StatusCode, axum::Json<Value>) {
    let body = json!({
        "__type": format!("com.amazonaws.dynamodb.v20120810#{error_type}"),
        "message": message,
    });
    (code, axum::Json(body))
}

fn resource_not_found(msg: &str) -> (StatusCode, axum::Json<Value>) {
    ddb_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}

fn validation_error(msg: &str) -> (StatusCode, axum::Json<Value>) {
    ddb_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn condition_failed(msg: &str) -> (StatusCode, axum::Json<Value>) {
    ddb_error(StatusCode::BAD_REQUEST, "ConditionalCheckFailedException", msg)
}

fn resource_in_use(msg: &str) -> (StatusCode, axum::Json<Value>) {
    ddb_error(StatusCode::BAD_REQUEST, "ResourceInUseException", msg)
}

fn internal_error(msg: &str) -> (StatusCode, axum::Json<Value>) {
    ddb_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError", msg)
}

// ── Dispatch ──────────────────────────────────────────────────────────────────

async fn dispatch_inner(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split('.').last())
        .unwrap_or("")
        .to_string();

    tracing::debug!("DynamoDB operation={target}");

    // Parse body
    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"message": format!("Failed to read body: {e}")})),
            );
        }
    };

    let body: Value = if body_bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"message": format!("Invalid JSON: {e}")})),
                );
            }
        }
    };

    match target.as_str() {
        "CreateTable" => handle_create_table(&state, &body).await,
        "DeleteTable" => handle_delete_table(&state, &body).await,
        "DescribeTable" => handle_describe_table(&state, &body).await,
        "ListTables" => handle_list_tables(&state, &body).await,
        "UpdateTable" => handle_update_table(&state, &body).await,
        "PutItem" => handle_put_item(&state, &body).await,
        "GetItem" => handle_get_item(&state, &body).await,
        "DeleteItem" => handle_delete_item(&state, &body).await,
        "UpdateItem" => handle_update_item(&state, &body).await,
        "Query" => handle_query(&state, &body).await,
        "Scan" => handle_scan(&state, &body).await,
        "BatchGetItem" => handle_batch_get_item(&state, &body).await,
        "BatchWriteItem" => handle_batch_write_item(&state, &body).await,
        "TransactGetItems" => handle_transact_get_items(&state, &body).await,
        "TransactWriteItems" => handle_transact_write_items(&state, &body).await,
        "UpdateTimeToLive" => handle_update_ttl(&state, &body).await,
        "DescribeTimeToLive" => handle_describe_ttl(&state, &body).await,
        "DescribeStream" => handle_describe_stream(&state, &body).await,
        "GetShardIterator" => handle_get_shard_iterator(&state, &body).await,
        "GetRecords" => handle_get_records(&state, &body).await,
        "ListStreams" => handle_list_streams(&state, &body).await,
        other => {
            tracing::warn!("unknown DynamoDB operation: {other}");
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({
                    "__type": "com.amazonaws.dynamodb.v20120810#UnknownOperationException",
                    "message": format!("Unknown operation: {other}")
                })),
            )
        }
    }
}

// ── Table Operations ──────────────────────────────────────────────────────────

async fn handle_create_table(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n.to_string(),
        None => return validation_error("Missing TableName"),
    };

    // Check if table exists already
    match load_table_meta(&state.dynamodb, &table_name).await {
        Ok(Some(_)) => return resource_in_use(&format!("Table already exists: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
        Ok(None) => {}
    }

    let meta = match create_table_meta(body) {
        Ok(m) => m,
        Err(e) => return validation_error(&e.to_string()),
    };

    if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
        return internal_error(&e.to_string());
    }

    let table_desc = meta.to_describe_json();
    (StatusCode::OK, axum::Json(json!({"TableDescription": table_desc})))
}

async fn handle_delete_table(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    if let Err(e) = delete_table_data(&state.dynamodb, table_name).await {
        return internal_error(&e.to_string());
    }

    // Also delete streams
    let stream_prefix = format!("streams/{table_name}");
    if let Ok(keys) = state.dynamodb.list(&stream_prefix).await {
        for k in keys {
            let _ = state.dynamodb.delete(&k).await;
        }
    }
    let _ = state.dynamodb.delete(&format!("streams/{table_name}/_seq")).await;

    let table_desc = meta.to_describe_json();
    (StatusCode::OK, axum::Json(json!({"TableDescription": table_desc})))
}

async fn handle_describe_table(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(meta)) => {
            let table_desc = meta.to_describe_json();
            (StatusCode::OK, axum::Json(json!({"Table": table_desc})))
        }
        Ok(None) => resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => internal_error(&e.to_string()),
    }
}

async fn handle_list_tables(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let limit = body["Limit"].as_u64().unwrap_or(100) as usize;
    let exclusive_start = body["ExclusiveStartTableName"].as_str();

    match list_tables(&state.dynamodb).await {
        Ok(mut names) => {
            // Apply ExclusiveStartTableName pagination
            if let Some(start) = exclusive_start {
                if let Some(pos) = names.iter().position(|n| n == start) {
                    names = names[pos + 1..].to_vec();
                }
            }

            let last_evaluated = if names.len() > limit {
                let last = names[limit - 1].clone();
                names.truncate(limit);
                Some(last)
            } else {
                None
            };

            let mut resp = json!({"TableNames": names});
            if let Some(last) = last_evaluated {
                resp["LastEvaluatedTableName"] = json!(last);
            }
            (StatusCode::OK, axum::Json(resp))
        }
        Err(e) => internal_error(&e.to_string()),
    }
}

async fn handle_update_table(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    // Update billing mode
    if let Some(billing_mode) = body["BillingMode"].as_str() {
        meta.billing_mode = billing_mode.to_string();
    }

    // Update provisioned throughput
    if let Some(pt) = body.get("ProvisionedThroughput") {
        if let Some(rcu) = pt["ReadCapacityUnits"].as_u64() {
            meta.read_capacity_units = rcu;
        }
        if let Some(wcu) = pt["WriteCapacityUnits"].as_u64() {
            meta.write_capacity_units = wcu;
        }
    }

    // Update stream specification
    if let Some(ss) = body.get("StreamSpecification") {
        let enabled = ss["StreamEnabled"].as_bool().unwrap_or(false);
        meta.stream_enabled = enabled;
        if enabled {
            meta.stream_view_type = ss["StreamViewType"].as_str().map(str::to_string);
            let label = format!("{}", Utc::now().timestamp());
            meta.stream_arn = Some(store::stream_arn(table_name, &label));
            meta.stream_label = Some(label);
        } else {
            meta.stream_view_type = None;
            meta.stream_arn = None;
            meta.stream_label = None;
        }
    }

    if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
        return internal_error(&e.to_string());
    }

    let table_desc = meta.to_describe_json();
    (StatusCode::OK, axum::Json(json!({"TableDescription": table_desc})))
}

// ── Item Operations ───────────────────────────────────────────────────────────

async fn handle_put_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let item: Item = match serde_json::from_value(body["Item"].clone()) {
        Ok(i) => i,
        Err(e) => return validation_error(&format!("Invalid Item: {e}")),
    };

    // Extract key
    let (hash_val, range_val) = match extract_key(&item, &meta) {
        Some(k) => k,
        None => return validation_error("Item missing primary key attributes"),
    };

    let key = item_key(table_name, &hash_val, range_val.as_ref());

    // Load existing item for condition check and stream
    let old_item = match load_item(&state.dynamodb, &key).await {
        Ok(i) => i,
        Err(e) => return internal_error(&e.to_string()),
    };

    // Condition expression
    if let Some(cond_expr) = body["ConditionExpression"].as_str() {
        let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
        let expr_values = parse_expr_values(&body["ExpressionAttributeValues"]);
        let check_item = old_item.as_ref().cloned().unwrap_or_default();
        if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
            return condition_failed("The conditional request failed");
        }
    }

    let return_values = body["ReturnValues"].as_str().unwrap_or("NONE");
    let returned = match return_values {
        "ALL_OLD" => old_item.as_ref().map(|i| serde_json::to_value(i).unwrap_or(json!({}))),
        _ => None,
    };

    // Save item
    if let Err(e) = save_item(&state.dynamodb, &key, &item).await {
        return internal_error(&e.to_string());
    }

    // Update item count and size
    let size_bytes = serde_json::to_vec(&item).map(|v| v.len() as i64).unwrap_or(0);
    if old_item.is_none() {
        meta.item_count += 1;
    }
    meta.table_size_bytes += size_bytes;

    // Write stream record
    let event_name = if old_item.is_none() { "INSERT" } else { "MODIFY" };
    let mut keys_item: Item = HashMap::new();
    keys_item.insert(meta.hash_key().unwrap_or("").to_string(), hash_val.clone());
    if let Some(ref rk) = meta.range_key().map(str::to_string) {
        if let Some(ref rv) = range_val {
            keys_item.insert(rk.clone(), rv.clone());
        }
    }

    let _ = write_stream_record(
        &state.dynamodb,
        table_name,
        &meta,
        event_name,
        &keys_item,
        Some(&item),
        old_item.as_ref(),
    ).await;

    if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
        tracing::warn!("Failed to update table meta: {e}");
    }

    let mut resp = json!({});
    if let Some(rv) = returned {
        resp["Attributes"] = rv;
    }
    (StatusCode::OK, axum::Json(resp))
}

async fn handle_get_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let key_map: Item = match serde_json::from_value(body["Key"].clone()) {
        Ok(k) => k,
        Err(e) => return validation_error(&format!("Invalid Key: {e}")),
    };

    let hash_attr = match meta.hash_key() {
        Some(h) => h.to_string(),
        None => return validation_error("Table has no hash key"),
    };

    let hash_val = match key_map.get(&hash_attr) {
        Some(v) => v.clone(),
        None => return validation_error(&format!("Missing hash key attribute: {hash_attr}")),
    };

    let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
    let key = item_key(table_name, &hash_val, range_val.as_ref());

    match load_item(&state.dynamodb, &key).await {
        Ok(Some(mut item)) => {
            // Apply projection
            let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
            if let Some(proj) = body["ProjectionExpression"].as_str() {
                item = apply_projection(&item, proj, &expr_names);
            }
            (StatusCode::OK, axum::Json(json!({"Item": item})))
        }
        Ok(None) => (StatusCode::OK, axum::Json(json!({}))),
        Err(e) => internal_error(&e.to_string()),
    }
}

async fn handle_delete_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let key_map: Item = match serde_json::from_value(body["Key"].clone()) {
        Ok(k) => k,
        Err(e) => return validation_error(&format!("Invalid Key: {e}")),
    };

    let hash_attr = match meta.hash_key() {
        Some(h) => h.to_string(),
        None => return validation_error("Table has no hash key"),
    };

    let hash_val = match key_map.get(&hash_attr) {
        Some(v) => v.clone(),
        None => return validation_error(&format!("Missing hash key: {hash_attr}")),
    };

    let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
    let key = item_key(table_name, &hash_val, range_val.as_ref());

    let old_item = match load_item(&state.dynamodb, &key).await {
        Ok(i) => i,
        Err(e) => return internal_error(&e.to_string()),
    };

    // Condition expression
    if let Some(cond_expr) = body["ConditionExpression"].as_str() {
        let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
        let expr_values = parse_expr_values(&body["ExpressionAttributeValues"]);
        let check_item = old_item.as_ref().cloned().unwrap_or_default();
        if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
            return condition_failed("The conditional request failed");
        }
    }

    if let Some(ref _existing) = old_item {
        if let Err(e) = state.dynamodb.delete(&key).await {
            return internal_error(&e.to_string());
        }

        // Update meta
        meta.item_count -= 1;
        let size_bytes = serde_json::to_vec(_existing).map(|v| v.len() as i64).unwrap_or(0);
        meta.table_size_bytes -= size_bytes;

        // Stream record
        let mut keys_item: Item = HashMap::new();
        keys_item.insert(hash_attr.clone(), hash_val);
        if let Some(ref rk) = meta.range_key().map(str::to_string) {
            if let Some(ref rv) = range_val {
                keys_item.insert(rk.clone(), rv.clone());
            }
        }
        let _ = write_stream_record(
            &state.dynamodb,
            table_name,
            &meta,
            "REMOVE",
            &keys_item,
            None,
            Some(_existing),
        ).await;

        if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
            tracing::warn!("Failed to update table meta: {e}");
        }
    }

    let return_values = body["ReturnValues"].as_str().unwrap_or("NONE");
    let mut resp = json!({});
    if return_values == "ALL_OLD" {
        if let Some(old) = &old_item {
            resp["Attributes"] = serde_json::to_value(old).unwrap_or(json!({}));
        }
    }
    (StatusCode::OK, axum::Json(resp))
}

async fn handle_update_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let key_map: Item = match serde_json::from_value(body["Key"].clone()) {
        Ok(k) => k,
        Err(e) => return validation_error(&format!("Invalid Key: {e}")),
    };

    let hash_attr = match meta.hash_key() {
        Some(h) => h.to_string(),
        None => return validation_error("Table has no hash key"),
    };

    let hash_val = match key_map.get(&hash_attr) {
        Some(v) => v.clone(),
        None => return validation_error(&format!("Missing hash key: {hash_attr}")),
    };

    let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
    let key = item_key(table_name, &hash_val, range_val.as_ref());

    let old_item = match load_item(&state.dynamodb, &key).await {
        Ok(i) => i,
        Err(e) => return internal_error(&e.to_string()),
    };

    let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
    let expr_values = parse_expr_values(&body["ExpressionAttributeValues"]);

    // Condition expression
    if let Some(cond_expr) = body["ConditionExpression"].as_str() {
        let check_item = old_item.as_ref().cloned().unwrap_or_default();
        if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
            return condition_failed("The conditional request failed");
        }
    }

    // Start with existing item or key-only item
    let mut new_item = old_item.clone().unwrap_or_else(|| key_map.clone());

    // Apply UpdateExpression
    if let Some(update_expr) = body["UpdateExpression"].as_str() {
        if let Err(e) = apply_update_expression(&mut new_item, update_expr, &expr_names, &expr_values) {
            return validation_error(&format!("Invalid UpdateExpression: {e}"));
        }
    }

    let return_values = body["ReturnValues"].as_str().unwrap_or("NONE");

    // Save updated item
    if let Err(e) = save_item(&state.dynamodb, &key, &new_item).await {
        return internal_error(&e.to_string());
    }

    // Update meta counts
    if old_item.is_none() {
        meta.item_count += 1;
    }
    let size_bytes = serde_json::to_vec(&new_item).map(|v| v.len() as i64).unwrap_or(0);
    meta.table_size_bytes += size_bytes;

    // Stream record
    let event_name = if old_item.is_none() { "INSERT" } else { "MODIFY" };
    let mut keys_item: Item = HashMap::new();
    keys_item.insert(hash_attr.clone(), hash_val);
    if let Some(ref rk) = meta.range_key().map(str::to_string) {
        if let Some(ref rv) = range_val {
            keys_item.insert(rk.clone(), rv.clone());
        }
    }
    let _ = write_stream_record(
        &state.dynamodb,
        table_name,
        &meta,
        event_name,
        &keys_item,
        Some(&new_item),
        old_item.as_ref(),
    ).await;

    if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
        tracing::warn!("Failed to update table meta: {e}");
    }

    let mut resp = json!({});
    match return_values {
        "ALL_NEW" => {
            resp["Attributes"] = serde_json::to_value(&new_item).unwrap_or(json!({}));
        }
        "ALL_OLD" => {
            if let Some(old) = &old_item {
                resp["Attributes"] = serde_json::to_value(old).unwrap_or(json!({}));
            }
        }
        "UPDATED_NEW" => {
            // Return only updated attributes - for simplicity return the full new item
            resp["Attributes"] = serde_json::to_value(&new_item).unwrap_or(json!({}));
        }
        "UPDATED_OLD" => {
            if let Some(old) = &old_item {
                resp["Attributes"] = serde_json::to_value(old).unwrap_or(json!({}));
            }
        }
        _ => {}
    }
    (StatusCode::OK, axum::Json(resp))
}

// ── Query & Scan ──────────────────────────────────────────────────────────────

async fn handle_query(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
    let expr_values = parse_expr_values(&body["ExpressionAttributeValues"]);
    let limit = body["Limit"].as_u64().map(|l| l as usize);
    let scan_index_forward = body["ScanIndexForward"].as_bool().unwrap_or(true);
    let exclusive_start_key: Option<Item> = body
        .get("ExclusiveStartKey")
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    // Parse KeyConditionExpression
    let key_cond_expr = match body["KeyConditionExpression"].as_str() {
        Some(e) => e,
        None => return validation_error("Missing KeyConditionExpression"),
    };

    let key_cond = match parse_key_condition(key_cond_expr, &expr_names, &expr_values) {
        Some(kc) => kc,
        None => return validation_error("Cannot parse KeyConditionExpression"),
    };

    // Load all items and filter by PK
    let all_items = match list_all_items(&state.dynamodb, table_name).await {
        Ok(items) => items,
        Err(e) => return internal_error(&e.to_string()),
    };

    let hash_attr = meta.hash_key().unwrap_or("").to_string();
    let range_attr = meta.range_key().map(str::to_string);

    // Filter by PK
    let mut filtered: Vec<Item> = all_items
        .into_iter()
        .filter(|item| {
            item.get(&hash_attr)
                .map(|v| v == &key_cond.pk_val)
                .unwrap_or(false)
        })
        .collect();

    // Filter by SK condition
    if let (Some(sk_cond), Some(rk)) = (&key_cond.sk_condition, &range_attr) {
        filtered.retain(|item| {
            item.get(rk)
                .map(|sk| sk_cond.matches(sk))
                .unwrap_or(false)
        });
    }

    // Sort by SK
    if let Some(ref rk) = range_attr {
        filtered.sort_by(|a, b| {
            let av = a.get(rk);
            let bv = b.get(rk);
            match (av, bv) {
                (Some(av), Some(bv)) => {
                    expressions::compare_sort_keys(av, bv)
                }
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        if !scan_index_forward {
            filtered.reverse();
        }
    }

    // Apply FilterExpression
    if let Some(filter_expr) = body["FilterExpression"].as_str() {
        filtered.retain(|item| eval_condition(filter_expr, item, &expr_names, &expr_values));
    }

    // Pagination
    let (items, last_evaluated_key) = paginate_items(filtered, exclusive_start_key.as_ref(), limit, &meta);

    // Apply projection
    let expr_names2 = parse_expr_names(&body["ExpressionAttributeNames"]);
    let items: Vec<Value> = items
        .into_iter()
        .map(|mut item| {
            if let Some(proj) = body["ProjectionExpression"].as_str() {
                item = apply_projection(&item, proj, &expr_names2);
            }
            serde_json::to_value(item).unwrap_or(json!({}))
        })
        .collect();

    let count = items.len();
    let mut resp = json!({
        "Items": items,
        "Count": count,
        "ScannedCount": count,
    });

    if let Some(lek) = last_evaluated_key {
        resp["LastEvaluatedKey"] = serde_json::to_value(lek).unwrap_or(json!({}));
    }

    (StatusCode::OK, axum::Json(resp))
}

async fn handle_scan(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let expr_names = parse_expr_names(&body["ExpressionAttributeNames"]);
    let expr_values = parse_expr_values(&body["ExpressionAttributeValues"]);
    let limit = body["Limit"].as_u64().map(|l| l as usize);
    let exclusive_start_key: Option<Item> = body
        .get("ExclusiveStartKey")
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let all_items = match list_all_items(&state.dynamodb, table_name).await {
        Ok(items) => items,
        Err(e) => return internal_error(&e.to_string()),
    };

    let scanned_count = all_items.len();

    // Apply FilterExpression
    let mut filtered: Vec<Item> = all_items;
    if let Some(filter_expr) = body["FilterExpression"].as_str() {
        filtered.retain(|item| eval_condition(filter_expr, item, &expr_names, &expr_values));
    }

    // Pagination
    let (items, last_evaluated_key) = paginate_items(filtered, exclusive_start_key.as_ref(), limit, &meta);

    // Apply projection
    let items: Vec<Value> = items
        .into_iter()
        .map(|mut item| {
            if let Some(proj) = body["ProjectionExpression"].as_str() {
                item = apply_projection(&item, proj, &expr_names);
            }
            serde_json::to_value(item).unwrap_or(json!({}))
        })
        .collect();

    let count = items.len();
    let mut resp = json!({
        "Items": items,
        "Count": count,
        "ScannedCount": scanned_count,
    });

    if let Some(lek) = last_evaluated_key {
        resp["LastEvaluatedKey"] = serde_json::to_value(lek).unwrap_or(json!({}));
    }

    (StatusCode::OK, axum::Json(resp))
}

fn paginate_items(
    items: Vec<Item>,
    exclusive_start_key: Option<&Item>,
    limit: Option<usize>,
    meta: &TableMeta,
) -> (Vec<Item>, Option<Item>) {
    let hash_attr = meta.hash_key().unwrap_or("").to_string();
    let range_attr = meta.range_key().map(str::to_string);

    // Find start position
    let start_idx = if let Some(esk) = exclusive_start_key {
        let esk_hash = esk.get(&hash_attr);
        let esk_range = range_attr.as_deref().and_then(|rk| esk.get(rk));

        items
            .iter()
            .position(|item| {
                let matches_hash = item.get(&hash_attr) == esk_hash;
                let matches_range = range_attr
                    .as_deref()
                    .map(|rk| item.get(rk) == esk_range)
                    .unwrap_or(true);
                matches_hash && matches_range
            })
            .map(|p| p + 1)
            .unwrap_or(0)
    } else {
        0
    };

    let remaining: Vec<Item> = items.into_iter().skip(start_idx).collect();
    let effective_limit = limit.unwrap_or(usize::MAX);

    if remaining.len() > effective_limit {
        let page: Vec<Item> = remaining[..effective_limit].to_vec();
        let last = page.last().cloned().map(|item| {
            let mut key_item = HashMap::new();
            if let Some(v) = item.get(&hash_attr) {
                key_item.insert(hash_attr.clone(), v.clone());
            }
            if let Some(ref rk) = range_attr {
                if let Some(v) = item.get(rk) {
                    key_item.insert(rk.clone(), v.clone());
                }
            }
            key_item
        });
        (page, last)
    } else {
        (remaining, None)
    }
}

// ── Batch Operations ──────────────────────────────────────────────────────────

async fn handle_batch_get_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let request_items = match body["RequestItems"].as_object() {
        Some(r) => r,
        None => return validation_error("Missing RequestItems"),
    };

    let mut responses: serde_json::Map<String, Value> = serde_json::Map::new();
    let unprocessed_keys: serde_json::Map<String, Value> = serde_json::Map::new();

    for (table_name, table_request) in request_items {
        let meta = match load_table_meta(&state.dynamodb, table_name).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                return resource_not_found(&format!("Table not found: {table_name}"));
            }
            Err(e) => return internal_error(&e.to_string()),
        };

        let keys_val = match table_request["Keys"].as_array() {
            Some(k) => k,
            None => continue,
        };

        let projection = table_request["ProjectionExpression"].as_str();
        let expr_names = parse_expr_names(&table_request["ExpressionAttributeNames"]);

        let mut items = Vec::new();
        for key_val in keys_val {
            let key_map: Item = match serde_json::from_value(key_val.clone()) {
                Ok(k) => k,
                Err(_) => continue,
            };

            let hash_attr = match meta.hash_key() {
                Some(h) => h.to_string(),
                None => continue,
            };
            let hash_val = match key_map.get(&hash_attr) {
                Some(v) => v.clone(),
                None => continue,
            };
            let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
            let key = item_key(table_name, &hash_val, range_val.as_ref());

            if let Ok(Some(mut item)) = load_item(&state.dynamodb, &key).await {
                if let Some(proj) = projection {
                    item = apply_projection(&item, proj, &expr_names);
                }
                items.push(serde_json::to_value(item).unwrap_or(json!({})));
            }
        }

        responses.insert(table_name.clone(), json!(items));
    }

    (
        StatusCode::OK,
        axum::Json(json!({
            "Responses": responses,
            "UnprocessedKeys": unprocessed_keys,
        })),
    )
}

async fn handle_batch_write_item(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let request_items = match body["RequestItems"].as_object() {
        Some(r) => r,
        None => return validation_error("Missing RequestItems"),
    };

    let unprocessed_items: serde_json::Map<String, Value> = serde_json::Map::new();

    for (table_name, requests) in request_items {
        let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                return resource_not_found(&format!("Table not found: {table_name}"));
            }
            Err(e) => return internal_error(&e.to_string()),
        };

        let requests = match requests.as_array() {
            Some(r) => r,
            None => continue,
        };

        for request in requests {
            if let Some(put_req) = request.get("PutRequest") {
                let item: Item = match serde_json::from_value(put_req["Item"].clone()) {
                    Ok(i) => i,
                    Err(_) => continue,
                };

                let (hash_val, range_val) = match extract_key(&item, &meta) {
                    Some(k) => k,
                    None => continue,
                };

                let key = item_key(table_name, &hash_val, range_val.as_ref());
                let old_item = load_item(&state.dynamodb, &key).await.ok().flatten();

                if let Err(e) = save_item(&state.dynamodb, &key, &item).await {
                    tracing::warn!("BatchWrite PutRequest failed: {e}");
                    continue;
                }

                let event_name = if old_item.is_none() { "INSERT" } else { "MODIFY" };
                let mut keys_item: Item = HashMap::new();
                if let Some(hk) = meta.hash_key() {
                    keys_item.insert(hk.to_string(), hash_val);
                }
                if let Some(rk) = meta.range_key() {
                    if let Some(rv) = range_val {
                        keys_item.insert(rk.to_string(), rv);
                    }
                }
                let _ = write_stream_record(&state.dynamodb, table_name, &meta, event_name, &keys_item, Some(&item), old_item.as_ref()).await;

                if old_item.is_none() {
                    meta.item_count += 1;
                }
            } else if let Some(del_req) = request.get("DeleteRequest") {
                let key_map: Item = match serde_json::from_value(del_req["Key"].clone()) {
                    Ok(k) => k,
                    Err(_) => continue,
                };

                let hash_attr = match meta.hash_key() {
                    Some(h) => h.to_string(),
                    None => continue,
                };
                let hash_val = match key_map.get(&hash_attr) {
                    Some(v) => v.clone(),
                    None => continue,
                };
                let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
                let key = item_key(table_name, &hash_val, range_val.as_ref());

                let old_item = load_item(&state.dynamodb, &key).await.ok().flatten();
                if old_item.is_some() {
                    let _ = state.dynamodb.delete(&key).await;

                    let mut keys_item: Item = HashMap::new();
                    keys_item.insert(hash_attr, hash_val);
                    if let Some(rk) = meta.range_key() {
                        if let Some(rv) = range_val {
                            keys_item.insert(rk.to_string(), rv);
                        }
                    }
                    let _ = write_stream_record(&state.dynamodb, table_name, &meta, "REMOVE", &keys_item, None, old_item.as_ref()).await;
                    meta.item_count -= 1;
                }
            }
        }

        if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
            tracing::warn!("Failed to update table meta: {e}");
        }
    }

    (
        StatusCode::OK,
        axum::Json(json!({
            "UnprocessedItems": unprocessed_items,
        })),
    )
}

// ── Transactions ──────────────────────────────────────────────────────────────

async fn handle_transact_get_items(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let transact_items = match body["TransactItems"].as_array() {
        Some(t) => t,
        None => return validation_error("Missing TransactItems"),
    };

    let mut responses = Vec::new();

    for tx_item in transact_items {
        if let Some(get) = tx_item.get("Get") {
            let table_name = match get["TableName"].as_str() {
                Some(n) => n,
                None => return validation_error("Missing TableName in Get"),
            };

            let meta = match load_table_meta(&state.dynamodb, table_name).await {
                Ok(Some(m)) => m,
                Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
                Err(e) => return internal_error(&e.to_string()),
            };

            let key_map: Item = match serde_json::from_value(get["Key"].clone()) {
                Ok(k) => k,
                Err(e) => return validation_error(&format!("Invalid Key: {e}")),
            };

            let hash_attr = match meta.hash_key() {
                Some(h) => h.to_string(),
                None => return validation_error("Table has no hash key"),
            };
            let hash_val = match key_map.get(&hash_attr) {
                Some(v) => v.clone(),
                None => return validation_error(&format!("Missing hash key: {hash_attr}")),
            };
            let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
            let key = item_key(table_name, &hash_val, range_val.as_ref());

            match load_item(&state.dynamodb, &key).await {
                Ok(Some(mut item)) => {
                    let expr_names = parse_expr_names(&get["ExpressionAttributeNames"]);
                    if let Some(proj) = get["ProjectionExpression"].as_str() {
                        item = apply_projection(&item, proj, &expr_names);
                    }
                    responses.push(json!({"Item": item}));
                }
                Ok(None) => {
                    responses.push(json!({}));
                }
                Err(e) => return internal_error(&e.to_string()),
            }
        }
    }

    (StatusCode::OK, axum::Json(json!({"Responses": responses})))
}

async fn handle_transact_write_items(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let transact_items = match body["TransactItems"].as_array() {
        Some(t) => t,
        None => return validation_error("Missing TransactItems"),
    };

    // Pre-validate all conditions first (for atomicity)
    for tx_item in transact_items {
        if let Some(cw) = tx_item.get("ConditionCheck") {
            let table_name = cw["TableName"].as_str().unwrap_or("");
            let meta = match load_table_meta(&state.dynamodb, table_name).await {
                Ok(Some(m)) => m,
                Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
                Err(e) => return internal_error(&e.to_string()),
            };

            let key_map: Item = serde_json::from_value(cw["Key"].clone()).unwrap_or_default();
            let hash_attr = meta.hash_key().unwrap_or("").to_string();
            let hash_val = match key_map.get(&hash_attr) {
                Some(v) => v.clone(),
                None => return validation_error("Missing hash key"),
            };
            let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
            let key = item_key(table_name, &hash_val, range_val.as_ref());

            let existing = load_item(&state.dynamodb, &key).await.ok().flatten();
            if let Some(cond_expr) = cw["ConditionExpression"].as_str() {
                let expr_names = parse_expr_names(&cw["ExpressionAttributeNames"]);
                let expr_values = parse_expr_values(&cw["ExpressionAttributeValues"]);
                let check_item = existing.unwrap_or_default();
                if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
                    return condition_failed("The conditional request failed");
                }
            }
        }
    }

    // Execute all writes
    for tx_item in transact_items {
        if let Some(put) = tx_item.get("Put") {
            let table_name = put["TableName"].as_str().unwrap_or("");
            let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
                Ok(Some(m)) => m,
                _ => continue,
            };

            let item: Item = match serde_json::from_value(put["Item"].clone()) {
                Ok(i) => i,
                Err(_) => continue,
            };

            // Condition check
            let (hash_val, range_val) = match extract_key(&item, &meta) {
                Some(k) => k,
                None => continue,
            };
            let key = item_key(table_name, &hash_val, range_val.as_ref());
            let old_item = load_item(&state.dynamodb, &key).await.ok().flatten();

            if let Some(cond_expr) = put["ConditionExpression"].as_str() {
                let expr_names = parse_expr_names(&put["ExpressionAttributeNames"]);
                let expr_values = parse_expr_values(&put["ExpressionAttributeValues"]);
                let check_item = old_item.as_ref().cloned().unwrap_or_default();
                if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
                    return condition_failed("The conditional request failed");
                }
            }

            let event_name = if old_item.is_none() { "INSERT" } else { "MODIFY" };
            let _ = save_item(&state.dynamodb, &key, &item).await;

            let mut keys_item: Item = HashMap::new();
            if let Some(hk) = meta.hash_key() {
                keys_item.insert(hk.to_string(), hash_val);
            }
            if let Some(rk) = meta.range_key() {
                if let Some(rv) = range_val {
                    keys_item.insert(rk.to_string(), rv);
                }
            }
            let _ = write_stream_record(&state.dynamodb, table_name, &meta, event_name, &keys_item, Some(&item), old_item.as_ref()).await;

            if old_item.is_none() {
                meta.item_count += 1;
            }
            let _ = save_table_meta(&state.dynamodb, &meta).await;
        } else if let Some(del) = tx_item.get("Delete") {
            let table_name = del["TableName"].as_str().unwrap_or("");
            let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
                Ok(Some(m)) => m,
                _ => continue,
            };

            let key_map: Item = serde_json::from_value(del["Key"].clone()).unwrap_or_default();
            let hash_attr = meta.hash_key().unwrap_or("").to_string();
            let hash_val = match key_map.get(&hash_attr) {
                Some(v) => v.clone(),
                None => continue,
            };
            let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
            let key = item_key(table_name, &hash_val, range_val.as_ref());

            let old_item = load_item(&state.dynamodb, &key).await.ok().flatten();

            if let Some(cond_expr) = del["ConditionExpression"].as_str() {
                let expr_names = parse_expr_names(&del["ExpressionAttributeNames"]);
                let expr_values = parse_expr_values(&del["ExpressionAttributeValues"]);
                let check_item = old_item.as_ref().cloned().unwrap_or_default();
                if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
                    return condition_failed("The conditional request failed");
                }
            }

            if old_item.is_some() {
                let _ = state.dynamodb.delete(&key).await;

                let mut keys_item: Item = HashMap::new();
                keys_item.insert(hash_attr, hash_val);
                if let Some(rk) = meta.range_key() {
                    if let Some(rv) = range_val {
                        keys_item.insert(rk.to_string(), rv);
                    }
                }
                let _ = write_stream_record(&state.dynamodb, table_name, &meta, "REMOVE", &keys_item, None, old_item.as_ref()).await;
                meta.item_count -= 1;
                let _ = save_table_meta(&state.dynamodb, &meta).await;
            }
        } else if let Some(update) = tx_item.get("Update") {
            let table_name = update["TableName"].as_str().unwrap_or("");
            let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
                Ok(Some(m)) => m,
                _ => continue,
            };

            let key_map: Item = serde_json::from_value(update["Key"].clone()).unwrap_or_default();
            let hash_attr = meta.hash_key().unwrap_or("").to_string();
            let hash_val = match key_map.get(&hash_attr) {
                Some(v) => v.clone(),
                None => continue,
            };
            let range_val = meta.range_key().and_then(|rk| key_map.get(rk)).cloned();
            let key = item_key(table_name, &hash_val, range_val.as_ref());

            let old_item = load_item(&state.dynamodb, &key).await.ok().flatten();

            if let Some(cond_expr) = update["ConditionExpression"].as_str() {
                let expr_names = parse_expr_names(&update["ExpressionAttributeNames"]);
                let expr_values = parse_expr_values(&update["ExpressionAttributeValues"]);
                let check_item = old_item.as_ref().cloned().unwrap_or_default();
                if !eval_condition(cond_expr, &check_item, &expr_names, &expr_values) {
                    return condition_failed("The conditional request failed");
                }
            }

            let mut new_item = old_item.clone().unwrap_or_else(|| key_map.clone());
            if let Some(update_expr) = update["UpdateExpression"].as_str() {
                let expr_names = parse_expr_names(&update["ExpressionAttributeNames"]);
                let expr_values = parse_expr_values(&update["ExpressionAttributeValues"]);
                let _ = apply_update_expression(&mut new_item, update_expr, &expr_names, &expr_values);
            }

            let event_name = if old_item.is_none() { "INSERT" } else { "MODIFY" };
            let _ = save_item(&state.dynamodb, &key, &new_item).await;

            let mut keys_item: Item = HashMap::new();
            keys_item.insert(hash_attr, hash_val);
            if let Some(rk) = meta.range_key() {
                if let Some(rv) = range_val {
                    keys_item.insert(rk.to_string(), rv);
                }
            }
            let _ = write_stream_record(&state.dynamodb, table_name, &meta, event_name, &keys_item, Some(&new_item), old_item.as_ref()).await;

            if old_item.is_none() {
                meta.item_count += 1;
            }
            let _ = save_table_meta(&state.dynamodb, &meta).await;
        }
    }

    (StatusCode::OK, axum::Json(json!({})))
}

// ── TTL ───────────────────────────────────────────────────────────────────────

async fn handle_update_ttl(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let mut meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    if let Some(ttl_spec) = body.get("TimeToLiveSpecification") {
        let enabled = ttl_spec["Enabled"].as_bool().unwrap_or(false);
        let attr = ttl_spec["AttributeName"].as_str().map(str::to_string);
        meta.ttl_enabled = enabled;
        meta.ttl_attribute = if enabled { attr } else { None };
    }

    if let Err(e) = save_table_meta(&state.dynamodb, &meta).await {
        return internal_error(&e.to_string());
    }

    let ttl_spec = json!({
        "TimeToLiveSpecification": {
            "AttributeName": meta.ttl_attribute.as_deref().unwrap_or(""),
            "Enabled": meta.ttl_enabled,
        }
    });
    (StatusCode::OK, axum::Json(ttl_spec))
}

async fn handle_describe_ttl(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_name = match body["TableName"].as_str() {
        Some(n) => n,
        None => return validation_error("Missing TableName"),
    };

    let meta = match load_table_meta(&state.dynamodb, table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Table not found: {table_name}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let status = if meta.ttl_enabled { "ENABLED" } else { "DISABLED" };
    let resp = json!({
        "TimeToLiveDescription": {
            "TimeToLiveStatus": status,
            "AttributeName": meta.ttl_attribute.as_deref().unwrap_or(""),
        }
    });
    (StatusCode::OK, axum::Json(resp))
}

// ── Streams ───────────────────────────────────────────────────────────────────

async fn handle_describe_stream(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let stream_arn_val = match body["StreamArn"].as_str() {
        Some(a) => a,
        None => return validation_error("Missing StreamArn"),
    };

    // Parse table name from ARN: arn:aws:dynamodb:...:table/{name}/stream/...
    let table_name = parse_table_name_from_stream_arn(stream_arn_val)
        .unwrap_or_default();

    let meta = match load_table_meta(&state.dynamodb, &table_name).await {
        Ok(Some(m)) => m,
        Ok(None) => return resource_not_found(&format!("Stream not found: {stream_arn_val}")),
        Err(e) => return internal_error(&e.to_string()),
    };

    let current_seq = current_stream_seq(&state.dynamodb, &table_name).await.unwrap_or(0);

    let resp = json!({
        "StreamDescription": {
            "StreamArn": stream_arn_val,
            "StreamLabel": meta.stream_label.as_deref().unwrap_or(""),
            "StreamStatus": "ENABLED",
            "StreamViewType": meta.stream_view_type.as_deref().unwrap_or("NEW_AND_OLD_IMAGES"),
            "TableName": table_name,
            "Shards": [{
                "ShardId": format!("shardId-{table_name}-0"),
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "0",
                    "EndingSequenceNumber": current_seq.to_string(),
                }
            }],
        }
    });
    (StatusCode::OK, axum::Json(resp))
}

async fn handle_get_shard_iterator(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let stream_arn_val = match body["StreamArn"].as_str() {
        Some(a) => a,
        None => return validation_error("Missing StreamArn"),
    };

    let table_name = parse_table_name_from_stream_arn(stream_arn_val)
        .unwrap_or_default();

    let iterator_type = body["ShardIteratorType"].as_str().unwrap_or("TRIM_HORIZON");
    let seq_number = body["SequenceNumber"].as_str();

    let current_seq = current_stream_seq(&state.dynamodb, &table_name).await.unwrap_or(0);

    let start_seq = match iterator_type {
        "TRIM_HORIZON" => 0u64,
        "LATEST" => current_seq,
        "AT_SEQUENCE_NUMBER" => {
            seq_number.and_then(|s| s.parse().ok()).unwrap_or(0)
        }
        "AFTER_SEQUENCE_NUMBER" => {
            seq_number.and_then(|s| s.parse::<u64>().ok()).map(|s| s + 1).unwrap_or(0)
        }
        _ => 0,
    };

    // Encode iterator as base64url of "{table_name}:{start_seq}"
    let token_str = format!("{table_name}:{start_seq}");
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_str.as_bytes());

    (StatusCode::OK, axum::Json(json!({"ShardIterator": token})))
}

async fn handle_get_records(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let iterator = match body["ShardIterator"].as_str() {
        Some(i) => i,
        None => return validation_error("Missing ShardIterator"),
    };

    let limit = body["Limit"].as_u64().unwrap_or(1000) as usize;

    // Decode iterator
    let token_bytes = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(iterator) {
        Ok(b) => b,
        Err(_) => return validation_error("Invalid ShardIterator"),
    };

    let token_str = String::from_utf8_lossy(&token_bytes);
    let parts: Vec<&str> = token_str.splitn(2, ':').collect();
    if parts.len() != 2 {
        return validation_error("Invalid ShardIterator format");
    }

    let table_name = parts[0];
    let start_seq: u64 = parts[1].parse().unwrap_or(0);

    // List stream records after start_seq
    let prefix = format!("streams/{table_name}");
    let all_keys = match state.dynamodb.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return internal_error(&e.to_string()),
    };

    // Filter stream record files (not _seq)
    let mut record_keys: Vec<(u64, String)> = all_keys
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .filter_map(|k| {
            // Key format: streams/{table_name}/{seq:020}.json
            let filename = k.split('/').last()?;
            let seq_str = filename.strip_suffix(".json")?;
            let seq: u64 = seq_str.parse().ok()?;
            if seq >= start_seq {
                Some((seq, k))
            } else {
                None
            }
        })
        .collect();

    record_keys.sort_by_key(|(seq, _)| *seq);
    record_keys.truncate(limit);

    let mut records = Vec::new();
    let mut last_seq = start_seq;

    for (seq, key) in &record_keys {
        if let Ok(Some(bytes)) = state.dynamodb.get(key).await {
            if let Ok(record) = serde_json::from_slice::<Value>(&bytes) {
                records.push(record);
                last_seq = *seq + 1;
            }
        }
    }

    // Build next iterator
    let next_token_str = format!("{table_name}:{last_seq}");
    let next_iterator = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(next_token_str.as_bytes());

    (StatusCode::OK, axum::Json(json!({
        "Records": records,
        "NextShardIterator": next_iterator,
    })))
}

async fn handle_list_streams(state: &Arc<AppState>, body: &Value) -> (StatusCode, axum::Json<Value>) {
    let table_filter = body["TableName"].as_str();
    let limit = body["Limit"].as_u64().unwrap_or(100) as usize;

    let tables = match list_tables(&state.dynamodb).await {
        Ok(t) => t,
        Err(e) => return internal_error(&e.to_string()),
    };

    let mut streams = Vec::new();
    for table_name in &tables {
        if let Some(filter) = table_filter {
            if table_name != filter {
                continue;
            }
        }
        if let Ok(Some(meta)) = load_table_meta(&state.dynamodb, table_name).await {
            if meta.stream_enabled {
                streams.push(json!({
                    "StreamArn": meta.stream_arn.as_deref().unwrap_or(""),
                    "StreamLabel": meta.stream_label.as_deref().unwrap_or(""),
                    "TableName": table_name,
                }));
            }
        }
    }

    streams.truncate(limit);
    (StatusCode::OK, axum::Json(json!({"Streams": streams})))
}

// ── TTL sweep task ────────────────────────────────────────────────────────────

async fn ttl_sweep_task(state: Arc<AppState>) {
    let interval = std::time::Duration::from_secs(60);
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = do_ttl_sweep(&state).await {
            tracing::warn!("TTL sweep error: {e}");
        }
    }
}

async fn do_ttl_sweep(state: &Arc<AppState>) -> anyhow::Result<()> {
    let tables = list_tables(&state.dynamodb).await?;
    let now = Utc::now().timestamp() as u64;

    for table_name in &tables {
        let meta = match load_table_meta(&state.dynamodb, table_name).await? {
            Some(m) => m,
            None => continue,
        };

        if !meta.ttl_enabled {
            continue;
        }

        let ttl_attr = match &meta.ttl_attribute {
            Some(a) => a.clone(),
            None => continue,
        };

        let items = list_all_items(&state.dynamodb, table_name).await?;
        for item in &items {
            // Check TTL attribute
            let ttl_val = match item.get(&ttl_attr) {
                Some(AttributeValue::N(n)) => n.parse::<u64>().unwrap_or(u64::MAX),
                _ => continue,
            };

            if ttl_val <= now {
                // Delete expired item
                if let Some((hash_val, range_val)) = extract_key(item, &meta) {
                    let key = item_key(table_name, &hash_val, range_val.as_ref());
                    let _ = state.dynamodb.delete(&key).await;

                    // Emit stream record
                    let mut keys_item: Item = HashMap::new();
                    if let Some(hk) = meta.hash_key() {
                        keys_item.insert(hk.to_string(), hash_val);
                    }
                    if let Some(rk) = meta.range_key() {
                        if let Some(rv) = range_val {
                            keys_item.insert(rk.to_string(), rv);
                        }
                    }
                    let _ = write_stream_record(
                        &state.dynamodb,
                        table_name,
                        &meta,
                        "REMOVE",
                        &keys_item,
                        None,
                        Some(item),
                    ).await;

                    tracing::debug!("TTL expired item in table {table_name}");
                }
            }
        }
    }

    Ok(())
}

// ── Helper functions ──────────────────────────────────────────────────────────

fn parse_table_name_from_stream_arn(arn: &str) -> Option<String> {
    // arn:aws:dynamodb:eu-west-1:000000000000:table/{name}/stream/{label}
    let parts: Vec<&str> = arn.split('/').collect();
    if parts.len() >= 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}
