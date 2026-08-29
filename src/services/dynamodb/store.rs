
//! DynamoDB store: persistence layer on top of FileStorage.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use crate::storage::file::FileStorage;
use crate::storage::Storage;
use crate::services::dynamodb::types::{AttributeValue, Item, TableMeta};

const REGION: &str = "eu-west-1";
const ACCOUNT_ID: &str = "000000000000";

/// Encode an attribute value as a base64url key component.
pub fn encode_av_key(av: &AttributeValue) -> String {
    let json = serde_json::to_string(av).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.as_bytes())
}

/// Get the storage key for an item.
pub fn item_key(table_name: &str, hash_val: &AttributeValue, range_val: Option<&AttributeValue>) -> String {
    let pk_b64 = encode_av_key(hash_val);
    match range_val {
        Some(sk) => {
            let sk_b64 = encode_av_key(sk);
            format!("tables/{table_name}/items/{pk_b64}/{sk_b64}")
        }
        None => format!("tables/{table_name}/items/{pk_b64}"),
    }
}

/// Get the meta key for a table.
pub fn meta_key(table_name: &str) -> String {
    format!("tables/{table_name}/_meta.json")
}

/// Load table metadata.
pub async fn load_table_meta(storage: &Arc<FileStorage>, table_name: &str) -> Result<Option<TableMeta>> {
    let key = meta_key(table_name);
    match storage.get(&key).await? {
        Some(bytes) => {
            let meta: TableMeta = serde_json::from_slice(&bytes)?;
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

/// Save table metadata.
pub async fn save_table_meta(storage: &Arc<FileStorage>, meta: &TableMeta) -> Result<()> {
    let key = meta_key(&meta.table_name);
    let bytes = serde_json::to_vec(meta)?;
    storage.put(&key, bytes).await?;
    Ok(())
}

/// Delete table metadata and all its items.
pub async fn delete_table_data(storage: &Arc<FileStorage>, table_name: &str) -> Result<()> {
    // Delete meta
    let key = meta_key(table_name);
    storage.delete(&key).await?;

    // List and delete all items
    let item_prefix = format!("tables/{table_name}/items");
    let item_keys = storage.list(&item_prefix).await?;
    for k in item_keys {
        storage.delete(&k).await?;
    }

    Ok(())
}

/// Load a single item by its storage key.
pub async fn load_item(storage: &Arc<FileStorage>, key: &str) -> Result<Option<Item>> {
    match storage.get(key).await? {
        Some(bytes) => {
            let item: Item = serde_json::from_slice(&bytes)?;
            Ok(Some(item))
        }
        None => Ok(None),
    }
}

/// Save an item.
pub async fn save_item(storage: &Arc<FileStorage>, key: &str, item: &Item) -> Result<()> {
    let bytes = serde_json::to_vec(item)?;
    storage.put(key, bytes).await?;
    Ok(())
}

/// List all items in a table.
pub async fn list_all_items(storage: &Arc<FileStorage>, table_name: &str) -> Result<Vec<Item>> {
    let prefix = format!("tables/{table_name}/items");
    let keys = storage.list(&prefix).await?;
    let mut items = Vec::new();
    for key in keys {
        if let Some(item) = load_item(storage, &key).await? {
            items.push(item);
        }
    }
    Ok(items)
}

/// List all table names.
pub async fn list_tables(storage: &Arc<FileStorage>) -> Result<Vec<String>> {
    let keys = storage.list("tables").await?;
    let mut names = Vec::new();
    for key in keys {
        // Key looks like "tables/{name}/_meta.json"
        if key.ends_with("/_meta.json") {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 2 {
                names.push(parts[1].to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

/// Extract primary key values from an item.
pub fn extract_key(item: &Item, meta: &TableMeta) -> Option<(AttributeValue, Option<AttributeValue>)> {
    let hash_attr = meta.hash_key()?;
    let hash_val = item.get(hash_attr)?.clone();
    let range_val = meta.range_key().and_then(|rk| item.get(rk)).cloned();
    Some((hash_val, range_val))
}

/// Build the stream ARN for a table.
pub fn stream_arn(table_name: &str, label: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{table_name}/stream/{label}")
}

/// Build the table ARN.
pub fn table_arn(table_name: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{table_name}")
}

/// Get the next stream sequence number for a table (atomic increment via file).
pub async fn next_stream_seq(storage: &Arc<FileStorage>, table_name: &str) -> Result<u64> {
    let seq_key = format!("streams/{table_name}/_seq");
    let current = match storage.get(&seq_key).await? {
        Some(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            s.trim().parse::<u64>().unwrap_or(0)
        }
        None => 0,
    };
    let next = current + 1;
    storage.put(&seq_key, next.to_string().into_bytes()).await?;
    Ok(next)
}

/// Get the current stream sequence number.
pub async fn current_stream_seq(storage: &Arc<FileStorage>, table_name: &str) -> Result<u64> {
    let seq_key = format!("streams/{table_name}/_seq");
    match storage.get(&seq_key).await? {
        Some(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            Ok(s.trim().parse::<u64>().unwrap_or(0))
        }
        None => Ok(0),
    }
}

/// Write a stream record.
pub async fn write_stream_record(
    storage: &Arc<FileStorage>,
    table_name: &str,
    meta: &TableMeta,
    event_name: &str,
    keys: &Item,
    new_image: Option<&Item>,
    old_image: Option<&Item>,
) -> Result<()> {
    if !meta.stream_enabled {
        return Ok(());
    }

    let seq = next_stream_seq(storage, table_name).await?;
    let view_type = meta.stream_view_type.as_deref().unwrap_or("NEW_AND_OLD_IMAGES");

    let keys_val = serde_json::to_value(keys)?;
    let mut dynamodb_obj = serde_json::json!({
        "Keys": keys_val,
        "SequenceNumber": seq.to_string(),
        "SizeBytes": 100,
        "StreamViewType": view_type,
        "ApproximateCreationDateTime": Utc::now().timestamp() as f64,
    });

    let include_new = matches!(view_type, "NEW_IMAGE" | "NEW_AND_OLD_IMAGES" | "KEYS_ONLY");
    let include_old = matches!(view_type, "OLD_IMAGE" | "NEW_AND_OLD_IMAGES");

    if event_name != "REMOVE" && include_new {
        if let Some(ni) = new_image {
            dynamodb_obj["NewImage"] = serde_json::to_value(ni)?;
        }
    }
    if event_name != "INSERT" && include_old {
        if let Some(oi) = old_image {
            dynamodb_obj["OldImage"] = serde_json::to_value(oi)?;
        }
    }

    let stream_arn_val = meta.stream_arn.as_deref().unwrap_or("");
    let record = serde_json::json!({
        "eventID": Uuid::new_v4().to_string(),
        "eventName": event_name,
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": REGION,
        "dynamodb": dynamodb_obj,
        "eventSourceARN": stream_arn_val,
    });

    let record_key = format!("streams/{table_name}/{seq:020}.json");
    let bytes = serde_json::to_vec(&record)?;
    storage.put(&record_key, bytes).await?;

    Ok(())
}

/// Parse an item from a JSON Value (the request body attribute map).
pub fn parse_item_from_value(val: &Value) -> Result<Item> {
    let item: Item = serde_json::from_value(val.clone())?;
    Ok(item)
}

/// Parse expression attribute values from JSON.
pub fn parse_expr_values(val: &Value) -> HashMap<String, AttributeValue> {
    crate::services::dynamodb::expressions::parse_expr_values(val)
}

/// Parse expression attribute names from JSON.
pub fn parse_expr_names(val: &Value) -> HashMap<String, String> {
    crate::services::dynamodb::expressions::parse_expr_names(val)
}

/// Create a TableMeta from CreateTable request.
pub fn create_table_meta(body: &Value) -> Result<TableMeta> {
    let table_name = body["TableName"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing TableName"))?
        .to_string();

    let key_schema: Vec<crate::services::dynamodb::types::KeySchemaElement> =
        serde_json::from_value(body["KeySchema"].clone())?;

    let attribute_definitions: Vec<crate::services::dynamodb::types::AttributeDefinition> =
        serde_json::from_value(body["AttributeDefinitions"].clone())?;

    let billing_mode = body["BillingMode"]
        .as_str()
        .unwrap_or("PAY_PER_REQUEST")
        .to_string();

    let (rcu, wcu) = if let Some(pt) = body.get("ProvisionedThroughput") {
        (
            pt["ReadCapacityUnits"].as_u64().unwrap_or(0),
            pt["WriteCapacityUnits"].as_u64().unwrap_or(0),
        )
    } else {
        (0, 0)
    };

    let now = Utc::now().timestamp() as f64;
    let table_id = Uuid::new_v4().to_string();
    let arn = table_arn(&table_name);

    // Stream specification
    let (stream_enabled, stream_view_type, stream_arn_val, stream_label) =
        if let Some(ss) = body.get("StreamSpecification") {
            let enabled = ss["StreamEnabled"].as_bool().unwrap_or(false);
            let view_type = ss["StreamViewType"].as_str().map(str::to_string);
            if enabled {
                let label = format!("{}", Utc::now().timestamp());
                let arn = stream_arn(&table_name, &label);
                (enabled, view_type, Some(arn), Some(label))
            } else {
                (false, None, None, None)
            }
        } else {
            (false, None, None, None)
        };

    Ok(TableMeta {
        table_name,
        status: "ACTIVE".to_string(),
        creation_datetime: now,
        key_schema,
        attribute_definitions,
        billing_mode,
        read_capacity_units: rcu,
        write_capacity_units: wcu,
        table_arn: arn,
        table_id,
        item_count: 0,
        table_size_bytes: 0,
        stream_enabled,
        stream_view_type,
        stream_arn: stream_arn_val,
        stream_label,
        ttl_attribute: None,
        ttl_enabled: false,
    })
}
