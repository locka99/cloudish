
//! Integration tests for the DynamoDB service.
//!
//! Run with: `cargo test --test dynamodb -- --nocapture`

use aws_sdk_dynamodb::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType, TimeToLiveSpecification,
        WriteRequest, PutRequest, DeleteRequest, TransactWriteItem, Put,
        TransactGetItem, Get,
    },
};
use std::collections::HashMap;
use uuid::Uuid;

// ── Server startup ────────────────────────────────────────────────────────────

fn start_server_sync() -> u16 {
    use std::net::TcpListener as StdListener;

    let std_listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();

    let data_dir = format!("data/test_{port}");

    // Wipe any leftover data from previous test runs.
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

    // Wait for the server to accept connections (up to 1 s).
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

fn dynamodb_client(port: u16) -> Client {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    Client::from_conf(conf)
}

fn unique_table() -> String {
    format!("test-{}", Uuid::new_v4().simple())
}

// ── Drop-guard cleanup ────────────────────────────────────────────────────────

struct TableGuard {
    client: Client,
    table: String,
}

impl TableGuard {
    fn new(client: Client, table: impl Into<String>) -> Self {
        Self { client, table: table.into() }
    }
    fn table(&self) -> &str {
        &self.table
    }
}

impl Drop for TableGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let table = self.table.clone();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let _ = client.delete_table().table_name(&table).send().await;
                });
        });
    }
}

// ── Helper: create a simple hash-key table ────────────────────────────────────

async fn create_simple_table(client: &Client, table_name: &str) {
    client
        .create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

/// Helper: create a hash+sort key table.
async fn create_hash_sort_table(client: &Client, table_name: &str) {
    client
        .create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_list_delete_table() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    // Create table
    let resp = client
        .create_table()
        .table_name(&table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.table_description().unwrap().table_name().unwrap(),
        table
    );
    assert_eq!(
        resp.table_description().unwrap().table_status().unwrap().as_str(),
        "ACTIVE"
    );

    // Describe table
    let desc = client
        .describe_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.table().unwrap().table_name().unwrap(),
        table
    );

    // List tables - should include our table
    let list_resp = client
        .list_tables()
        .send()
        .await
        .unwrap();
    let table_names: Vec<&str> = list_resp.table_names().iter().map(|s| s.as_str()).collect();
    assert!(
        table_names.contains(&table.as_str()),
        "Table not in list: {:?}",
        table_names
    );

    // Delete table
    let del_resp = client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    assert_eq!(
        del_resp.table_description().unwrap().table_name().unwrap(),
        table
    );

    // Table should no longer appear in list
    let list_resp2 = client.list_tables().send().await.unwrap();
    let table_names2: Vec<&str> = list_resp2.table_names().iter().map(|s| s.as_str()).collect();
    assert!(!table_names2.contains(&table.as_str()));
}

#[tokio::test]
async fn test_put_get_delete_item() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // PutItem
    client
        .put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S("hello".into()))
        .item("value", AttributeValue::N("42".into()))
        .item("name", AttributeValue::S("world".into()))
        .send()
        .await
        .unwrap();

    // GetItem
    let get_resp = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("hello".into()))
        .send()
        .await
        .unwrap();

    let item = get_resp.item().unwrap();
    assert_eq!(item["pk"].as_s().unwrap(), "hello");
    assert_eq!(item["value"].as_n().unwrap(), "42");
    assert_eq!(item["name"].as_s().unwrap(), "world");

    // DeleteItem
    client
        .delete_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("hello".into()))
        .send()
        .await
        .unwrap();

    // GetItem after delete should return nothing
    let get_resp2 = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("hello".into()))
        .send()
        .await
        .unwrap();
    assert!(get_resp2.item().is_none());
}

#[tokio::test]
async fn test_update_item() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // Create initial item
    client
        .put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S("u1".into()))
        .item("count", AttributeValue::N("10".into()))
        .item("tags", AttributeValue::Ss(vec!["a".into(), "b".into()]))
        .item("to_remove", AttributeValue::S("remove_me".into()))
        .send()
        .await
        .unwrap();

    // UpdateItem: SET count = count + 5, REMOVE to_remove, ADD tags "c"
    client
        .update_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("u1".into()))
        .update_expression("SET #cnt = #cnt + :inc REMOVE to_remove ADD tags :new_tag")
        .expression_attribute_names("#cnt", "count")
        .expression_attribute_values(":inc", AttributeValue::N("5".into()))
        .expression_attribute_values(":new_tag", AttributeValue::Ss(vec!["c".into()]))
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("u1".into()))
        .send()
        .await
        .unwrap();

    let item = get_resp.item().unwrap();
    assert_eq!(item["count"].as_n().unwrap(), "15");
    assert!(!item.contains_key("to_remove"));
    let tags = item["tags"].as_ss().unwrap();
    assert!(tags.contains(&"c".to_string()), "tags should contain c: {tags:?}");
}

#[tokio::test]
async fn test_query_hash_only() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // Insert items
    for i in 0..5u32 {
        client
            .put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(format!("item-{i}")))
            .item("value", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    // Query for a specific pk
    let query_resp = client
        .query()
        .table_name(&table)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_values(":pk", AttributeValue::S("item-2".into()))
        .send()
        .await
        .unwrap();

    assert_eq!(query_resp.count(), 1);
    let items = query_resp.items();
    assert_eq!(items[0]["pk"].as_s().unwrap(), "item-2");
    assert_eq!(items[0]["value"].as_n().unwrap(), "2");
}

#[tokio::test]
async fn test_query_hash_sort() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_hash_sort_table(&client, &table).await;

    // Insert items with same pk but different sk
    let pk = "partition-key";
    for i in 0..10u32 {
        client
            .put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(pk.into()))
            .item("sk", AttributeValue::S(format!("sort-{i:03}")))
            .item("value", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    // Query with begins_with on sk
    let query_resp = client
        .query()
        .table_name(&table)
        .key_condition_expression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_names("#sk", "sk")
        .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
        .expression_attribute_values(":prefix", AttributeValue::S("sort-00".into()))
        .send()
        .await
        .unwrap();

    // Should match sort-000 through sort-009 (first 10)
    assert_eq!(query_resp.count(), 10, "Expected 10 items beginning with sort-00");

    // Query with BETWEEN on sk
    let query_resp2 = client
        .query()
        .table_name(&table)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :v1 AND :v2")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_names("#sk", "sk")
        .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
        .expression_attribute_values(":v1", AttributeValue::S("sort-003".into()))
        .expression_attribute_values(":v2", AttributeValue::S("sort-006".into()))
        .send()
        .await
        .unwrap();

    assert_eq!(query_resp2.count(), 4, "Expected 4 items between sort-003 and sort-006");

    // Query with ScanIndexForward = false (descending)
    let query_resp3 = client
        .query()
        .table_name(&table)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
        .scan_index_forward(false)
        .limit(3)
        .send()
        .await
        .unwrap();

    assert_eq!(query_resp3.count(), 3);
    // First item should be sort-009 (largest)
    assert_eq!(
        query_resp3.items()[0]["sk"].as_s().unwrap(),
        "sort-009"
    );
}

#[tokio::test]
async fn test_scan() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // Insert items
    for i in 0..10u32 {
        client
            .put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(format!("scan-{i}")))
            .item("value", AttributeValue::N(i.to_string()))
            .item("category", AttributeValue::S(if i % 2 == 0 { "even" } else { "odd" }.into()))
            .send()
            .await
            .unwrap();
    }

    // Full scan
    let scan_resp = client
        .scan()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    assert_eq!(scan_resp.count(), 10);

    // Scan with filter expression (even values)
    let scan_resp2 = client
        .scan()
        .table_name(&table)
        .filter_expression("category = :cat")
        .expression_attribute_values(":cat", AttributeValue::S("even".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(scan_resp2.count(), 5);

    // Scan with limit
    let scan_resp3 = client
        .scan()
        .table_name(&table)
        .limit(3)
        .send()
        .await
        .unwrap();
    assert_eq!(scan_resp3.count(), 3);
    assert!(scan_resp3.last_evaluated_key().is_some());
}

#[tokio::test]
async fn test_batch_operations() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // BatchWriteItem: put 5 items
    let mut request_items = HashMap::new();
    let writes: Vec<WriteRequest> = (0..5u32)
        .map(|i| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .item("pk", AttributeValue::S(format!("batch-{i}")))
                        .item("value", AttributeValue::N(i.to_string()))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();
    request_items.insert(table.clone(), writes);

    client
        .batch_write_item()
        .set_request_items(Some(request_items))
        .send()
        .await
        .unwrap();

    // BatchGetItem: get 3 of them
    let mut request_items = HashMap::new();
    let keys_and_attrs = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
        .keys(HashMap::from([("pk".to_string(), AttributeValue::S("batch-0".into()))]))
        .keys(HashMap::from([("pk".to_string(), AttributeValue::S("batch-2".into()))]))
        .keys(HashMap::from([("pk".to_string(), AttributeValue::S("batch-4".into()))]))
        .build()
        .unwrap();
    request_items.insert(table.clone(), keys_and_attrs);

    let batch_get_resp = client
        .batch_get_item()
        .set_request_items(Some(request_items))
        .send()
        .await
        .unwrap();

    let responses = batch_get_resp.responses().unwrap();
    let table_items = responses.get(table.as_str()).unwrap();
    assert_eq!(table_items.len(), 3);

    // BatchWriteItem: delete 2 items
    let mut del_items = HashMap::new();
    let del_writes: Vec<WriteRequest> = vec![
        WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .key("pk", AttributeValue::S("batch-0".into()))
                    .build()
                    .unwrap(),
            )
            .build(),
        WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .key("pk", AttributeValue::S("batch-1".into()))
                    .build()
                    .unwrap(),
            )
            .build(),
    ];
    del_items.insert(table.clone(), del_writes);

    client
        .batch_write_item()
        .set_request_items(Some(del_items))
        .send()
        .await
        .unwrap();

    // Verify deletion
    let get_resp = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("batch-0".into()))
        .send()
        .await
        .unwrap();
    assert!(get_resp.item().is_none());
}

#[tokio::test]
async fn test_transact_operations() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // TransactWriteItems: put 3 items atomically
    client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&table)
                        .item("pk", AttributeValue::S("tx-1".into()))
                        .item("value", AttributeValue::N("100".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&table)
                        .item("pk", AttributeValue::S("tx-2".into()))
                        .item("value", AttributeValue::N("200".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&table)
                        .item("pk", AttributeValue::S("tx-3".into()))
                        .item("value", AttributeValue::N("300".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // TransactGetItems: get all 3
    let tx_get_resp = client
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&table)
                        .key("pk", AttributeValue::S("tx-1".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&table)
                        .key("pk", AttributeValue::S("tx-2".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&table)
                        .key("pk", AttributeValue::S("tx-3".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let responses = tx_get_resp.responses();
    assert_eq!(responses.len(), 3);
    assert_eq!(
        responses[0].item().unwrap()["value"].as_n().unwrap(),
        "100"
    );
}

#[tokio::test]
async fn test_ttl() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // UpdateTimeToLive
    let ttl_resp = client
        .update_time_to_live()
        .table_name(&table)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires_at")
                .enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let spec = ttl_resp.time_to_live_specification().unwrap();
    assert_eq!(spec.attribute_name(), "expires_at");
    assert!(spec.enabled());

    // DescribeTimeToLive
    let desc_ttl = client
        .describe_time_to_live()
        .table_name(&table)
        .send()
        .await
        .unwrap();

    let desc = desc_ttl.time_to_live_description().unwrap();
    assert_eq!(desc.attribute_name(), Some("expires_at"));

    // Disable TTL
    let ttl_resp2 = client
        .update_time_to_live()
        .table_name(&table)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires_at")
                .enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert!(!ttl_resp2.time_to_live_specification().unwrap().enabled());
}

#[tokio::test]
async fn test_streams() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    // Create table with streams enabled
    client
        .create_table()
        .table_name(&table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Insert some items
    for i in 0..3u32 {
        client
            .put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(format!("stream-{i}")))
            .item("value", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    // DescribeTable to get stream ARN
    let desc = client
        .describe_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();

    let table_desc = desc.table().unwrap();
    let stream_arn = table_desc.latest_stream_arn().unwrap();
    assert!(!stream_arn.is_empty());

    // Use reqwest for Streams API (not in aws-sdk-dynamodb)
    let http_client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    // DescribeStream
    let desc_stream_resp = http_client
        .post(format!("{base_url}/dynamodb/"))
        .header("X-Amz-Target", "DynamoDBStreams_20120810.DescribeStream")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"StreamArn": stream_arn}))
        .send()
        .await
        .unwrap();
    assert!(desc_stream_resp.status().is_success());
    let desc_stream_body: serde_json::Value = desc_stream_resp.json().await.unwrap();
    let shards = desc_stream_body["StreamDescription"]["Shards"].as_array().unwrap();
    assert!(!shards.is_empty());
    let shard_id = shards[0]["ShardId"].as_str().unwrap();

    // GetShardIterator (TRIM_HORIZON)
    let iter_resp = http_client
        .post(format!("{base_url}/dynamodb/"))
        .header("X-Amz-Target", "DynamoDBStreams_20120810.GetShardIterator")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "StreamArn": stream_arn,
            "ShardId": shard_id,
            "ShardIteratorType": "TRIM_HORIZON"
        }))
        .send()
        .await
        .unwrap();
    assert!(iter_resp.status().is_success());
    let iter_body: serde_json::Value = iter_resp.json().await.unwrap();
    let iterator = iter_body["ShardIterator"].as_str().unwrap();

    // GetRecords
    let records_resp = http_client
        .post(format!("{base_url}/dynamodb/"))
        .header("X-Amz-Target", "DynamoDBStreams_20120810.GetRecords")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"ShardIterator": iterator}))
        .send()
        .await
        .unwrap();
    assert!(records_resp.status().is_success());
    let records_body: serde_json::Value = records_resp.json().await.unwrap();
    let records = records_body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 3, "Expected 3 stream records, got: {records:?}");
    // All should be INSERT events
    for record in records {
        assert_eq!(record["eventName"].as_str().unwrap(), "INSERT");
    }
}

#[tokio::test]
async fn test_condition_expressions() {
    let port = port().await;
    let client = dynamodb_client(port);
    let table = unique_table();
    let _guard = TableGuard::new(client.clone(), &table);

    create_simple_table(&client, &table).await;

    // PutItem with attribute_not_exists condition (should succeed on new item)
    client
        .put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S("cond-1".into()))
        .item("value", AttributeValue::N("10".into()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .unwrap();

    // PutItem with attribute_not_exists condition (should FAIL since item exists)
    let result = client
        .put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S("cond-1".into()))
        .item("value", AttributeValue::N("99".into()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;

    assert!(result.is_err(), "Should have failed due to condition");

    // UpdateItem with condition: value < 20
    client
        .update_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("cond-1".into()))
        .update_expression("SET #val = :new_val")
        .condition_expression("#val < :limit")
        .expression_attribute_names("#val", "value")
        .expression_attribute_values(":new_val", AttributeValue::N("20".into()))
        .expression_attribute_values(":limit", AttributeValue::N("15".into()))
        .send()
        .await
        .unwrap();

    let get_resp = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("cond-1".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.item().unwrap()["value"].as_n().unwrap(), "20");

    // DeleteItem with condition: value = 20
    client
        .delete_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("cond-1".into()))
        .condition_expression("#val = :expected")
        .expression_attribute_names("#val", "value")
        .expression_attribute_values(":expected", AttributeValue::N("20".into()))
        .send()
        .await
        .unwrap();

    let get_resp2 = client
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("cond-1".into()))
        .send()
        .await
        .unwrap();
    assert!(get_resp2.item().is_none());
}
