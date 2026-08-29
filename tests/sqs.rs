//! Integration tests for the SQS service.
//!
//! Run with: `cargo test --test sqs -- --nocapture`

use aws_sdk_sqs::{
    Client as SqsClient,
    config::{BehaviorVersion, Credentials, Region},
    types::{MessageAttributeValue, QueueAttributeName},
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

fn sqs_client(port: u16) -> SqsClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_sqs::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    SqsClient::from_conf(conf)
}

// ── Drop guard ────────────────────────────────────────────────────────────────

struct QueueGuard {
    client: SqsClient,
    queue_url: String,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let queue_url = self.queue_url.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let _ = client.delete_queue().queue_url(queue_url).send().await;
            });
        })
        .join()
        .ok();
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unique_name(prefix: &str) -> String {
    format!("{}-{}", prefix, Uuid::new_v4())
}

fn unique_fifo_name(prefix: &str) -> String {
    format!("{}-{}.fifo", prefix, Uuid::new_v4())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_list_delete_queue() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("test-queue");

    // Create
    let create_resp = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue failed");

    let queue_url = create_resp.queue_url().unwrap().to_string();
    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    assert!(queue_url.contains(&name));

    // GetQueueUrl
    let url_resp = client
        .get_queue_url()
        .queue_name(&name)
        .send()
        .await
        .expect("GetQueueUrl failed");
    assert_eq!(url_resp.queue_url().unwrap(), queue_url);

    // ListQueues with prefix
    let list_resp = client
        .list_queues()
        .queue_name_prefix("test-queue-")
        .send()
        .await
        .expect("ListQueues failed");

    let urls = list_resp.queue_urls();
    assert!(urls.iter().any(|u| u == &queue_url), "queue not in list");

    // DeleteQueue is handled by guard
}

#[tokio::test]
async fn test_send_receive_delete() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("srq");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send
    let send_resp = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("hello world")
        .send()
        .await
        .expect("SendMessage");

    let sent_id = send_resp.message_id().unwrap().to_string();
    assert!(!sent_id.is_empty());

    // Receive
    let recv_resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage");

    let messages = recv_resp.messages();
    assert_eq!(messages.len(), 1);
    let msg = &messages[0];
    assert_eq!(msg.message_id().unwrap(), sent_id);
    assert_eq!(msg.body().unwrap(), "hello world");

    let receipt = msg.receipt_handle().unwrap().to_string();

    // Delete
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage");

    // Verify empty
    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage after delete");

    assert!(recv2.messages().is_empty(), "queue should be empty after delete");
}

#[tokio::test]
async fn test_visibility_timeout() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("vis-timeout");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .attributes(
            QueueAttributeName::VisibilityTimeout,
            "1", // 1 second timeout
        )
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("visibility test")
        .send()
        .await
        .expect("SendMessage");

    // Receive once - makes it invisible for 1s
    let recv1 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage 1");

    assert_eq!(recv1.messages().len(), 1);
    let msg1_id = recv1.messages()[0].message_id().unwrap().to_string();

    // Immediately try to receive - should be empty (still invisible)
    let recv_empty = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage empty");

    assert!(recv_empty.messages().is_empty(), "message should be invisible");

    // Wait for visibility timeout to expire
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Should be visible again
    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage 2");

    assert_eq!(recv2.messages().len(), 1);
    assert_eq!(recv2.messages()[0].message_id().unwrap(), msg1_id);

    // Cleanup
    let receipt = recv2.messages()[0].receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage");
}

#[tokio::test]
async fn test_long_poll() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("long-poll");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send a message after a short delay in a background task
    let client_clone = client.clone();
    let url_clone = queue_url.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        client_clone
            .send_message()
            .queue_url(url_clone)
            .message_body("long poll message")
            .send()
            .await
            .expect("background SendMessage");
    });

    // Long-poll for 5 seconds - should return message before timeout
    let start = tokio::time::Instant::now();
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(5)
        .send()
        .await
        .expect("ReceiveMessage long poll");

    let elapsed = start.elapsed();
    assert_eq!(recv.messages().len(), 1, "should receive message");
    assert_eq!(recv.messages()[0].body().unwrap(), "long poll message");
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "long poll should return early, took {:?}",
        elapsed
    );

    // Cleanup
    let receipt = recv.messages()[0].receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage");
}

#[tokio::test]
async fn test_batch_send_receive_delete() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("batch");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send batch of 3 messages
    let send_batch = client
        .send_message_batch()
        .queue_url(&queue_url)
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("1")
                .message_body("msg1")
                .build()
                .unwrap(),
        )
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("2")
                .message_body("msg2")
                .build()
                .unwrap(),
        )
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("3")
                .message_body("msg3")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("SendMessageBatch");

    assert_eq!(send_batch.successful().len(), 3);
    assert!(send_batch.failed().is_empty());

    // Receive up to 10
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage");

    assert_eq!(recv.messages().len(), 3);

    // Delete batch
    let entries: Vec<_> = recv
        .messages()
        .iter()
        .enumerate()
        .map(|(i, m)| {
            aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                .id(format!("del-{i}"))
                .receipt_handle(m.receipt_handle().unwrap())
                .build()
                .unwrap()
        })
        .collect();

    let del_batch = client
        .delete_message_batch()
        .queue_url(&queue_url)
        .set_entries(Some(entries))
        .send()
        .await
        .expect("DeleteMessageBatch");

    assert_eq!(del_batch.successful().len(), 3);
    assert!(del_batch.failed().is_empty());

    // Verify empty
    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage after batch delete");

    assert!(recv2.messages().is_empty());
}

#[tokio::test]
async fn test_purge_queue() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("purge");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send some messages
    for i in 0..5 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("msg {i}"))
            .send()
            .await
            .expect("SendMessage");
    }

    // Purge
    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("PurgeQueue");

    // Verify empty
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage after purge");

    assert!(recv.messages().is_empty(), "queue should be empty after purge");
}

#[tokio::test]
async fn test_queue_attributes() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("attrs");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .attributes(QueueAttributeName::VisibilityTimeout, "60")
        .attributes(QueueAttributeName::MessageRetentionPeriod, "86400")
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // GetQueueAttributes
    let get_resp = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .expect("GetQueueAttributes");

    let attrs = get_resp.attributes().cloned().unwrap_or_default();
    assert_eq!(
        attrs.get(&QueueAttributeName::VisibilityTimeout),
        Some(&"60".to_string())
    );
    assert_eq!(
        attrs.get(&QueueAttributeName::MessageRetentionPeriod),
        Some(&"86400".to_string())
    );
    assert!(attrs.contains_key(&QueueAttributeName::QueueArn));

    // SetQueueAttributes
    client
        .set_queue_attributes()
        .queue_url(&queue_url)
        .attributes(QueueAttributeName::VisibilityTimeout, "45")
        .send()
        .await
        .expect("SetQueueAttributes");

    let get_resp2 = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .expect("GetQueueAttributes after set");

    let attrs2 = get_resp2.attributes().cloned().unwrap_or_default();
    assert_eq!(
        attrs2.get(&QueueAttributeName::VisibilityTimeout),
        Some(&"45".to_string())
    );

    // Send a message and check ApproximateNumberOfMessages
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("test")
        .send()
        .await
        .expect("SendMessage for attrs test");

    let get_count = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .expect("GetQueueAttributes count");

    let count_attrs = get_count.attributes().cloned().unwrap_or_default();
    assert_eq!(
        count_attrs.get(&QueueAttributeName::ApproximateNumberOfMessages),
        Some(&"1".to_string())
    );

    // Purge to clean up
    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("PurgeQueue cleanup");
}

#[tokio::test]
async fn test_fifo_queue() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_fifo_name("fifo");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .expect("CreateQueue FIFO")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send a message
    let send1 = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("fifo msg 1")
        .message_group_id("grp1")
        .send()
        .await
        .expect("SendMessage FIFO 1");

    let msg_id_1 = send1.message_id().unwrap().to_string();

    // Send duplicate (same body = same dedup ID with ContentBasedDeduplication)
    let send2 = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("fifo msg 1")
        .message_group_id("grp1")
        .send()
        .await
        .expect("SendMessage FIFO 2 duplicate");

    // Same message ID returned (dedup)
    assert_eq!(
        send2.message_id().unwrap(),
        msg_id_1,
        "duplicate message should return same message ID"
    );

    // Receive - should get only one message (dedup)
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage FIFO");

    assert_eq!(recv.messages().len(), 1);
    assert_eq!(recv.messages()[0].body().unwrap(), "fifo msg 1");

    // Send a different message with explicit dedup ID
    let send3 = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("fifo msg 2")
        .message_group_id("grp1")
        .message_deduplication_id("unique-dedup-id-2")
        .send()
        .await
        .expect("SendMessage FIFO 3");

    assert_ne!(send3.message_id().unwrap(), msg_id_1);

    // Cleanup: delete received message and the new one
    let receipt = recv.messages()[0].receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage FIFO 1");

    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage FIFO 2");

    if !recv2.messages().is_empty() {
        let receipt2 = recv2.messages()[0].receipt_handle().unwrap().to_string();
        client
            .delete_message()
            .queue_url(&queue_url)
            .receipt_handle(receipt2)
            .send()
            .await
            .expect("DeleteMessage FIFO 2");
    }
}

#[tokio::test]
async fn test_dead_letter_queue() {
    let port = port().await;
    let client = sqs_client(port);
    let dlq_name = unique_name("dlq");
    let src_name = unique_name("src");

    // Create DLQ
    let dlq_url = client
        .create_queue()
        .queue_name(&dlq_name)
        .send()
        .await
        .expect("CreateQueue DLQ")
        .queue_url()
        .unwrap()
        .to_string();

    let _dlq_guard = QueueGuard {
        client: client.clone(),
        queue_url: dlq_url.clone(),
    };

    let dlq_arn = format!(
        "arn:aws:sqs:eu-west-1:000000000000:{dlq_name}"
    );
    let max_receive_count = 2u32;

    let redrive_policy = serde_json::json!({
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": max_receive_count
    })
    .to_string();

    // Create source queue with VisibilityTimeout=0 for easy re-receiving
    let src_url = client
        .create_queue()
        .queue_name(&src_name)
        .attributes(QueueAttributeName::VisibilityTimeout, "0")
        .attributes(QueueAttributeName::RedrivePolicy, &redrive_policy)
        .send()
        .await
        .expect("CreateQueue source")
        .queue_url()
        .unwrap()
        .to_string();

    let _src_guard = QueueGuard {
        client: client.clone(),
        queue_url: src_url.clone(),
    };

    // Send a message to source
    client
        .send_message()
        .queue_url(&src_url)
        .message_body("dlq test message")
        .send()
        .await
        .expect("SendMessage to source");

    // Receive maxReceiveCount+1 times to trigger DLQ move
    // With VisibilityTimeout=0, message becomes visible again immediately after receive
    for i in 0..=(max_receive_count) {
        let recv = client
            .receive_message()
            .queue_url(&src_url)
            .max_number_of_messages(1)
            .send()
            .await
            .expect(&format!("ReceiveMessage iteration {i}"));

        if recv.messages().is_empty() {
            // Message may have been moved to DLQ
            break;
        }
    }

    // Give a moment for any async operations
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Source queue should be empty
    let src_recv = client
        .receive_message()
        .queue_url(&src_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage from source after DLQ");

    assert!(
        src_recv.messages().is_empty(),
        "source queue should be empty after DLQ move"
    );

    // DLQ should have the message
    let dlq_recv = client
        .receive_message()
        .queue_url(&dlq_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage from DLQ");

    assert_eq!(
        dlq_recv.messages().len(),
        1,
        "DLQ should have 1 message"
    );
    assert_eq!(dlq_recv.messages()[0].body().unwrap(), "dlq test message");

    // Cleanup DLQ message
    let receipt = dlq_recv.messages()[0].receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&dlq_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage from DLQ");
}

#[tokio::test]
async fn test_change_message_visibility() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("change-vis");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .attributes(QueueAttributeName::VisibilityTimeout, "30")
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("change vis test")
        .send()
        .await
        .expect("SendMessage");

    // Receive - message is invisible for 30s
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage");

    assert_eq!(recv.messages().len(), 1);
    let receipt = recv.messages()[0].receipt_handle().unwrap().to_string();
    let msg_id = recv.messages()[0].message_id().unwrap().to_string();

    // Change visibility to 0 - makes it immediately visible
    client
        .change_message_visibility()
        .queue_url(&queue_url)
        .receipt_handle(&receipt)
        .visibility_timeout(0)
        .send()
        .await
        .expect("ChangeMessageVisibility");

    // Should be able to receive immediately
    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("ReceiveMessage after visibility change");

    assert_eq!(recv2.messages().len(), 1);
    assert_eq!(recv2.messages()[0].message_id().unwrap(), msg_id);

    // Cleanup
    let receipt2 = recv2.messages()[0].receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt2)
        .send()
        .await
        .expect("DeleteMessage");
}

#[tokio::test]
async fn test_message_attributes() {
    let port = port().await;
    let client = sqs_client(port);
    let name = unique_name("msg-attrs");

    let queue_url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .expect("CreateQueue")
        .queue_url()
        .unwrap()
        .to_string();

    let _guard = QueueGuard {
        client: client.clone(),
        queue_url: queue_url.clone(),
    };

    // Send with message attributes
    let mut msg_attrs = HashMap::new();
    msg_attrs.insert(
        "Color".to_string(),
        MessageAttributeValue::builder()
            .data_type("String")
            .string_value("Red")
            .build()
            .unwrap(),
    );
    msg_attrs.insert(
        "Count".to_string(),
        MessageAttributeValue::builder()
            .data_type("Number")
            .string_value("42")
            .build()
            .unwrap(),
    );

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("with attrs")
        .set_message_attributes(Some(msg_attrs))
        .send()
        .await
        .expect("SendMessage with attrs");

    // Receive and request message attributes
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .message_attribute_names("All")
        .send()
        .await
        .expect("ReceiveMessage");

    assert_eq!(recv.messages().len(), 1);
    let msg = &recv.messages()[0];
    let received_attrs = msg.message_attributes().cloned().unwrap_or_default();

    assert!(received_attrs.contains_key("Color"), "Color attribute missing");
    assert_eq!(
        received_attrs["Color"].string_value().unwrap_or(""),
        "Red"
    );
    assert!(received_attrs.contains_key("Count"), "Count attribute missing");

    // Cleanup
    let receipt = msg.receipt_handle().unwrap().to_string();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt)
        .send()
        .await
        .expect("DeleteMessage");
}
