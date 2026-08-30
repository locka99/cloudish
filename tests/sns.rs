//! Integration tests for the SNS service.
//!
//! Run with: `cargo test --test sns -- --nocapture`

use aws_sdk_sns::{
    Client as SnsClient,
    config::{BehaviorVersion, Credentials, Region},
};
use aws_sdk_sqs::Client as SqsClient;
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

fn sns_client(port: u16) -> SnsClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_sns::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    SnsClient::from_conf(conf)
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

// ── Drop guards ───────────────────────────────────────────────────────────────

/// Deletes a topic via raw TCP so it works inside Drop even from another runtime.
struct TopicGuard {
    port: u16,
    topic_arn: String,
}

impl Drop for TopicGuard {
    fn drop(&mut self) {
        use std::io::Write;
        let addr = format!("127.0.0.1:{}", self.port);
        // SNS uses POST with form body
        let body = format!("Action=DeleteTopic&TopicArn={}&Version=2010-03-31",
            urlencoding::encode(&self.topic_arn));
        let request = format!(
            "POST / HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(request.as_bytes());
        }
    }
}

/// Deletes an SQS queue via raw TCP.
struct QueueGuard {
    port: u16,
    queue_name: String,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        use std::io::Write;
        let addr = format!("127.0.0.1:{}", self.port);
        let queue_url = format!("http://127.0.0.1:{}/000000000000/{}", self.port, self.queue_name);
        let body = format!("Action=DeleteQueue&QueueUrl={}&Version=2012-11-05",
            urlencoding::encode(&queue_url));
        let request = format!(
            "POST / HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(request.as_bytes());
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topic_crud() {
    let port = port().await;
    let client = sns_client(port);

    let name = format!("test-{}", Uuid::new_v4().simple());

    // CreateTopic
    let create = client.create_topic().name(&name).send().await.unwrap();
    let arn = create.topic_arn().unwrap().to_string();
    assert!(arn.ends_with(&name));

    let _guard = TopicGuard { port, topic_arn: arn.clone() };

    // CreateTopic is idempotent — same ARN returned
    let create2 = client.create_topic().name(&name).send().await.unwrap();
    assert_eq!(create2.topic_arn().unwrap(), &arn);

    // ListTopics — our topic appears
    let list = client.list_topics().send().await.unwrap();
    let arns: Vec<_> = list.topics().iter().filter_map(|t| t.topic_arn()).collect();
    assert!(arns.contains(&&arn.as_str()), "expected {arn} in list: {arns:?}");

    // GetTopicAttributes
    let attrs = client.get_topic_attributes().topic_arn(&arn).send().await.unwrap();
    let attr_map = attrs.attributes().unwrap();
    assert_eq!(attr_map.get("TopicArn").map(|s| s.as_str()), Some(arn.as_str()));

    // DeleteTopic
    client.delete_topic().topic_arn(&arn).send().await.unwrap();

    // After deletion the topic should not appear in list
    let list2 = client.list_topics().send().await.unwrap();
    let arns2: Vec<_> = list2.topics().iter().filter_map(|t| t.topic_arn()).collect();
    assert!(!arns2.contains(&&arn.as_str()), "topic still in list after delete");

    // Guard drop will attempt another delete — that's fine (no-op for missing topics)
}

#[tokio::test]
async fn test_topic_tags() {
    let port = port().await;
    let client = sns_client(port);

    let name = format!("test-tags-{}", Uuid::new_v4().simple());
    let create = client.create_topic().name(&name).send().await.unwrap();
    let arn = create.topic_arn().unwrap().to_string();
    let _guard = TopicGuard { port, topic_arn: arn.clone() };

    // Tag it
    client
        .tag_resource()
        .resource_arn(&arn)
        .tags(aws_sdk_sns::types::Tag::builder().key("Env").value("test").build().unwrap())
        .tags(aws_sdk_sns::types::Tag::builder().key("Owner").value("ci").build().unwrap())
        .send()
        .await
        .unwrap();

    // ListTagsForResource
    let tags = client.list_tags_for_resource().resource_arn(&arn).send().await.unwrap();
    let tag_map: HashMap<_, _> = tags.tags().iter().map(|t| (t.key(), t.value())).collect();
    assert_eq!(tag_map.get("Env"), Some(&"test"));
    assert_eq!(tag_map.get("Owner"), Some(&"ci"));

    // UntagResource
    client.untag_resource().resource_arn(&arn).tag_keys("Owner").send().await.unwrap();
    let tags2 = client.list_tags_for_resource().resource_arn(&arn).send().await.unwrap();
    let tag_map2: HashMap<_, _> = tags2.tags().iter().map(|t| (t.key(), t.value())).collect();
    assert!(tag_map2.contains_key("Env"));
    assert!(!tag_map2.contains_key("Owner"));
}

#[tokio::test]
async fn test_subscribe_and_list() {
    let port = port().await;
    let client = sns_client(port);

    let name = format!("test-sub-{}", Uuid::new_v4().simple());
    let create = client.create_topic().name(&name).send().await.unwrap();
    let arn = create.topic_arn().unwrap().to_string();
    let _guard = TopicGuard { port, topic_arn: arn.clone() };

    // Subscribe with email (log-only delivery)
    let sub = client
        .subscribe()
        .topic_arn(&arn)
        .protocol("email")
        .endpoint("test@example.com")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();
    assert!(!sub_arn.is_empty());
    assert!(sub_arn.contains(&arn));

    // ListSubscriptions
    let list = client.list_subscriptions().send().await.unwrap();
    let sub_arns: Vec<_> = list.subscriptions().iter()
        .filter_map(|s| s.subscription_arn())
        .collect();
    assert!(sub_arns.contains(&&sub_arn.as_str()));

    // ListSubscriptionsByTopic
    let by_topic = client.list_subscriptions_by_topic().topic_arn(&arn).send().await.unwrap();
    let by_topic_arns: Vec<_> = by_topic.subscriptions().iter()
        .filter_map(|s| s.subscription_arn())
        .collect();
    assert!(by_topic_arns.contains(&&sub_arn.as_str()));

    // GetSubscriptionAttributes
    let attrs = client.get_subscription_attributes().subscription_arn(&sub_arn).send().await.unwrap();
    let attr_map = attrs.attributes().unwrap();
    assert_eq!(attr_map.get("TopicArn").map(|s| s.as_str()), Some(arn.as_str()));

    // Unsubscribe
    client.unsubscribe().subscription_arn(&sub_arn).send().await.unwrap();

    // After unsubscribe, the subscription should not appear in list
    let list2 = client.list_subscriptions_by_topic().topic_arn(&arn).send().await.unwrap();
    let arns2: Vec<_> = list2.subscriptions().iter()
        .filter_map(|s| s.subscription_arn())
        .collect();
    assert!(!arns2.contains(&&sub_arn.as_str()));
}

#[tokio::test]
async fn test_publish_to_sqs() {
    let port = port().await;
    let sns = sns_client(port);
    let sqs = sqs_client(port);

    // Create a topic
    let topic_name = format!("test-pub-{}", Uuid::new_v4().simple());
    let topic = sns.create_topic().name(&topic_name).send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();
    let _topic_guard = TopicGuard { port, topic_arn: topic_arn.clone() };

    // Create an SQS queue
    let queue_name = format!("test-sns-{}", Uuid::new_v4().simple());
    let _queue_guard = QueueGuard { port, queue_name: queue_name.clone() };
    sqs.create_queue().queue_name(&queue_name).send().await.unwrap();
    let queue_url = format!("http://127.0.0.1:{port}/000000000000/{queue_name}");

    // Subscribe queue to topic
    let queue_arn = format!("arn:aws:sqs:eu-west-1:000000000000:{queue_name}");
    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    // Publish a message
    let publish = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("hello from SNS")
        .subject("test-subject")
        .send()
        .await
        .unwrap();
    let msg_id = publish.message_id().unwrap();
    assert!(!msg_id.is_empty());

    // Give delivery a moment
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Receive from SQS
    let recv = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(2)
        .send()
        .await
        .unwrap();

    let messages = recv.messages();
    assert_eq!(messages.len(), 1, "expected 1 message in SQS queue");

    let body = messages[0].body().unwrap();
    // SNS wraps the message in a JSON envelope by default
    let envelope: serde_json::Value = serde_json::from_str(body).expect("body should be JSON");
    assert_eq!(envelope["Type"].as_str(), Some("Notification"));
    assert_eq!(envelope["Message"].as_str(), Some("hello from SNS"));
    assert_eq!(envelope["TopicArn"].as_str(), Some(topic_arn.as_str()));

    // Clean up subscription
    sns.unsubscribe().subscription_arn(&sub_arn).send().await.unwrap();
}

#[tokio::test]
async fn test_publish_batch() {
    let port = port().await;
    let sns = sns_client(port);
    let sqs = sqs_client(port);

    let topic_name = format!("test-batch-{}", Uuid::new_v4().simple());
    let topic = sns.create_topic().name(&topic_name).send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();
    let _topic_guard = TopicGuard { port, topic_arn: topic_arn.clone() };

    let queue_name = format!("test-batch-sqs-{}", Uuid::new_v4().simple());
    let _queue_guard = QueueGuard { port, queue_name: queue_name.clone() };
    sqs.create_queue().queue_name(&queue_name).send().await.unwrap();
    let queue_url = format!("http://127.0.0.1:{port}/000000000000/{queue_name}");

    let queue_arn = format!("arn:aws:sqs:eu-west-1:000000000000:{queue_name}");
    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    // PublishBatch — send 3 messages
    let batch = sns
        .publish_batch()
        .topic_arn(&topic_arn)
        .publish_batch_request_entries(
            aws_sdk_sns::types::PublishBatchRequestEntry::builder()
                .id("1")
                .message("msg-one")
                .build()
                .unwrap(),
        )
        .publish_batch_request_entries(
            aws_sdk_sns::types::PublishBatchRequestEntry::builder()
                .id("2")
                .message("msg-two")
                .build()
                .unwrap(),
        )
        .publish_batch_request_entries(
            aws_sdk_sns::types::PublishBatchRequestEntry::builder()
                .id("3")
                .message("msg-three")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(batch.successful().len(), 3);
    assert!(batch.failed().is_empty());

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Receive all 3 messages
    let mut received = 0usize;
    for _ in 0..5 {
        let recv = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        received += recv.messages().len();
        if received >= 3 { break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(received, 3, "expected 3 messages from batch, got {received}");

    sns.unsubscribe().subscription_arn(&sub_arn).send().await.unwrap();
}

#[tokio::test]
async fn test_raw_message_delivery() {
    let port = port().await;
    let sns = sns_client(port);
    let sqs = sqs_client(port);

    let topic_name = format!("test-raw-{}", Uuid::new_v4().simple());
    let topic = sns.create_topic().name(&topic_name).send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();
    let _topic_guard = TopicGuard { port, topic_arn: topic_arn.clone() };

    let queue_name = format!("test-raw-sqs-{}", Uuid::new_v4().simple());
    let _queue_guard = QueueGuard { port, queue_name: queue_name.clone() };
    sqs.create_queue().queue_name(&queue_name).send().await.unwrap();
    let queue_url = format!("http://127.0.0.1:{port}/000000000000/{queue_name}");

    let queue_arn = format!("arn:aws:sqs:eu-west-1:000000000000:{queue_name}");

    // Subscribe with RawMessageDelivery=true
    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .attributes("RawMessageDelivery", "true")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    sns.publish()
        .topic_arn(&topic_arn)
        .message("raw-payload")
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let recv = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(2)
        .send()
        .await
        .unwrap();

    let messages = recv.messages();
    assert_eq!(messages.len(), 1);
    // With raw delivery, the body should be the raw message, not a JSON envelope
    assert_eq!(messages[0].body().unwrap(), "raw-payload");

    sns.unsubscribe().subscription_arn(&sub_arn).send().await.unwrap();
}

#[tokio::test]
async fn test_set_topic_attributes() {
    let port = port().await;
    let client = sns_client(port);

    let name = format!("test-attr-{}", Uuid::new_v4().simple());
    let create = client.create_topic().name(&name).send().await.unwrap();
    let arn = create.topic_arn().unwrap().to_string();
    let _guard = TopicGuard { port, topic_arn: arn.clone() };

    // SetTopicAttributes — change DisplayName
    client
        .set_topic_attributes()
        .topic_arn(&arn)
        .attribute_name("DisplayName")
        .attribute_value("My Test Topic")
        .send()
        .await
        .unwrap();

    let attrs = client.get_topic_attributes().topic_arn(&arn).send().await.unwrap();
    assert_eq!(
        attrs.attributes().unwrap().get("DisplayName").map(|s| s.as_str()),
        Some("My Test Topic")
    );
}
