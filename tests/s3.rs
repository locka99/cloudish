//! Integration tests for the S3 service.
//!
//! Run with: `cargo test --test s3 -- --nocapture`
//!
//! The tests start their own server on an OS-assigned port.

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
};
use uuid::Uuid;

fn start_server_sync() -> u16 {
    use std::net::TcpListener as StdListener;
    // Bind port 0 to get OS-assigned port using std (sync)
    let std_listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    // Convert to non-blocking so tokio can take it
    std_listener.set_nonblocking(true).unwrap();

    // Spawn a dedicated OS thread with its own tokio runtime to host the server.
    // This ensures the server outlives any individual test's runtime.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            let state = std::sync::Arc::new(
                cloudish::AppState::new_with_data_dir(format!("data/test_{}", port))
                    .await
                    .unwrap(),
            );
            let app = cloudish::build_app(state).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });

    // Give the server a moment to start accepting connections.
    // Retry up to 1 second in case the OS needs time to bind.
    for _ in 0..20 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    port
}

async fn port() -> u16 {
    static PORT: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *PORT.get_or_init(start_server_sync)
}

fn s3_client(port: u16) -> Client {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

fn unique_bucket() -> String {
    format!("test-{}", Uuid::new_v4().simple())
}

/// Delete all objects in a bucket then delete the bucket itself.
async fn cleanup(client: &Client, bucket: &str) {
    // List and delete all objects
    if let Ok(resp) = client.list_objects_v2().bucket(bucket).send().await {
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let _ = client.delete_object().bucket(bucket).key(key).send().await;
            }
        }
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ── Bucket tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_delete_bucket() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();

    client.create_bucket().bucket(&bucket).send().await.unwrap();

    // Head bucket should succeed
    client.head_bucket().bucket(&bucket).send().await.unwrap();

    // Delete bucket
    client.delete_bucket().bucket(&bucket).send().await.unwrap();

    // Head bucket should now fail
    let result = client.head_bucket().bucket(&bucket).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_list_buckets() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();

    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let resp = client.list_buckets().send().await.unwrap();
    let names: Vec<&str> = resp
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();
    assert!(names.contains(&bucket.as_str()), "bucket not in list: {names:?}");

    cleanup(&client, &bucket).await;
}

// ── Object tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_put_and_get_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let body = b"hello, cloudish!";
    client
        .put_object()
        .bucket(&bucket)
        .key("hello.txt")
        .content_type("text/plain")
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key("hello.txt")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.content_type().unwrap_or(""), "text/plain");
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), body);

    cleanup(&client, &bucket).await;
}

#[tokio::test]
async fn test_head_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let body = b"head test data";
    client
        .put_object()
        .bucket(&bucket)
        .key("head-test.bin")
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    let resp = client
        .head_object()
        .bucket(&bucket)
        .key("head-test.bin")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.content_length(), Some(body.len() as i64));

    cleanup(&client, &bucket).await;
}

#[tokio::test]
async fn test_delete_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("to-delete.txt")
        .body(ByteStream::from_static(b"bye"))
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket(&bucket)
        .key("to-delete.txt")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket(&bucket)
        .key("to-delete.txt")
        .send()
        .await;
    assert!(result.is_err(), "object should be deleted");

    cleanup(&client, &bucket).await;
}

#[tokio::test]
async fn test_object_not_found() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let result = client
        .get_object()
        .bucket(&bucket)
        .key("nonexistent.txt")
        .send()
        .await;
    assert!(result.is_err());

    cleanup(&client, &bucket).await;
}

// ── User metadata ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_object_user_metadata() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("meta.txt")
        .body(ByteStream::from_static(b"data"))
        .metadata("author", "cloudish")
        .metadata("version", "42")
        .send()
        .await
        .unwrap();

    let resp = client
        .head_object()
        .bucket(&bucket)
        .key("meta.txt")
        .send()
        .await
        .unwrap();

    let meta = resp.metadata().expect("metadata should be present");
    assert_eq!(meta.get("author").map(String::as_str), Some("cloudish"));
    assert_eq!(meta.get("version").map(String::as_str), Some("42"));

    cleanup(&client, &bucket).await;
}

// ── List objects ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_objects() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    for key in ["a/1.txt", "a/2.txt", "b/1.txt"] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }

    // List all
    let resp = client.list_objects_v2().bucket(&bucket).send().await.unwrap();
    assert_eq!(resp.key_count(), Some(3));

    // List with prefix
    let resp = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("a/")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count(), Some(2));

    // List with delimiter (common prefixes)
    let resp = client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|cp| cp.prefix())
        .collect();
    assert!(prefixes.contains(&"a/"), "expected a/ in {prefixes:?}");
    assert!(prefixes.contains(&"b/"), "expected b/ in {prefixes:?}");

    cleanup(&client, &bucket).await;
}

// ── Multipart upload ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_multipart_upload() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    // AWS requires parts to be at least 5 MB except the last one
    let part1 = vec![b'A'; 5 * 1024 * 1024];
    let part2 = vec![b'B'; 1024]; // last part can be smaller

    // Initiate
    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create_resp.upload_id().unwrap().to_string();

    // Upload parts
    let p1 = client
        .upload_part()
        .bucket(&bucket)
        .key("multi.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await
        .unwrap();

    let p2 = client
        .upload_part()
        .bucket(&bucket)
        .key("multi.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await
        .unwrap();

    // Complete
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(p1.e_tag().unwrap_or("")).build())
        .parts(CompletedPart::builder().part_number(2).e_tag(p2.e_tag().unwrap_or("")).build())
        .build();

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("multi.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();

    // Verify the assembled object
    let resp = client
        .get_object()
        .bucket(&bucket)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let data = resp.body.collect().await.unwrap().into_bytes();
    let expected_len = part1.len() + part2.len();
    assert_eq!(data.len(), expected_len, "assembled object size mismatch");
    assert!(data[..5].iter().all(|&b| b == b'A'));
    assert!(data[part1.len()..].iter().all(|&b| b == b'B'));

    cleanup(&client, &bucket).await;
}

// ── Presigned URLs ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_presigned_get() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let body = b"presigned content";
    client
        .put_object()
        .bucket(&bucket)
        .key("presigned.txt")
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key("presigned.txt")
        .presigned(
            aws_sdk_s3::presigning::PresigningConfig::expires_in(
                std::time::Duration::from_secs(300),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Fetch using reqwest (plain HTTP, no SDK)
    let resp = reqwest::get(presigned.uri().to_string()).await.unwrap();
    assert!(resp.status().is_success());
    let fetched = resp.bytes().await.unwrap();
    assert_eq!(fetched.as_ref(), body);

    cleanup(&client, &bucket).await;
}
