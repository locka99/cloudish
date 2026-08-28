//! Integration tests for the S3 service.
//!
//! Run with: `cargo test --test s3 -- --nocapture`
//!
//! The tests start their own server on an OS-assigned port. The test data
//! directory (`data/test_{port}/`) is wiped at server startup and each test
//! cleans up its own resources via a drop guard — cleanup runs even on panic.

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
};
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
    // std::sync::Once ensures start_server_sync runs exactly once across all
    // test runtimes. AtomicU16 stores the result. spawn_blocking offloads the
    // blocking init to the thread pool so we don't starve the executor.
    static INIT: std::sync::Once = std::sync::Once::new();
    static PORT: std::sync::atomic::AtomicU16 =
        std::sync::atomic::AtomicU16::new(0);

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

// ── Client factory ────────────────────────────────────────────────────────────

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

// ── Drop-guard cleanup ────────────────────────────────────────────────────────

/// RAII guard that deletes a bucket (and all its objects) when dropped.
/// Cleanup runs even if the test panics.
struct BucketGuard {
    client: Client,
    bucket: String,
}

impl BucketGuard {
    fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self { client, bucket: bucket.into() }
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

impl Drop for BucketGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        // Fire-and-forget: spawn a background thread for cleanup.
        // We must NOT call join() here — doing so while inside a #[tokio::test]
        // single-threaded runtime would deadlock because the spawned thread's
        // new_current_thread runtime would be blocked trying to send requests
        // while this thread (the runtime's thread) is parked in join().
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    // Delete all objects (handle pagination).
                    let mut continuation_token: Option<String> = None;
                    loop {
                        let mut req = client.list_objects_v2().bucket(&bucket);
                        if let Some(tok) = &continuation_token {
                            req = req.continuation_token(tok);
                        }
                        let resp = match req.send().await {
                            Ok(r) => r,
                            Err(_) => break,
                        };
                        for obj in resp.contents() {
                            if let Some(key) = obj.key() {
                                let _ = client
                                    .delete_object()
                                    .bucket(&bucket)
                                    .key(key)
                                    .send()
                                    .await;
                            }
                        }
                        if resp.is_truncated().unwrap_or(false) {
                            continuation_token = resp.next_continuation_token().map(str::to_string);
                        } else {
                            break;
                        }
                    }
                    let _ = client.delete_bucket().bucket(&bucket).send().await;
                });
        });
    }
}

// ── Bucket tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_delete_bucket() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);

    client.create_bucket().bucket(&bucket).send().await.unwrap();

    // Head bucket should succeed.
    client.head_bucket().bucket(&bucket).send().await.unwrap();

    // Delete the bucket manually as part of the test assertion.
    client.delete_bucket().bucket(&bucket).send().await.unwrap();

    // Head bucket should now fail.
    assert!(
        client.head_bucket().bucket(&bucket).send().await.is_err(),
        "bucket should no longer exist"
    );
    // Guard drop is a no-op here since the bucket is already gone.
}

#[tokio::test]
async fn test_list_buckets() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);

    client.create_bucket().bucket(&bucket).send().await.unwrap();

    let resp = client.list_buckets().send().await.unwrap();
    let names: Vec<&str> = resp.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(names.contains(&bucket.as_str()), "bucket not in list: {names:?}");
}

// ── Object tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_put_and_get_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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
}

#[tokio::test]
async fn test_head_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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
}

#[tokio::test]
async fn test_delete_object() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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

    assert!(
        client
            .get_object()
            .bucket(&bucket)
            .key("to-delete.txt")
            .send()
            .await
            .is_err(),
        "object should be deleted"
    );
}

#[tokio::test]
async fn test_object_not_found() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    assert!(
        client
            .get_object()
            .bucket(&bucket)
            .key("nonexistent.txt")
            .send()
            .await
            .is_err()
    );
}

// ── User metadata ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_object_user_metadata() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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
}

// ── List objects ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_objects() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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

    // List all.
    let resp = client.list_objects_v2().bucket(&bucket).send().await.unwrap();
    assert_eq!(resp.key_count(), Some(3));

    // List with prefix.
    let resp = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("a/")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count(), Some(2));

    // List with delimiter (common prefixes).
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
}

// ── Multipart upload ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_multipart_upload() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
    client.create_bucket().bucket(&bucket).send().await.unwrap();

    // Parts must be ≥5 MiB except the last.
    let part1 = vec![b'A'; 5 * 1024 * 1024];
    let part2 = vec![b'B'; 1024];

    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create_resp.upload_id().unwrap().to_string();

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

    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(p1.e_tag().unwrap_or(""))
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(p2.e_tag().unwrap_or(""))
                .build(),
        )
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

    let resp = client
        .get_object()
        .bucket(&bucket)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.len(), part1.len() + part2.len());
    assert!(data[..5].iter().all(|&b| b == b'A'));
    assert!(data[part1.len()..].iter().all(|&b| b == b'B'));
}

// ── Presigned URLs ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_presigned_get() {
    let port = port().await;
    let client = s3_client(port);
    let bucket = unique_bucket();
    let _guard = BucketGuard::new(client.clone(), &bucket);
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

    let resp = reqwest::get(presigned.uri().to_string()).await.unwrap();
    assert!(resp.status().is_success());
    let fetched = resp.bytes().await.unwrap();
    assert_eq!(fetched.as_ref(), body);
}
