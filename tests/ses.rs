//! Integration tests for the SES service.
//!
//! Run with: `cargo test --test ses -- --nocapture`

use aws_sdk_ses::{
    Client as SesClient,
    config::{BehaviorVersion, Credentials, Region},
    types::{Body, Content, Destination, Message},
};
use uuid::Uuid;

// ── Server startup ─────────────────────────────────────────────────────────────

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

fn ses_client(port: u16) -> SesClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_ses::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    SesClient::from_conf(conf)
}

// ── Drop guard ─────────────────────────────────────────────────────────────────

struct IdentityGuard {
    client: SesClient,
    address: String,
}

impl Drop for IdentityGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let address = self.address.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let _ = client.delete_identity().identity(&address).send().await;
            });
        })
        .join()
        .ok();
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_verify_and_list_identity() {
    let port = port().await;
    let client = ses_client(port);
    let address = format!("test-{}@example.com", Uuid::new_v4());
    let _guard = IdentityGuard { client: client.clone(), address: address.clone() };

    client
        .verify_email_identity()
        .email_address(&address)
        .send()
        .await
        .expect("VerifyEmailIdentity failed");

    let list = client
        .list_identities()
        .send()
        .await
        .expect("ListIdentities failed");

    assert!(
        list.identities().contains(&address),
        "verified address not found in ListIdentities"
    );
}

#[tokio::test]
async fn test_get_identity_verification_attributes() {
    let port = port().await;
    let client = ses_client(port);
    let address = format!("verify-attr-{}@example.com", Uuid::new_v4());
    let _guard = IdentityGuard { client: client.clone(), address: address.clone() };

    client
        .verify_email_identity()
        .email_address(&address)
        .send()
        .await
        .expect("VerifyEmailIdentity failed");

    let attrs = client
        .get_identity_verification_attributes()
        .identities(&address)
        .send()
        .await
        .expect("GetIdentityVerificationAttributes failed");

    let verification_attrs = attrs.verification_attributes();
    let status = verification_attrs
        .get(&address)
        .expect("address not in response")
        .verification_status();
    assert_eq!(
        status,
        &aws_sdk_ses::types::VerificationStatus::Success,
        "expected Success verification status"
    );
}

#[tokio::test]
async fn test_delete_identity() {
    let port = port().await;
    let client = ses_client(port);
    let address = format!("delete-{}@example.com", Uuid::new_v4());

    client
        .verify_email_identity()
        .email_address(&address)
        .send()
        .await
        .expect("VerifyEmailIdentity failed");

    client
        .delete_identity()
        .identity(&address)
        .send()
        .await
        .expect("DeleteIdentity failed");

    let list = client
        .list_identities()
        .send()
        .await
        .expect("ListIdentities failed");

    assert!(
        !list.identities().contains(&address),
        "deleted address should not appear in ListIdentities"
    );
}

#[tokio::test]
async fn test_send_email() {
    let port = port().await;
    let client = ses_client(port);
    let from = format!("sender-{}@example.com", Uuid::new_v4());
    let to = format!("recipient-{}@example.com", Uuid::new_v4());
    let _guard = IdentityGuard { client: client.clone(), address: from.clone() };

    client
        .verify_email_identity()
        .email_address(&from)
        .send()
        .await
        .expect("VerifyEmailIdentity failed");

    let subject = Content::builder()
        .data("Hello from cloudish")
        .charset("UTF-8")
        .build()
        .unwrap();

    let body_text = Content::builder()
        .data("This is a test email.")
        .charset("UTF-8")
        .build()
        .unwrap();

    let body = Body::builder().text(body_text).build();

    let message = Message::builder()
        .subject(subject)
        .body(body)
        .build();

    let dest = Destination::builder().to_addresses(&to).build();

    let resp = client
        .send_email()
        .source(&from)
        .destination(dest)
        .message(message)
        .send()
        .await
        .expect("SendEmail failed");

    let msg_id = resp.message_id();
    assert!(!msg_id.is_empty(), "expected a non-empty MessageId");
    assert!(
        msg_id.ends_with("@email.amazonses.com"),
        "MessageId should end with @email.amazonses.com, got: {msg_id}"
    );
}

#[tokio::test]
async fn test_get_send_quota() {
    let port = port().await;
    let client = ses_client(port);

    let quota = client
        .get_send_quota()
        .send()
        .await
        .expect("GetSendQuota failed");

    assert!(quota.max24_hour_send() > 0.0, "Max24HourSend should be positive");
    assert!(quota.max_send_rate() > 0.0, "MaxSendRate should be positive");
}

#[tokio::test]
async fn test_get_send_statistics() {
    let port = port().await;
    let client = ses_client(port);

    client
        .get_send_statistics()
        .send()
        .await
        .expect("GetSendStatistics failed");
}
