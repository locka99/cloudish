//! Integration tests for the IoT control plane service.
//!
//! Run with: `cargo test --test iot -- --nocapture`

use aws_sdk_iot::{
    Client as IotClient,
    config::{BehaviorVersion, Credentials, Region},
};
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

fn iot_client(port: u16) -> IotClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_iot::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    IotClient::from_conf(conf)
}

// ── Drop guards ───────────────────────────────────────────────────────────────

struct ThingGuard {
    port: u16,
    thing_name: String,
}

impl Drop for ThingGuard {
    fn drop(&mut self) {
        use std::io::{Read, Write};
        let addr = format!("127.0.0.1:{}", self.port);
        let req = format!(
            "DELETE /things/{} HTTP/1.0\r\nHost: {addr}\r\n\r\n",
            self.thing_name
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(req.as_bytes());
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
        }
    }
}

struct CertGuard {
    port: u16,
    cert_id: String,
}

impl Drop for CertGuard {
    fn drop(&mut self) {
        use std::io::{Read, Write};
        let addr = format!("127.0.0.1:{}", self.port);
        let req = format!(
            "DELETE /certificates/{} HTTP/1.0\r\nHost: {addr}\r\n\r\n",
            self.cert_id
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(req.as_bytes());
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
        }
    }
}

struct PolicyGuard {
    port: u16,
    policy_name: String,
}

impl Drop for PolicyGuard {
    fn drop(&mut self) {
        use std::io::{Read, Write};
        let addr = format!("127.0.0.1:{}", self.port);
        let req = format!(
            "DELETE /policies/{} HTTP/1.0\r\nHost: {addr}\r\n\r\n",
            self.policy_name
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(req.as_bytes());
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_describe_thing() {
    let port = port().await;
    let client = iot_client(port);

    let name = format!("test-thing-{}", Uuid::new_v4().simple());
    let _guard = ThingGuard { port, thing_name: name.clone() };

    // CreateThing
    let create = client
        .create_thing()
        .thing_name(&name)
        .send()
        .await
        .unwrap();

    assert_eq!(create.thing_name().unwrap(), &name);
    let arn = create.thing_arn().unwrap();
    assert!(arn.contains(&name), "ARN should contain thing name: {arn}");
    assert!(arn.starts_with("arn:aws:iot:"), "ARN format wrong: {arn}");
    assert!(create.thing_id().is_some(), "thingId should be set");

    // DescribeThing
    let desc = client
        .describe_thing()
        .thing_name(&name)
        .send()
        .await
        .unwrap();

    assert_eq!(desc.thing_name().unwrap(), &name);
    assert_eq!(desc.thing_arn().unwrap(), arn);
    assert_eq!(desc.version(), 1);
}

#[tokio::test]
async fn test_list_things() {
    let port = port().await;
    let client = iot_client(port);

    let name1 = format!("test-list-a-{}", Uuid::new_v4().simple());
    let name2 = format!("test-list-b-{}", Uuid::new_v4().simple());

    let _guard1 = ThingGuard { port, thing_name: name1.clone() };
    let _guard2 = ThingGuard { port, thing_name: name2.clone() };

    client.create_thing().thing_name(&name1).send().await.unwrap();
    client.create_thing().thing_name(&name2).send().await.unwrap();

    let list = client.list_things().send().await.unwrap();
    let names: Vec<&str> = list
        .things()
        .iter()
        .filter_map(|t| t.thing_name())
        .collect();

    assert!(
        names.contains(&name1.as_str()),
        "{name1} not in list: {names:?}"
    );
    assert!(
        names.contains(&name2.as_str()),
        "{name2} not in list: {names:?}"
    );
}

#[tokio::test]
async fn test_delete_thing() {
    let port = port().await;
    let client = iot_client(port);

    let name = format!("test-del-{}", Uuid::new_v4().simple());

    client.create_thing().thing_name(&name).send().await.unwrap();
    client.delete_thing().thing_name(&name).send().await.unwrap();

    // DescribeThing after delete should return ResourceNotFoundException
    let err = client
        .describe_thing()
        .thing_name(&name)
        .send()
        .await
        .unwrap_err();

    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("ResourceNotFoundException") || err_str.contains("not found"),
        "expected not-found error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_create_keys_and_certificate() {
    let port = port().await;
    let client = iot_client(port);

    let resp = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .unwrap();

    let cert_id = resp.certificate_id().unwrap();
    // AWS cert ID is 64-char lowercase hex
    assert_eq!(cert_id.len(), 64, "cert_id should be 64 chars, got: {cert_id}");
    assert!(
        cert_id.chars().all(|c| c.is_ascii_hexdigit()),
        "cert_id should be hex: {cert_id}"
    );

    let cert_arn = resp.certificate_arn().unwrap();
    assert!(cert_arn.contains(cert_id), "ARN should contain cert ID");

    let cert_pem = resp.certificate_pem().unwrap();
    assert!(cert_pem.contains("-----BEGIN CERTIFICATE-----"), "cert PEM missing header");

    let key_pair = resp.key_pair().unwrap();
    assert!(
        key_pair.public_key().unwrap().contains("-----BEGIN PUBLIC KEY-----"),
        "public key missing header"
    );
    assert!(
        key_pair.private_key().unwrap().contains("-----BEGIN"),
        "private key missing header"
    );

    // Clean up
    let _guard = CertGuard { port, cert_id: cert_id.to_string() };
}

#[tokio::test]
async fn test_describe_certificate() {
    let port = port().await;
    let client = iot_client(port);

    let create = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .unwrap();
    let cert_id = create.certificate_id().unwrap().to_string();
    let _guard = CertGuard { port, cert_id: cert_id.clone() };

    let desc = client
        .describe_certificate()
        .certificate_id(&cert_id)
        .send()
        .await
        .unwrap();

    let cert_desc = desc.certificate_description().unwrap();
    assert_eq!(cert_desc.certificate_id().unwrap(), cert_id);
    assert_eq!(cert_desc.status().unwrap().as_str(), "ACTIVE");
    assert!(cert_desc.certificate_arn().is_some());
}

#[tokio::test]
async fn test_update_certificate() {
    let port = port().await;
    let client = iot_client(port);

    let create = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .unwrap();
    let cert_id = create.certificate_id().unwrap().to_string();
    let _guard = CertGuard { port, cert_id: cert_id.clone() };

    // Update to INACTIVE
    client
        .update_certificate()
        .certificate_id(&cert_id)
        .new_status(aws_sdk_iot::types::CertificateStatus::Inactive)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_certificate()
        .certificate_id(&cert_id)
        .send()
        .await
        .unwrap();

    let cert_desc = desc.certificate_description().unwrap();
    assert_eq!(cert_desc.status().unwrap().as_str(), "INACTIVE");
}

#[tokio::test]
async fn test_create_and_get_policy() {
    let port = port().await;
    let client = iot_client(port);

    let name = format!("test-policy-{}", Uuid::new_v4().simple());
    let _guard = PolicyGuard { port, policy_name: name.clone() };

    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}"#;

    let create = client
        .create_policy()
        .policy_name(&name)
        .policy_document(doc)
        .send()
        .await
        .unwrap();

    assert_eq!(create.policy_name().unwrap(), &name);
    let policy_arn = create.policy_arn().unwrap();
    assert!(policy_arn.contains(&name), "ARN should contain policy name");
    assert_eq!(create.policy_version_id().unwrap(), "1");

    let get = client.get_policy().policy_name(&name).send().await.unwrap();
    assert_eq!(get.policy_name().unwrap(), &name);
    assert_eq!(get.policy_arn().unwrap(), policy_arn);
    assert_eq!(get.default_version_id().unwrap(), "1");

    let retrieved_doc = get.policy_document().unwrap();
    assert!(
        retrieved_doc.contains("iot:*"),
        "policy document should contain 'iot:*'"
    );
}

#[tokio::test]
async fn test_list_policies() {
    let port = port().await;
    let client = iot_client(port);

    let name1 = format!("test-pol-a-{}", Uuid::new_v4().simple());
    let name2 = format!("test-pol-b-{}", Uuid::new_v4().simple());

    let _guard1 = PolicyGuard { port, policy_name: name1.clone() };
    let _guard2 = PolicyGuard { port, policy_name: name2.clone() };

    let doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    client
        .create_policy()
        .policy_name(&name1)
        .policy_document(doc)
        .send()
        .await
        .unwrap();
    client
        .create_policy()
        .policy_name(&name2)
        .policy_document(doc)
        .send()
        .await
        .unwrap();

    let list = client.list_policies().send().await.unwrap();
    let names: Vec<&str> = list
        .policies()
        .iter()
        .filter_map(|p| p.policy_name())
        .collect();

    assert!(
        names.contains(&name1.as_str()),
        "{name1} not in list: {names:?}"
    );
    assert!(
        names.contains(&name2.as_str()),
        "{name2} not in list: {names:?}"
    );
}

#[tokio::test]
async fn test_attach_policy_to_certificate() {
    let port = port().await;
    let client = iot_client(port);

    // Create certificate
    let cert_resp = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .unwrap();
    let cert_id = cert_resp.certificate_id().unwrap().to_string();
    let cert_arn = cert_resp.certificate_arn().unwrap().to_string();
    let _cert_guard = CertGuard { port, cert_id: cert_id.clone() };

    // Create policy
    let policy_name = format!("test-attach-pol-{}", Uuid::new_v4().simple());
    let _pol_guard = PolicyGuard { port, policy_name: policy_name.clone() };

    let doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    client
        .create_policy()
        .policy_name(&policy_name)
        .policy_document(doc)
        .send()
        .await
        .unwrap();

    // Attach policy to certificate
    client
        .attach_policy()
        .policy_name(&policy_name)
        .target(&cert_arn)
        .send()
        .await
        .unwrap();

    // List attached policies for cert ARN
    let list = client
        .list_attached_policies()
        .target(&cert_arn)
        .send()
        .await
        .unwrap();

    let attached_names: Vec<&str> = list
        .policies()
        .iter()
        .filter_map(|p| p.policy_name())
        .collect();

    assert!(
        attached_names.contains(&policy_name.as_str()),
        "{policy_name} not in attached policies: {attached_names:?}"
    );

    // Detach for cleanup
    client
        .detach_policy()
        .policy_name(&policy_name)
        .target(&cert_arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_attach_thing_principal() {
    let port = port().await;
    let client = iot_client(port);

    // Create thing
    let thing_name = format!("test-tp-thing-{}", Uuid::new_v4().simple());
    let _thing_guard = ThingGuard { port, thing_name: thing_name.clone() };
    client
        .create_thing()
        .thing_name(&thing_name)
        .send()
        .await
        .unwrap();

    // Create certificate to use as principal
    let cert_resp = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .unwrap();
    let cert_id = cert_resp.certificate_id().unwrap().to_string();
    let cert_arn = cert_resp.certificate_arn().unwrap().to_string();
    let _cert_guard = CertGuard { port, cert_id: cert_id.clone() };

    // Attach principal to thing
    client
        .attach_thing_principal()
        .thing_name(&thing_name)
        .principal(&cert_arn)
        .send()
        .await
        .unwrap();

    // List thing's principals
    let list = client
        .list_thing_principals()
        .thing_name(&thing_name)
        .send()
        .await
        .unwrap();

    let principals: Vec<&str> = list.principals().iter().map(|s| s.as_str()).collect();
    assert!(
        principals.contains(&cert_arn.as_str()),
        "{cert_arn} not in principals: {principals:?}"
    );

    // Detach for cleanup
    client
        .detach_thing_principal()
        .thing_name(&thing_name)
        .principal(&cert_arn)
        .send()
        .await
        .unwrap();
}
