//! Integration tests for the AppConfig service.
//!
//! Run with: `cargo test --test appconfig -- --nocapture`

use aws_sdk_appconfig::{
    Client as AppConfigClient,
    config::{BehaviorVersion, Credentials, Region},
};
use aws_sdk_appconfigdata::Client as AppConfigDataClient;
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
                cloudish::AppState::new_with_data_dir(&data_dir).await.unwrap(),
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

fn mgmt_client(port: u16) -> AppConfigClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_appconfig::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    AppConfigClient::from_conf(conf)
}

fn data_client(port: u16) -> AppConfigDataClient {
    let creds = aws_sdk_appconfigdata::config::Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_appconfigdata::config::Builder::new()
        .behavior_version(aws_sdk_appconfigdata::config::BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(aws_sdk_appconfigdata::config::Region::new("eu-west-1"))
        .build();
    AppConfigDataClient::from_conf(conf)
}

// ── Drop guard ─────────────────────────────────────────────────────────────────

/// Guard that deletes an AppConfig application on drop (even on panic).
/// Uses raw TCP to avoid tokio runtime conflicts — drop may run inside a tokio task.
struct AppGuard {
    port: u16,
    app_id: String,
}

impl Drop for AppGuard {
    fn drop(&mut self) {
        use std::io::Write;
        let addr = format!("127.0.0.1:{}", self.port);
        let request = format!(
            "DELETE /applications/{} HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            self.app_id, addr
        );
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(request.as_bytes());
            // Don't need to read the response.
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_raw_http_applications() {
    let port = port().await;
    let client = reqwest::Client::new();

    // Test POST /applications
    let resp = client
        .post(format!("http://127.0.0.1:{port}/applications"))
        .header("Content-Type", "application/json")
        .body(r#"{"Name":"test-raw","Description":""}"#)
        .send()
        .await
        .expect("POST /applications failed");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    println!("POST /applications → {status}: {body}");
    assert!(status.is_success(), "expected 2xx, got {status}: {body}");

    // Test GET /applications
    let resp2 = client
        .get(format!("http://127.0.0.1:{port}/applications"))
        .send()
        .await
        .expect("GET /applications failed");
    let status2 = resp2.status();
    let body2 = resp2.text().await.unwrap_or_default();
    println!("GET /applications → {status2}: {body2}");
    assert!(status2.is_success(), "expected 2xx, got {status2}: {body2}");
}

#[tokio::test]
async fn test_application_crud() {
    let port = port().await;
    let client = mgmt_client(port);
    let name = format!("app-{}", Uuid::new_v4());

    let created = client
        .create_application()
        .name(&name)
        .description("test application")
        .send()
        .await
        .expect("CreateApplication failed");

    let app_id = created.id().expect("missing Id");
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    assert_eq!(created.name().unwrap_or(""), name);

    // Get
    let got = client
        .get_application()
        .application_id(app_id)
        .send()
        .await
        .expect("GetApplication failed");
    assert_eq!(got.id().unwrap_or(""), app_id);

    // Update
    let updated = client
        .update_application()
        .application_id(app_id)
        .description("updated description")
        .send()
        .await
        .expect("UpdateApplication failed");
    assert_eq!(updated.description().unwrap_or(""), "updated description");

    // List
    let list = client.list_applications().send().await.expect("ListApplications failed");
    assert!(
        list.items().iter().any(|a| a.id().unwrap_or("") == app_id),
        "app not in list"
    );
}

#[tokio::test]
async fn test_environment_crud() {
    let port = port().await;
    let client = mgmt_client(port);
    let app_name = format!("app-env-{}", Uuid::new_v4());

    let app = client.create_application().name(&app_name).send().await.expect("create app");
    let app_id = app.id().unwrap();
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    let env = client
        .create_environment()
        .application_id(app_id)
        .name("production")
        .send()
        .await
        .expect("CreateEnvironment failed");

    let env_id = env.id().expect("missing env Id");
    assert_eq!(env.name().unwrap_or(""), "production");

    let got = client
        .get_environment()
        .application_id(app_id)
        .environment_id(env_id)
        .send()
        .await
        .expect("GetEnvironment failed");
    assert_eq!(got.id().unwrap_or(""), env_id);

    client
        .update_environment()
        .application_id(app_id)
        .environment_id(env_id)
        .description("prod env")
        .send()
        .await
        .expect("UpdateEnvironment failed");

    let list = client
        .list_environments()
        .application_id(app_id)
        .send()
        .await
        .expect("ListEnvironments failed");
    assert!(list.items().iter().any(|e| e.id().unwrap_or("") == env_id));

    client
        .delete_environment()
        .application_id(app_id)
        .environment_id(env_id)
        .send()
        .await
        .expect("DeleteEnvironment failed");
}

#[tokio::test]
async fn test_configuration_profile_crud() {
    let port = port().await;
    let client = mgmt_client(port);
    let app_name = format!("app-profile-{}", Uuid::new_v4());

    let app = client.create_application().name(&app_name).send().await.expect("create app");
    let app_id = app.id().unwrap();
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    let profile = client
        .create_configuration_profile()
        .application_id(app_id)
        .name("my-config")
        .location_uri("hosted")
        .send()
        .await
        .expect("CreateConfigurationProfile failed");

    let profile_id = profile.id().expect("missing profile Id");

    let got = client
        .get_configuration_profile()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .send()
        .await
        .expect("GetConfigurationProfile failed");
    assert_eq!(got.id().unwrap_or(""), profile_id);

    let list = client
        .list_configuration_profiles()
        .application_id(app_id)
        .send()
        .await
        .expect("ListConfigurationProfiles failed");
    assert!(list.items().iter().any(|p| p.id().unwrap_or("") == profile_id));

    client
        .delete_configuration_profile()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .send()
        .await
        .expect("DeleteConfigurationProfile failed");
}

#[tokio::test]
async fn test_hosted_configuration_versions() {
    let port = port().await;
    let client = mgmt_client(port);
    let app_name = format!("app-hcv-{}", Uuid::new_v4());

    let app = client.create_application().name(&app_name).send().await.expect("create app");
    let app_id = app.id().unwrap();
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    let profile = client
        .create_configuration_profile()
        .application_id(app_id)
        .name("config")
        .location_uri("hosted")
        .send()
        .await
        .expect("CreateConfigurationProfile failed");
    let profile_id = profile.id().unwrap();

    let config_content = r#"{"feature_enabled": true, "max_retries": 3}"#;

    let v1 = client
        .create_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .content(aws_sdk_appconfig::primitives::Blob::new(config_content.as_bytes()))
        .content_type("application/json")
        .send()
        .await
        .expect("CreateHostedConfigurationVersion failed");

    assert_eq!(v1.version_number(), 1);

    // Create a second version
    let config_v2 = r#"{"feature_enabled": false, "max_retries": 5}"#;
    let v2 = client
        .create_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .content(aws_sdk_appconfig::primitives::Blob::new(config_v2.as_bytes()))
        .content_type("application/json")
        .send()
        .await
        .expect("CreateHostedConfigurationVersion v2 failed");
    assert_eq!(v2.version_number(), 2);

    // List versions
    let list = client
        .list_hosted_configuration_versions()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .send()
        .await
        .expect("ListHostedConfigurationVersions failed");
    assert_eq!(list.items().len(), 2);

    // Get specific version
    let got = client
        .get_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .version_number(1)
        .send()
        .await
        .expect("GetHostedConfigurationVersion failed");
    let body = got.content.expect("missing content blob").into_inner();
    assert_eq!(std::str::from_utf8(&body).unwrap(), config_content);

    // Delete a version
    client
        .delete_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .version_number(1)
        .send()
        .await
        .expect("DeleteHostedConfigurationVersion failed");

    let list2 = client
        .list_hosted_configuration_versions()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .send()
        .await
        .expect("ListHostedConfigurationVersions after delete failed");
    assert_eq!(list2.items().len(), 1);
}

#[tokio::test]
async fn test_deployment_strategies() {
    let port = port().await;
    let client = mgmt_client(port);

    // Built-in strategies should be in the list
    let list = client
        .list_deployment_strategies()
        .send()
        .await
        .expect("ListDeploymentStrategies failed");
    let names: Vec<&str> = list.items().iter().filter_map(|s| s.name()).collect();
    assert!(names.contains(&"AppConfig.AllAtOnce"), "missing built-in strategy");

    // Get built-in
    let got = client
        .get_deployment_strategy()
        .deployment_strategy_id("AppConfig.AllAtOnce")
        .send()
        .await
        .expect("GetDeploymentStrategy failed");
    assert_eq!(got.name().unwrap_or(""), "AppConfig.AllAtOnce");
    assert_eq!(got.growth_factor(), Some(100.0f32));

    // Create custom strategy
    let custom = client
        .create_deployment_strategy()
        .name(&format!("custom-{}", Uuid::new_v4()))
        .deployment_duration_in_minutes(0)
        .growth_factor(100.0)
        .replicate_to(aws_sdk_appconfig::types::ReplicateTo::None)
        .send()
        .await
        .expect("CreateDeploymentStrategy failed");
    assert!(custom.id().is_some(), "missing strategy Id");
}

#[tokio::test]
async fn test_deployment_lifecycle() {
    let port = port().await;
    let client = mgmt_client(port);
    let app_name = format!("app-deploy-{}", Uuid::new_v4());

    let app = client.create_application().name(&app_name).send().await.expect("create app");
    let app_id = app.id().unwrap();
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    let env = client
        .create_environment()
        .application_id(app_id)
        .name("staging")
        .send()
        .await
        .expect("create env");
    let env_id = env.id().unwrap();

    let profile = client
        .create_configuration_profile()
        .application_id(app_id)
        .name("my-config")
        .location_uri("hosted")
        .send()
        .await
        .expect("create profile");
    let profile_id = profile.id().unwrap();

    client
        .create_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .content(aws_sdk_appconfig::primitives::Blob::new(b"{\"key\": \"value\"}"))
        .content_type("application/json")
        .send()
        .await
        .expect("create hcv");

    let deployment = client
        .start_deployment()
        .application_id(app_id)
        .environment_id(env_id)
        .deployment_strategy_id("AppConfig.AllAtOnce")
        .configuration_profile_id(profile_id)
        .configuration_version("1")
        .send()
        .await
        .expect("StartDeployment failed");

    assert_eq!(
        deployment.state(),
        Some(&aws_sdk_appconfig::types::DeploymentState::Complete)
    );
    assert_eq!(deployment.deployment_number(), 1);

    let got = client
        .get_deployment()
        .application_id(app_id)
        .environment_id(env_id)
        .deployment_number(1)
        .send()
        .await
        .expect("GetDeployment failed");
    assert_eq!(got.deployment_number(), 1);

    let list = client
        .list_deployments()
        .application_id(app_id)
        .environment_id(env_id)
        .send()
        .await
        .expect("ListDeployments failed");
    assert_eq!(list.items().len(), 1);
}

#[tokio::test]
async fn test_configuration_session_and_get_latest() {
    let port = port().await;
    let mgmt = mgmt_client(port);
    let data = data_client(port);
    let app_name = format!("app-session-{}", Uuid::new_v4());

    let app = mgmt.create_application().name(&app_name).send().await.expect("create app");
    let app_id = app.id().unwrap();
    let _guard = AppGuard { port, app_id: app_id.to_string() };

    let env = mgmt
        .create_environment()
        .application_id(app_id)
        .name("prod")
        .send()
        .await
        .expect("create env");
    let env_id = env.id().unwrap();

    let profile = mgmt
        .create_configuration_profile()
        .application_id(app_id)
        .name("flags")
        .location_uri("hosted")
        .send()
        .await
        .expect("create profile");
    let profile_id = profile.id().unwrap();

    let config_json = r#"{"dark_mode": true}"#;
    mgmt.create_hosted_configuration_version()
        .application_id(app_id)
        .configuration_profile_id(profile_id)
        .content(aws_sdk_appconfig::primitives::Blob::new(config_json.as_bytes()))
        .content_type("application/json")
        .send()
        .await
        .expect("create hcv");

    mgmt.start_deployment()
        .application_id(app_id)
        .environment_id(env_id)
        .deployment_strategy_id("AppConfig.AllAtOnce")
        .configuration_profile_id(profile_id)
        .configuration_version("1")
        .send()
        .await
        .expect("start deployment");

    // Start a configuration session via the data plane
    let session = data
        .start_configuration_session()
        .application_identifier(app_id)
        .environment_identifier(env_id)
        .configuration_profile_identifier(profile_id)
        .send()
        .await
        .expect("StartConfigurationSession failed");

    let token = session.initial_configuration_token().expect("missing token");

    // First poll — should return the full config
    let resp = data
        .get_latest_configuration()
        .configuration_token(token)
        .send()
        .await
        .expect("GetLatestConfiguration failed");

    let next_token = resp.next_poll_configuration_token().expect("missing next token").to_string();
    let body = resp.configuration.expect("missing configuration blob").into_inner();
    assert!(
        !body.is_empty(),
        "expected config content on first poll, got empty body"
    );
    assert_eq!(std::str::from_utf8(&body).unwrap(), config_json);

    // Second poll — config unchanged, should get empty body
    let resp2 = data
        .get_latest_configuration()
        .configuration_token(next_token)
        .send()
        .await
        .expect("GetLatestConfiguration second poll failed");

    // When config is unchanged the SDK returns None for configuration (empty body).
    let body2 = resp2.configuration.map(|b| b.into_inner()).unwrap_or_default();
    assert!(body2.is_empty(), "expected empty body on second poll (no change)");
}
