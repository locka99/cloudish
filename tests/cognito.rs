//! Integration tests for the Cognito Identity Provider service.
//!
//! Run with: `cargo test --test cognito -- --nocapture`

use aws_sdk_cognitoidentityprovider::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::AttributeType,
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

fn cognito_client(port: u16) -> Client {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_cognitoidentityprovider::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    Client::from_conf(conf)
}

fn unique_pool_name() -> String {
    format!("test-pool-{}", Uuid::new_v4().simple())
}

// ── Drop guard ─────────────────────────────────────────────────────────────────

struct PoolGuard {
    client: Client,
    pool_id: String,
}

impl PoolGuard {
    fn new(client: Client, pool_id: impl Into<String>) -> Self {
        Self { client, pool_id: pool_id.into() }
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let pool_id = self.pool_id.clone();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let _ = client.delete_user_pool().user_pool_id(&pool_id).send().await;
                });
        });
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_list_delete_pool() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    // Create
    let create = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("create_user_pool failed");
    let pool = create.user_pool.unwrap();
    let pool_id = pool.id.unwrap();
    assert!(pool_id.starts_with("eu-west-1_"));

    let guard = PoolGuard::new(client.clone(), &pool_id);

    // List
    let list = client
        .list_user_pools()
        .max_results(10)
        .send()
        .await
        .expect("list_user_pools failed");
    let ids: Vec<String> = list.user_pools.unwrap_or_default().iter()
        .filter_map(|p| p.id.clone())
        .collect();
    assert!(ids.contains(&pool_id), "pool_id not found in list");

    // Describe
    let describe = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("describe_user_pool failed");
    assert_eq!(describe.user_pool.unwrap().name.unwrap(), pool_name);

    drop(guard);
}

#[tokio::test]
async fn test_create_pool_client() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    // Create client
    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("my-app")
        .send()
        .await
        .expect("create_user_pool_client failed");
    let user_pool_client = create_client.user_pool_client.unwrap();
    let client_id = user_pool_client.client_id.unwrap();
    assert_eq!(client_id.len(), 26);

    // List clients
    let list = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .expect("list_user_pool_clients failed");
    let client_ids: Vec<String> = list.user_pool_clients.unwrap_or_default().iter()
        .filter_map(|c| c.client_id.clone())
        .collect();
    assert!(client_ids.contains(&client_id));

    // Describe client
    let desc = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("describe_user_pool_client failed");
    assert_eq!(desc.user_pool_client.unwrap().client_name.unwrap(), "my-app");

    drop(guard);
}

#[tokio::test]
async fn test_admin_create_and_get_user() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let username = format!("user-{}", Uuid::new_v4().simple());

    // Create user
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("TempPass1!")
        .user_attributes(AttributeType::builder().name("email").value("test@example.com").build().unwrap())
        .send()
        .await
        .expect("admin_create_user failed");

    // Get user
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .send()
        .await
        .expect("admin_get_user failed");
    assert_eq!(get.username, username);

    let attrs = get.user_attributes.unwrap_or_default();
    let email = attrs.iter().find(|a| a.name == "email").map(|a| a.value.as_deref().unwrap_or(""));
    assert_eq!(email, Some("test@example.com"));

    // Delete user
    client
        .admin_delete_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .send()
        .await
        .expect("admin_delete_user failed");

    drop(guard);
}

#[tokio::test]
async fn test_user_password_auth() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("app")
        .send()
        .await
        .unwrap();
    let client_id = create_client.user_pool_client.unwrap().client_id.unwrap();

    let username = format!("user-{}", Uuid::new_v4().simple());

    // Create confirmed user
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("TempPass1!")
        .message_action(aws_sdk_cognitoidentityprovider::types::MessageActionType::Suppress)
        .send()
        .await
        .unwrap();

    // Set permanent password
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username(&username)
        .password("PermanentPass1!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    // Auth
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "PermanentPass1!")
        .send()
        .await
        .expect("initiate_auth failed");

    let result = auth.authentication_result.unwrap();
    assert!(result.access_token.is_some());
    assert!(result.id_token.is_some());
    assert!(result.refresh_token.is_some());

    let access_token = result.access_token.unwrap();

    // GetUser with access token
    let get_user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("get_user failed");
    assert_eq!(get_user.username, username);

    drop(guard);
}

#[tokio::test]
async fn test_force_change_password_challenge() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("app")
        .send()
        .await
        .unwrap();
    let client_id = create_client.user_pool_client.unwrap().client_id.unwrap();

    let username = format!("user-{}", Uuid::new_v4().simple());

    // Create user without SUPPRESS — should be FORCE_CHANGE_PASSWORD
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("TempPass1!")
        .send()
        .await
        .unwrap();

    // Auth should return NEW_PASSWORD_REQUIRED challenge
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "TempPass1!")
        .send()
        .await
        .expect("initiate_auth failed");

    assert!(auth.authentication_result.is_none(), "Should not have auth result on challenge");
    let challenge = auth.challenge_name.unwrap();
    assert_eq!(challenge.as_str(), "NEW_PASSWORD_REQUIRED");

    // Respond to challenge
    let respond = client
        .respond_to_auth_challenge()
        .client_id(&client_id)
        .challenge_name(aws_sdk_cognitoidentityprovider::types::ChallengeNameType::NewPasswordRequired)
        .challenge_responses("USERNAME", &username)
        .challenge_responses("NEW_PASSWORD", "NewSecurePass1!")
        .send()
        .await
        .expect("respond_to_auth_challenge failed");

    let result = respond.authentication_result.unwrap();
    assert!(result.access_token.is_some());
    assert!(result.refresh_token.is_some());

    drop(guard);
}

#[tokio::test]
async fn test_refresh_token_auth() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("app")
        .send()
        .await
        .unwrap();
    let client_id = create_client.user_pool_client.unwrap().client_id.unwrap();

    let username = format!("user-{}", Uuid::new_v4().simple());

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("TempPass1!")
        .message_action(aws_sdk_cognitoidentityprovider::types::MessageActionType::Suppress)
        .send()
        .await
        .unwrap();

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username(&username)
        .password("PermanentPass1!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    // Initial auth to get refresh token
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "PermanentPass1!")
        .send()
        .await
        .unwrap();

    let refresh_token = auth.authentication_result.unwrap().refresh_token.unwrap();

    // Refresh
    let refresh = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .client_id(&client_id)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await
        .expect("refresh token auth failed");

    let result = refresh.authentication_result.unwrap();
    assert!(result.access_token.is_some());
    assert!(result.id_token.is_some());
    // Refresh response does not include a new refresh token
    // (some implementations do, ours doesn't — either is acceptable)

    drop(guard);
}

#[tokio::test]
async fn test_signup_confirm() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("app")
        .send()
        .await
        .unwrap();
    let client_id = create_client.user_pool_client.unwrap().client_id.unwrap();

    let username = format!("user-{}", Uuid::new_v4().simple());

    // Sign up
    let signup = client
        .sign_up()
        .client_id(&client_id)
        .username(&username)
        .password("MyPass1!")
        .user_attributes(AttributeType::builder().name("email").value("signup@example.com").build().unwrap())
        .send()
        .await
        .expect("sign_up failed");
    assert!(!signup.user_confirmed);

    // Confirm with any code
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username(&username)
        .confirmation_code("123456")
        .send()
        .await
        .expect("confirm_sign_up failed");

    // Auth after confirmation
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "MyPass1!")
        .send()
        .await
        .expect("initiate_auth after confirm failed");

    assert!(auth.authentication_result.is_some());

    drop(guard);
}

#[tokio::test]
async fn test_admin_set_password() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let create_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("app")
        .send()
        .await
        .unwrap();
    let client_id = create_client.user_pool_client.unwrap().client_id.unwrap();

    let username = format!("user-{}", Uuid::new_v4().simple());

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("OldTemp1!")
        .send()
        .await
        .unwrap();

    // Set permanent password
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username(&username)
        .password("NewPermanent1!")
        .permanent(true)
        .send()
        .await
        .expect("admin_set_user_password failed");

    // Auth with new password (no challenge expected)
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "NewPermanent1!")
        .send()
        .await
        .expect("initiate_auth with new password failed");

    assert!(auth.authentication_result.is_some());

    drop(guard);
}

#[tokio::test]
async fn test_list_users() {
    let port = port().await;
    let client = cognito_client(port);
    let pool_name = unique_pool_name();

    let create = client.create_user_pool().pool_name(&pool_name).send().await.unwrap();
    let pool_id = create.user_pool.unwrap().id.unwrap();
    let guard = PoolGuard::new(client.clone(), &pool_id);

    let username1 = format!("user-a-{}", Uuid::new_v4().simple());
    let username2 = format!("user-b-{}", Uuid::new_v4().simple());

    for (uname, email) in [(&username1, "a@example.com"), (&username2, "b@example.com")] {
        client
            .admin_create_user()
            .user_pool_id(&pool_id)
            .username(uname)
            .temporary_password("TempPass1!")
            .user_attributes(AttributeType::builder().name("email").value(email).build().unwrap())
            .send()
            .await
            .unwrap();
    }

    // List all
    let list = client
        .list_users()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("list_users failed");
    let usernames: Vec<String> = list.users.unwrap_or_default().iter()
        .filter_map(|u| u.username.clone())
        .collect();
    assert!(usernames.contains(&username1));
    assert!(usernames.contains(&username2));

    // Filter by email
    let filtered = client
        .list_users()
        .user_pool_id(&pool_id)
        .filter(r#"email = "a@example.com""#)
        .send()
        .await
        .expect("list_users with filter failed");
    let filtered_names: Vec<String> = filtered.users.unwrap_or_default().iter()
        .filter_map(|u| u.username.clone())
        .collect();
    assert!(filtered_names.contains(&username1));
    assert!(!filtered_names.contains(&username2));

    drop(guard);
}
