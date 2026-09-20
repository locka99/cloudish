use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_cognitoidentityprovider::{
    config::{BehaviorVersion, Credentials, Region},
    Client as CognitoClient,
    types::{AuthFlowType, MessageActionType},
};
use aws_sdk_dynamodb::{
    Client as DynamoClient,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
};
use axum::{
    extract::{Json, State},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

// ── Constants ─────────────────────────────────────────────────────────────────

const CLOUDISH_ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "eu-west-1";
const USER_POOL_NAME: &str = "tenant-demo";
const APP_CLIENT_NAME: &str = "tenant-demo-client";
const USER_CONFIG_TABLE: &str = "user-config";
const TENANT_CONFIG_TABLE: &str = "tenant-config";
const NOTICES_TABLE: &str = "notices";

// ── Shared state ──────────────────────────────────────────────────────────────

struct AppState {
    cognito: CognitoClient,
    dynamodb: DynamoClient,
    client_id: String,
}

// ── API types ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    access_token: String,
    username: String,
    tenant_name: String,
}

#[derive(Deserialize)]
struct AddNoticeRequest {
    message: String,
}

#[derive(Serialize)]
struct Notice {
    id: String,
    author: String,
    message: String,
    created_at: String,
}

// ── Error type ────────────────────────────────────────────────────────────────

enum AppError {
    Unauthorized(String),
    NotFound(String),
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            AppError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
            AppError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            AppError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let creds = Credentials::new("test", "test", None, None, "cloudish");

    let cognito_conf = aws_sdk_cognitoidentityprovider::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(CLOUDISH_ENDPOINT)
        .credentials_provider(creds.clone())
        .region(Region::new(REGION))
        .build();
    let cognito = CognitoClient::from_conf(cognito_conf);

    let dynamo_conf = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(CLOUDISH_ENDPOINT)
        .credentials_provider(creds)
        .region(Region::new(REGION))
        .build();
    let dynamodb = DynamoClient::from_conf(dynamo_conf);

    let (_user_pool_id, client_id) = setup_cognito(&cognito).await;
    setup_dynamodb(&dynamodb).await;
    seed_data(&dynamodb).await;

    let state = Arc::new(AppState {
        cognito,
        dynamodb,
        client_id,
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/notices", get(list_notices).post(add_notice))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await.unwrap();
    tracing::info!("Tenant-demo backend listening on http://localhost:3002");
    axum::serve(listener, app).await.unwrap();
}

// ── Cognito setup ─────────────────────────────────────────────────────────────

async fn setup_cognito(cognito: &CognitoClient) -> (String, String) {
    let user_pool_id = find_or_create_user_pool(cognito).await;
    let client_id = find_or_create_app_client(cognito, &user_pool_id).await;

    for username in &["fred", "bob"] {
        create_user(cognito, &user_pool_id, username, "Password1!").await;
    }

    tracing::info!(
        "Cognito: pool '{}' ready (pool_id={}, client_id={})",
        USER_POOL_NAME,
        user_pool_id,
        client_id
    );
    (user_pool_id, client_id)
}

async fn find_or_create_user_pool(cognito: &CognitoClient) -> String {
    let resp = cognito
        .list_user_pools()
        .max_results(60)
        .send()
        .await
        .expect("Failed to list user pools");

    if let Some(pool) = resp
        .user_pools()
        .iter()
        .find(|p| p.name() == Some(USER_POOL_NAME))
    {
        let id = pool.id().unwrap_or_default().to_string();
        tracing::info!("Cognito: found existing pool '{}' (id={})", USER_POOL_NAME, id);
        return id;
    }

    let result = cognito
        .create_user_pool()
        .pool_name(USER_POOL_NAME)
        .send()
        .await
        .expect("Failed to create user pool");

    let id = result
        .user_pool()
        .and_then(|p| p.id())
        .unwrap_or_default()
        .to_string();

    tracing::info!("Cognito: created pool '{}' (id={})", USER_POOL_NAME, id);
    id
}

async fn find_or_create_app_client(cognito: &CognitoClient, user_pool_id: &str) -> String {
    let resp = cognito
        .list_user_pool_clients()
        .user_pool_id(user_pool_id)
        .max_results(60)
        .send()
        .await
        .expect("Failed to list user pool clients");

    if let Some(client) = resp
        .user_pool_clients()
        .iter()
        .find(|c| c.client_name() == Some(APP_CLIENT_NAME))
    {
        let id = client.client_id().unwrap_or_default().to_string();
        tracing::info!(
            "Cognito: found existing client '{}' (id={})",
            APP_CLIENT_NAME,
            id
        );
        return id;
    }

    let result = cognito
        .create_user_pool_client()
        .user_pool_id(user_pool_id)
        .client_name(APP_CLIENT_NAME)
        .send()
        .await
        .expect("Failed to create user pool client");

    let id = result
        .user_pool_client()
        .and_then(|c| c.client_id())
        .unwrap_or_default()
        .to_string();

    tracing::info!(
        "Cognito: created client '{}' (id={})",
        APP_CLIENT_NAME,
        id
    );
    id
}

async fn create_user(
    cognito: &CognitoClient,
    user_pool_id: &str,
    username: &str,
    password: &str,
) {
    let create_result = cognito
        .admin_create_user()
        .user_pool_id(user_pool_id)
        .username(username)
        .message_action(MessageActionType::Suppress)
        .temporary_password(password)
        .send()
        .await;

    match create_result {
        Ok(_) => tracing::info!("Cognito: created user '{}'", username),
        Err(e) if e.to_string().contains("UsernameExists") => {
            tracing::info!("Cognito: user '{}' already exists", username);
        }
        Err(e) => tracing::warn!("Cognito: create_user warning for '{}': {}", username, e),
    }

    // Move user to CONFIRMED state with a permanent password
    let set_result = cognito
        .admin_set_user_password()
        .user_pool_id(user_pool_id)
        .username(username)
        .password(password)
        .permanent(true)
        .send()
        .await;

    match set_result {
        Ok(_) => tracing::info!("Cognito: confirmed user '{}'", username),
        Err(e) => tracing::warn!(
            "Cognito: set_password warning for '{}': {}",
            username,
            e
        ),
    }
}

// ── DynamoDB setup ────────────────────────────────────────────────────────────

async fn setup_dynamodb(dynamo: &DynamoClient) {
    // user-config: PK = username
    create_table(dynamo, USER_CONFIG_TABLE, "username", None).await;
    // tenant-config: PK = tenant_id
    create_table(dynamo, TENANT_CONFIG_TABLE, "tenant_id", None).await;
    // notices: PK = tenant_id, SK = notice_id
    create_table(dynamo, NOTICES_TABLE, "tenant_id", Some("notice_id")).await;
}

async fn create_table(
    dynamo: &DynamoClient,
    table_name: &str,
    pk: &str,
    sk: Option<&str>,
) {
    let mut attr_defs = vec![AttributeDefinition::builder()
        .attribute_name(pk)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap()];

    let mut key_schema = vec![KeySchemaElement::builder()
        .attribute_name(pk)
        .key_type(KeyType::Hash)
        .build()
        .unwrap()];

    if let Some(sk_name) = sk {
        attr_defs.push(
            AttributeDefinition::builder()
                .attribute_name(sk_name)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        );
        key_schema.push(
            KeySchemaElement::builder()
                .attribute_name(sk_name)
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        );
    }

    let result = dynamo
        .create_table()
        .table_name(table_name)
        .set_attribute_definitions(Some(attr_defs))
        .set_key_schema(Some(key_schema))
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    match result {
        Ok(_) => tracing::info!("DynamoDB: created table '{}'", table_name),
        Err(e)
            if e.to_string().contains("ResourceInUse")
                || e.to_string().contains("TableAlreadyExists") =>
        {
            tracing::info!("DynamoDB: table '{}' already exists", table_name)
        }
        Err(e) => tracing::warn!(
            "DynamoDB: create_table warning for '{}': {}",
            table_name,
            e
        ),
    }
}

async fn seed_data(dynamo: &DynamoClient) {
    // Map users to tenants
    let users = [("fred", "tenant1"), ("bob", "tenant2")];
    for (username, tenant_id) in &users {
        let _ = dynamo
            .put_item()
            .table_name(USER_CONFIG_TABLE)
            .item("username", AttributeValue::S(username.to_string()))
            .item("tenant_id", AttributeValue::S(tenant_id.to_string()))
            .send()
            .await;
    }
    tracing::info!("DynamoDB: seeded {}", USER_CONFIG_TABLE);

    // Tenant metadata and (example) DB connection strings
    let tenants = [
        (
            "tenant1",
            "Acme Corp",
            "postgresql://acme:secret@localhost/acme_db",
        ),
        (
            "tenant2",
            "Globex Corp",
            "postgresql://globex:secret@localhost/globex_db",
        ),
    ];
    for (tenant_id, tenant_name, db_connection) in &tenants {
        let _ = dynamo
            .put_item()
            .table_name(TENANT_CONFIG_TABLE)
            .item("tenant_id", AttributeValue::S(tenant_id.to_string()))
            .item("tenant_name", AttributeValue::S(tenant_name.to_string()))
            .item(
                "db_connection",
                AttributeValue::S(db_connection.to_string()),
            )
            .send()
            .await;
    }
    tracing::info!("DynamoDB: seeded {}", TENANT_CONFIG_TABLE);
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(|t| t.to_string())
}

async fn get_username_from_token(
    cognito: &CognitoClient,
    token: &str,
) -> Result<String, AppError> {
    let resp = cognito
        .get_user()
        .access_token(token)
        .send()
        .await
        .map_err(|e| AppError::Unauthorized(format!("Invalid or expired token: {}", e)))?;

    Ok(resp.username().to_string())
}

async fn get_tenant_id(dynamo: &DynamoClient, username: &str) -> Result<String, AppError> {
    let resp = dynamo
        .get_item()
        .table_name(USER_CONFIG_TABLE)
        .key("username", AttributeValue::S(username.to_string()))
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("DynamoDB error: {}", e)))?;

    resp.item()
        .and_then(|item| item.get("tenant_id"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| AppError::NotFound(format!("No tenant configured for user '{}'", username)))
}

async fn get_tenant_name(dynamo: &DynamoClient, tenant_id: &str) -> Result<String, AppError> {
    let resp = dynamo
        .get_item()
        .table_name(TENANT_CONFIG_TABLE)
        .key("tenant_id", AttributeValue::S(tenant_id.to_string()))
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("DynamoDB error: {}", e)))?;

    resp.item()
        .and_then(|item| item.get("tenant_name"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| AppError::NotFound(format!("No config for tenant '{}'", tenant_id)))
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `POST /api/auth/login` — authenticate with Cognito, return an access token
/// and the name of the tenant the user belongs to.
async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<axum::Json<LoginResponse>, AppError> {
    let auth = state
        .cognito
        .initiate_auth()
        .auth_flow(AuthFlowType::UserPasswordAuth)
        .client_id(&state.client_id)
        .auth_parameters("USERNAME", &req.username)
        .auth_parameters("PASSWORD", &req.password)
        .send()
        .await
        .map_err(|e| AppError::Unauthorized(format!("Authentication failed: {}", e)))?;

    let access_token = auth
        .authentication_result()
        .and_then(|r| r.access_token())
        .ok_or_else(|| AppError::Internal("No access token in auth response".to_string()))?
        .to_string();

    let tenant_id = get_tenant_id(&state.dynamodb, &req.username).await?;
    let tenant_name = get_tenant_name(&state.dynamodb, &tenant_id).await?;

    tracing::info!(
        "Login: '{}' authenticated for tenant '{}'",
        req.username,
        tenant_id
    );

    Ok(axum::Json(LoginResponse {
        access_token,
        username: req.username,
        tenant_name,
    }))
}

/// `POST /api/auth/logout` — invalidate the Cognito session (best-effort).
async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> StatusCode {
    if let Some(token) = extract_bearer_token(&headers) {
        let _ = state
            .cognito
            .global_sign_out()
            .access_token(&token)
            .send()
            .await;
    }
    StatusCode::NO_CONTENT
}

/// `GET /api/notices` — return all notices for the authenticated user's tenant,
/// newest first.
async fn list_notices(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<axum::Json<Vec<Notice>>, AppError> {
    let token = extract_bearer_token(&headers)
        .ok_or_else(|| AppError::Unauthorized("Missing Authorization header".to_string()))?;

    let username = get_username_from_token(&state.cognito, &token).await?;
    let tenant_id = get_tenant_id(&state.dynamodb, &username).await?;

    let resp = state
        .dynamodb
        .query()
        .table_name(NOTICES_TABLE)
        .key_condition_expression("tenant_id = :tid")
        .expression_attribute_values(":tid", AttributeValue::S(tenant_id.clone()))
        .scan_index_forward(false) // newest first
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("DynamoDB query failed: {}", e)))?;

    let notices = resp
        .items()
        .iter()
        .map(|item| {
            let get_s = |k: &str| -> String {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            };
            Notice {
                id: get_s("notice_id"),
                author: get_s("author"),
                message: get_s("message"),
                created_at: get_s("created_at"),
            }
        })
        .collect();

    Ok(axum::Json(notices))
}

/// `POST /api/notices` — add a notice to the authenticated user's tenant board.
async fn add_notice(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AddNoticeRequest>,
) -> Result<axum::Json<Notice>, AppError> {
    if req.message.trim().is_empty() {
        return Err(AppError::Internal("Message cannot be empty".to_string()));
    }

    let token = extract_bearer_token(&headers)
        .ok_or_else(|| AppError::Unauthorized("Missing Authorization header".to_string()))?;

    let username = get_username_from_token(&state.cognito, &token).await?;
    let tenant_id = get_tenant_id(&state.dynamodb, &username).await?;

    // Use a zero-padded millis timestamp prefix so lexicographic order == time order.
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let notice_id = format!("{:015}_{}", millis, Uuid::new_v4());
    let created_at = chrono::Utc::now().to_rfc3339();

    state
        .dynamodb
        .put_item()
        .table_name(NOTICES_TABLE)
        .item("tenant_id", AttributeValue::S(tenant_id.clone()))
        .item("notice_id", AttributeValue::S(notice_id.clone()))
        .item("author", AttributeValue::S(username.clone()))
        .item("message", AttributeValue::S(req.message.clone()))
        .item("created_at", AttributeValue::S(created_at.clone()))
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("Failed to save notice: {}", e)))?;

    tracing::info!(
        "Notice posted by '{}' for tenant '{}'",
        username,
        tenant_id
    );

    Ok(axum::Json(Notice {
        id: notice_id,
        author: username,
        message: req.message,
        created_at,
    }))
}
