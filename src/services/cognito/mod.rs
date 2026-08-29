//! Cognito Identity Provider emulator.
//!
//! Routing:
//!   POST /cognito/           — direct HTTP endpoint (for testing)
//!   POST /                   — top-level dispatcher (X-Amz-Target routing)
//!   GET  /cognito/:pool_id/.well-known/jwks.json — JWKS endpoint

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::services::AppState;
use crate::storage::Storage;

// ── Key management ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct CognitoKeys {
    pub private_pem: String,
    pub public_pem: String,
}

static KEYS: tokio::sync::OnceCell<Arc<CognitoKeys>> = tokio::sync::OnceCell::const_new();

async fn get_keys(storage: &Arc<crate::storage::file::FileStorage>) -> Arc<CognitoKeys> {
    KEYS.get_or_init(|| async {
        Arc::new(load_or_generate_keys(storage).await.expect("failed to load/generate Cognito RSA keypair"))
    })
    .await
    .clone()
}

async fn load_or_generate_keys(
    storage: &Arc<crate::storage::file::FileStorage>,
) -> anyhow::Result<CognitoKeys> {
    use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};

    // Try to load existing keys
    if let (Some(priv_bytes), Some(pub_bytes)) = (
        storage.get("keypair/private.pem").await?,
        storage.get("keypair/public.pem").await?,
    ) {
        let private_pem = String::from_utf8(priv_bytes)?;
        let public_pem = String::from_utf8(pub_bytes)?;
        return Ok(CognitoKeys { private_pem, public_pem });
    }

    // Generate new keypair
    tracing::info!("Generating new RSA-2048 keypair for Cognito JWT signing");
    let mut rng = rand::rngs::OsRng;
    let private_key = rsa::RsaPrivateKey::new(&mut rng, 2048)?;
    let private_pem = private_key.to_pkcs8_pem(LineEnding::LF)?.to_string();
    let public_pem = private_key.to_public_key().to_public_key_pem(LineEnding::LF)?;

    storage.put("keypair/private.pem", private_pem.as_bytes().to_vec()).await?;
    storage.put("keypair/public.pem", public_pem.as_bytes().to_vec()).await?;

    Ok(CognitoKeys { private_pem, public_pem })
}

// ── Data structures ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserPoolMeta {
    id: String,
    name: String,
    arn: String,
    created_at: f64,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserPoolClientMeta {
    client_id: String,
    client_name: String,
    pool_id: String,
    client_secret: Option<String>,
    created_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AttributeType {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserMeta {
    username: String,
    pool_id: String,
    password_hash: String,
    status: String,
    attributes: Vec<AttributeType>,
    enabled: bool,
    created_at: f64,
    updated_at: f64,
    sub: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RefreshTokenMeta {
    username: String,
    pool_id: String,
    client_id: String,
    expires_at: f64,
}

// ── JWT Claims ─────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    iss: String,
    client_id: String,
    token_use: String,
    username: String,
    scope: String,
    exp: u64,
    iat: u64,
    jti: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct IdClaims {
    sub: String,
    iss: String,
    aud: String,
    token_use: String,
    #[serde(rename = "cognito:username")]
    cognito_username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    exp: u64,
    iat: u64,
    jti: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaimsPartial {
    sub: String,
    username: String,
    client_id: String,
    token_use: String,
    iss: String,
    exp: u64,
    iat: u64,
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

fn generate_pool_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        .chars()
        .collect();
    let suffix: String = (0..8).map(|_| chars[rng.gen_range(0..chars.len())]).collect();
    format!("eu-west-1_{suffix}")
}

fn generate_client_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        .chars()
        .collect();
    (0..26).map(|_| chars[rng.gen_range(0..chars.len())]).collect()
}

fn pool_arn(pool_id: &str) -> String {
    format!("arn:aws:cognito-idp:eu-west-1:000000000000:userpool/{pool_id}")
}

fn pool_issuer(pool_id: &str) -> String {
    format!("https://cognito-idp.eu-west-1.amazonaws.com/{pool_id}")
}

// ── Storage helpers ────────────────────────────────────────────────────────────

async fn save_pool(storage: &Arc<crate::storage::file::FileStorage>, pool: &UserPoolMeta) -> anyhow::Result<()> {
    let key = format!("pools/{}/_meta.json", pool.id);
    let data = serde_json::to_vec(pool)?;
    storage.put(&key, data).await
}

async fn load_pool(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str) -> anyhow::Result<Option<UserPoolMeta>> {
    let key = format!("pools/{pool_id}/_meta.json");
    match storage.get(&key).await? {
        Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
        None => Ok(None),
    }
}

async fn save_client(storage: &Arc<crate::storage::file::FileStorage>, client: &UserPoolClientMeta) -> anyhow::Result<()> {
    let key = format!("pools/{}/clients/{}.json", client.pool_id, client.client_id);
    let data = serde_json::to_vec(client)?;
    storage.put(&key, data).await
}

async fn load_client(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str, client_id: &str) -> anyhow::Result<Option<UserPoolClientMeta>> {
    let key = format!("pools/{pool_id}/clients/{client_id}.json");
    match storage.get(&key).await? {
        Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
        None => Ok(None),
    }
}

async fn save_user(storage: &Arc<crate::storage::file::FileStorage>, user: &UserMeta) -> anyhow::Result<()> {
    let key = format!("pools/{}/users/{}.json", user.pool_id, user.username);
    let data = serde_json::to_vec(user)?;
    storage.put(&key, data).await
}

async fn load_user(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str, username: &str) -> anyhow::Result<Option<UserMeta>> {
    let key = format!("pools/{pool_id}/users/{username}.json");
    match storage.get(&key).await? {
        Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
        None => Ok(None),
    }
}

async fn save_refresh_token(storage: &Arc<crate::storage::file::FileStorage>, token: &str, meta: &RefreshTokenMeta) -> anyhow::Result<()> {
    let key = format!("pools/{}/refresh_tokens/{}.json", meta.pool_id, token);
    let data = serde_json::to_vec(meta)?;
    storage.put(&key, data).await
}

async fn load_refresh_token(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str, token: &str) -> anyhow::Result<Option<RefreshTokenMeta>> {
    let key = format!("pools/{pool_id}/refresh_tokens/{token}.json");
    match storage.get(&key).await? {
        Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
        None => Ok(None),
    }
}

async fn list_pools(storage: &Arc<crate::storage::file::FileStorage>) -> anyhow::Result<Vec<UserPoolMeta>> {
    let keys = storage.list("pools/").await?;
    let mut pools = Vec::new();
    for key in keys {
        if key.ends_with("/_meta.json") {
            if let Some(data) = storage.get(&key).await? {
                if let Ok(pool) = serde_json::from_slice::<UserPoolMeta>(&data) {
                    pools.push(pool);
                }
            }
        }
    }
    pools.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(pools)
}

async fn list_clients(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str) -> anyhow::Result<Vec<UserPoolClientMeta>> {
    let prefix = format!("pools/{pool_id}/clients/");
    let keys = storage.list(&prefix).await?;
    let mut clients = Vec::new();
    for key in keys {
        if key.ends_with(".json") {
            if let Some(data) = storage.get(&key).await? {
                if let Ok(client) = serde_json::from_slice::<UserPoolClientMeta>(&data) {
                    clients.push(client);
                }
            }
        }
    }
    clients.sort_by(|a, b| a.client_name.cmp(&b.client_name));
    Ok(clients)
}

async fn list_users(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str) -> anyhow::Result<Vec<UserMeta>> {
    let prefix = format!("pools/{pool_id}/users/");
    let keys = storage.list(&prefix).await?;
    let mut users = Vec::new();
    for key in keys {
        if key.ends_with(".json") {
            if let Some(data) = storage.get(&key).await? {
                if let Ok(user) = serde_json::from_slice::<UserMeta>(&data) {
                    users.push(user);
                }
            }
        }
    }
    users.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(users)
}

async fn delete_pool_data(storage: &Arc<crate::storage::file::FileStorage>, pool_id: &str) -> anyhow::Result<()> {
    let prefix = format!("pools/{pool_id}");
    let keys = storage.list(&prefix).await?;
    for key in keys {
        let _ = storage.delete(&key).await;
    }
    Ok(())
}

// ── JWT issuance ───────────────────────────────────────────────────────────────

fn issue_tokens(
    keys: &CognitoKeys,
    pool_id: &str,
    client_id: &str,
    user: &UserMeta,
) -> anyhow::Result<(String, String, String)> {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

    let now = now_secs() as u64;
    let exp = now + 3600;
    let iss = pool_issuer(pool_id);
    let encoding_key = EncodingKey::from_rsa_pem(keys.private_pem.as_bytes())?;
    let header = Header {
        alg: Algorithm::RS256,
        kid: Some("1".to_string()),
        ..Default::default()
    };

    let email = user
        .attributes
        .iter()
        .find(|a| a.name == "email")
        .map(|a| a.value.clone());

    // Access token
    let access_claims = AccessClaims {
        sub: user.sub.clone(),
        iss: iss.clone(),
        client_id: client_id.to_string(),
        token_use: "access".to_string(),
        username: user.username.clone(),
        scope: "aws.cognito.signin.user.admin".to_string(),
        exp,
        iat: now,
        jti: Uuid::new_v4().to_string(),
    };
    let access_token = encode(&header, &access_claims, &encoding_key)?;

    // ID token
    let id_claims = IdClaims {
        sub: user.sub.clone(),
        iss: iss.clone(),
        aud: client_id.to_string(),
        token_use: "id".to_string(),
        cognito_username: user.username.clone(),
        email,
        exp,
        iat: now,
        jti: Uuid::new_v4().to_string(),
    };
    let id_token = encode(&header, &id_claims, &encoding_key)?;

    // Refresh token (UUID)
    let refresh_token = Uuid::new_v4().to_string();

    Ok((access_token, id_token, refresh_token))
}

fn validate_access_token(keys: &CognitoKeys, token: &str) -> anyhow::Result<AccessClaimsPartial> {
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

    let decoding_key = DecodingKey::from_rsa_pem(keys.public_pem.as_bytes())?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[] as &[String]);
    validation.validate_aud = false;
    let token_data = decode::<AccessClaimsPartial>(token, &decoding_key, &validation)?;
    Ok(token_data.claims)
}

// ── Error helpers ──────────────────────────────────────────────────────────────

fn cognito_error(status: StatusCode, error_type: &str, message: &str) -> (StatusCode, axum::Json<Value>) {
    (
        status,
        axum::Json(json!({
            "__type": error_type,
            "message": message,
        })),
    )
}

fn not_found(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}

fn user_not_found(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::BAD_REQUEST, "UserNotFoundException", msg)
}

fn username_exists(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::BAD_REQUEST, "UsernameExistsException", msg)
}

fn not_authorized(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::BAD_REQUEST, "NotAuthorizedException", msg)
}

fn invalid_password(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::BAD_REQUEST, "InvalidPasswordException", msg)
}

fn internal_err(msg: &str) -> (StatusCode, axum::Json<Value>) {
    cognito_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalErrorException", msg)
}

fn user_pool_to_json(pool: &UserPoolMeta) -> Value {
    json!({
        "Id": pool.id,
        "Name": pool.name,
        "Arn": pool.arn,
        "Status": pool.status,
        "CreationDate": pool.created_at,
        "LastModifiedDate": pool.created_at,
    })
}

fn user_to_json(user: &UserMeta) -> Value {
    let attrs: Vec<Value> = user.attributes.iter().map(|a| json!({"Name": a.name, "Value": a.value})).collect();
    json!({
        "Username": user.username,
        "Attributes": attrs,
        "UserStatus": user.status,
        "Enabled": user.enabled,
        "UserCreateDate": user.created_at,
        "UserLastModifiedDate": user.updated_at,
    })
}

// ── JWKS handler ───────────────────────────────────────────────────────────────

async fn jwks_handler(
    Path(_pool_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // pool_id is in the path but we use a single keypair for all pools
    let keys = get_keys(&state.cognito).await;

    // Parse the public key to extract n and e
    match build_jwks(&keys.public_pem) {
        Ok(jwks) => (StatusCode::OK, axum::Json(jwks)).into_response(),
        Err(e) => {
            tracing::error!("Failed to build JWKS: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to build JWKS").into_response()
        }
    }
}

fn build_jwks(public_pem: &str) -> anyhow::Result<Value> {
    use rsa::pkcs8::DecodePublicKey;
    use rsa::traits::PublicKeyParts;

    let public_key = rsa::RsaPublicKey::from_public_key_pem(public_pem)?;
    let n_bytes = public_key.n().to_bytes_be();
    let e_bytes = public_key.e().to_bytes_be();

    let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&n_bytes);
    let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&e_bytes);

    Ok(json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": "1",
            "n": n,
            "e": e,
        }]
    }))
}

// ── Router ─────────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/cognito/", post(dispatch_handler))
        .route("/cognito/{pool_id}/.well-known/jwks.json", get(jwks_handler))
}

async fn dispatch_handler(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(State(state), request).await
}

/// Public dispatch function - also called from top-level POST / handler.
pub(crate) async fn dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split('.').last())
        .unwrap_or("")
        .to_string();

    tracing::debug!("Cognito operation={target}");

    // Read body
    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", &format!("Failed to read body: {e}")).into_response(),
    };

    let body: Value = if body_bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(e) => return cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", &format!("Invalid JSON: {e}")).into_response(),
        }
    };

    let result = match target.as_str() {
        // Pool management
        "CreateUserPool" => handle_create_user_pool(&state, &body).await,
        "DeleteUserPool" => handle_delete_user_pool(&state, &body).await,
        "DescribeUserPool" => handle_describe_user_pool(&state, &body).await,
        "ListUserPools" => handle_list_user_pools(&state, &body).await,
        // Client management
        "CreateUserPoolClient" => handle_create_user_pool_client(&state, &body).await,
        "DeleteUserPoolClient" => handle_delete_user_pool_client(&state, &body).await,
        "DescribeUserPoolClient" => handle_describe_user_pool_client(&state, &body).await,
        "ListUserPoolClients" => handle_list_user_pool_clients(&state, &body).await,
        // User management
        "AdminCreateUser" => handle_admin_create_user(&state, &body).await,
        "AdminDeleteUser" => handle_admin_delete_user(&state, &body).await,
        "AdminGetUser" => handle_admin_get_user(&state, &body).await,
        "AdminSetUserPassword" => handle_admin_set_user_password(&state, &body).await,
        "AdminUpdateUserAttributes" => handle_admin_update_user_attributes(&state, &body).await,
        "ListUsers" => handle_list_users(&state, &body).await,
        // Auth
        "AdminInitiateAuth" | "InitiateAuth" => handle_initiate_auth(&state, &body).await,
        "AdminRespondToAuthChallenge" | "RespondToAuthChallenge" => handle_respond_to_auth_challenge(&state, &body).await,
        // Self-service
        "SignUp" => handle_sign_up(&state, &body).await,
        "ConfirmSignUp" => handle_confirm_sign_up(&state, &body).await,
        "GetUser" => handle_get_user(&state, &body).await,
        other => {
            tracing::warn!("unknown Cognito operation: {other}");
            Err(cognito_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                &format!("unknown operation: {other}"),
            ))
        }
    };

    match result {
        Ok(response) => response.into_response(),
        Err(err) => err.into_response(),
    }
}

type CognitoResult = Result<(StatusCode, axum::Json<Value>), (StatusCode, axum::Json<Value>)>;

// ── Pool Management ────────────────────────────────────────────────────────────

async fn handle_create_user_pool(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let name = body["PoolName"]
        .as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing PoolName"))?
        .to_string();

    let pool_id = generate_pool_id();
    let now = now_secs();
    let arn = pool_arn(&pool_id);

    let pool = UserPoolMeta {
        id: pool_id.clone(),
        name,
        arn,
        created_at: now,
        status: "Active".to_string(),
    };

    save_pool(&state.cognito, &pool).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({"UserPool": user_pool_to_json(&pool)}))))
}

async fn handle_delete_user_pool(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"]
        .as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;

    if load_pool(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?.is_none() {
        return Err(not_found(&format!("User pool {} not found", pool_id)));
    }

    delete_pool_data(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_describe_user_pool(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"]
        .as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;

    let pool = load_pool(&state.cognito, pool_id)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| not_found(&format!("User pool {} not found", pool_id)))?;

    Ok((StatusCode::OK, axum::Json(json!({"UserPool": user_pool_to_json(&pool)}))))
}

async fn handle_list_user_pools(state: &Arc<AppState>, _body: &Value) -> CognitoResult {
    let pools = list_pools(&state.cognito).await.map_err(|e| internal_err(&e.to_string()))?;
    let pool_list: Vec<Value> = pools.iter().map(|p| json!({
        "Id": p.id,
        "Name": p.name,
        "Status": p.status,
        "CreationDate": p.created_at,
        "LastModifiedDate": p.created_at,
    })).collect();
    Ok((StatusCode::OK, axum::Json(json!({"UserPools": pool_list}))))
}

// ── Client Management ──────────────────────────────────────────────────────────

async fn handle_create_user_pool_client(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"]
        .as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let client_name = body["ClientName"]
        .as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ClientName"))?;

    if load_pool(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?.is_none() {
        return Err(not_found(&format!("User pool {} not found", pool_id)));
    }

    let client_id = generate_client_id();
    let now = now_secs();
    let client_secret = body["GenerateSecret"].as_bool().unwrap_or(false).then(|| Uuid::new_v4().to_string());

    let client = UserPoolClientMeta {
        client_id: client_id.clone(),
        client_name: client_name.to_string(),
        pool_id: pool_id.to_string(),
        client_secret,
        created_at: now,
    };

    save_client(&state.cognito, &client).await.map_err(|e| internal_err(&e.to_string()))?;

    let client_json = client_to_json(&client);
    Ok((StatusCode::OK, axum::Json(json!({"UserPoolClient": client_json}))))
}

async fn handle_delete_user_pool_client(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let client_id = body["ClientId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ClientId"))?;

    let key = format!("pools/{pool_id}/clients/{client_id}.json");
    state.cognito.delete(&key).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_describe_user_pool_client(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let client_id = body["ClientId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ClientId"))?;

    let client = load_client(&state.cognito, pool_id, client_id)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| not_found("Client not found"))?;

    Ok((StatusCode::OK, axum::Json(json!({"UserPoolClient": client_to_json(&client)}))))
}

async fn handle_list_user_pool_clients(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;

    let clients = list_clients(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?;
    let client_list: Vec<Value> = clients.iter().map(|c| json!({
        "ClientId": c.client_id,
        "ClientName": c.client_name,
        "UserPoolId": c.pool_id,
    })).collect();
    Ok((StatusCode::OK, axum::Json(json!({"UserPoolClients": client_list}))))
}

fn client_to_json(client: &UserPoolClientMeta) -> Value {
    let mut obj = json!({
        "ClientId": client.client_id,
        "ClientName": client.client_name,
        "UserPoolId": client.pool_id,
        "CreationDate": client.created_at,
        "LastModifiedDate": client.created_at,
    });
    if let Some(ref secret) = client.client_secret {
        obj["ClientSecret"] = json!(secret);
    }
    obj
}

// ── User Management ────────────────────────────────────────────────────────────

async fn handle_admin_create_user(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;

    if load_pool(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?.is_none() {
        return Err(not_found(&format!("User pool {} not found", pool_id)));
    }

    // Check if user exists
    if load_user(&state.cognito, pool_id, username).await.map_err(|e| internal_err(&e.to_string()))?.is_some() {
        return Err(username_exists(&format!("User {} already exists", username)));
    }

    let temp_password = body["TemporaryPassword"].as_str().unwrap_or("TempPass1!").to_string();
    let message_action = body["MessageAction"].as_str().unwrap_or("");
    let suppress = message_action == "SUPPRESS";

    let status = if suppress {
        "CONFIRMED".to_string()
    } else {
        "FORCE_CHANGE_PASSWORD".to_string()
    };

    // Parse attributes
    let mut attributes = Vec::new();
    if let Some(attrs) = body["UserAttributes"].as_array() {
        for attr in attrs {
            if let (Some(name), Some(value)) = (attr["Name"].as_str(), attr["Value"].as_str()) {
                attributes.push(AttributeType { name: name.to_string(), value: value.to_string() });
            }
        }
    }

    let now = now_secs();
    let user = UserMeta {
        username: username.to_string(),
        pool_id: pool_id.to_string(),
        password_hash: hash_password(&temp_password),
        status,
        attributes,
        enabled: true,
        created_at: now,
        updated_at: now,
        sub: Uuid::new_v4().to_string(),
    };

    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({"User": user_to_json(&user)}))))
}

async fn handle_admin_delete_user(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;

    if load_user(&state.cognito, pool_id, username).await.map_err(|e| internal_err(&e.to_string()))?.is_none() {
        return Err(user_not_found(&format!("User {} not found", username)));
    }

    let key = format!("pools/{pool_id}/users/{username}.json");
    state.cognito.delete(&key).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_admin_get_user(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;

    let user = load_user(&state.cognito, pool_id, username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

    // AdminGetUser returns UserAttributes (not Attributes)
    let attrs: Vec<Value> = user.attributes.iter().map(|a| json!({"Name": a.name, "Value": a.value})).collect();
    Ok((StatusCode::OK, axum::Json(json!({
        "Username": user.username,
        "UserAttributes": attrs,
        "UserStatus": user.status,
        "Enabled": user.enabled,
        "UserCreateDate": user.created_at,
        "UserLastModifiedDate": user.updated_at,
    }))))
}

async fn handle_admin_set_user_password(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;
    let password = body["Password"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Password"))?;
    let permanent = body["Permanent"].as_bool().unwrap_or(false);

    let mut user = load_user(&state.cognito, pool_id, username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

    user.password_hash = hash_password(password);
    if permanent {
        user.status = "CONFIRMED".to_string();
    }
    user.updated_at = now_secs();
    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_admin_update_user_attributes(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;

    let mut user = load_user(&state.cognito, pool_id, username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

    if let Some(attrs) = body["UserAttributes"].as_array() {
        for attr in attrs {
            if let (Some(name), Some(value)) = (attr["Name"].as_str(), attr["Value"].as_str()) {
                // Update or add attribute
                if let Some(existing) = user.attributes.iter_mut().find(|a| a.name == name) {
                    existing.value = value.to_string();
                } else {
                    user.attributes.push(AttributeType { name: name.to_string(), value: value.to_string() });
                }
            }
        }
    }

    user.updated_at = now_secs();
    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_list_users(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let pool_id = body["UserPoolId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing UserPoolId"))?;

    let mut users = list_users(&state.cognito, pool_id).await.map_err(|e| internal_err(&e.to_string()))?;

    // Apply basic filter: `email = "x"` or `username = "x"`
    if let Some(filter_str) = body["Filter"].as_str() {
        let filter_str = filter_str.trim();
        // Parse simple `name = "value"` or `name ^= "value"` patterns
        if let Some((attr_name, attr_value)) = parse_filter(filter_str) {
            users.retain(|u| {
                if attr_name == "username" {
                    u.username == attr_value
                } else {
                    u.attributes.iter().any(|a| a.name == attr_name && a.value == attr_value)
                }
            });
        }
    }

    let limit = body["Limit"].as_u64().unwrap_or(60) as usize;
    users.truncate(limit);

    let user_list: Vec<Value> = users.iter().map(|u| user_to_json(u)).collect();
    Ok((StatusCode::OK, axum::Json(json!({"Users": user_list}))))
}

fn parse_filter(filter: &str) -> Option<(String, String)> {
    // Handle `name = "value"` or `name = 'value'`
    let parts: Vec<&str> = filter.splitn(2, '=').collect();
    if parts.len() == 2 {
        let name = parts[0].trim().to_string();
        let value = parts[1].trim().trim_matches('"').trim_matches('\'').to_string();
        return Some((name, value));
    }
    None
}

// ── Auth ───────────────────────────────────────────────────────────────────────

async fn handle_initiate_auth(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let auth_flow = body["AuthFlow"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing AuthFlow"))?;
    let auth_params = &body["AuthParameters"];
    let client_id = body["ClientId"].as_str()
        .or_else(|| body["UserPoolId"].as_str()) // AdminInitiateAuth uses UserPoolId too
        .unwrap_or("");

    // For AdminInitiateAuth, ClientId might be in a different field
    let client_id = body["ClientId"].as_str().unwrap_or(client_id);
    let pool_id = body["UserPoolId"].as_str().unwrap_or("");

    match auth_flow {
        "USER_PASSWORD_AUTH" => {
            let username = auth_params["USERNAME"].as_str()
                .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing USERNAME"))?;
            let password = auth_params["PASSWORD"].as_str()
                .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing PASSWORD"))?;

            // Determine pool_id: either from body or by searching if client_id is provided
            let resolved_pool_id = if !pool_id.is_empty() {
                pool_id.to_string()
            } else {
                find_pool_for_client(state, client_id).await
                    .map_err(|e| internal_err(&e.to_string()))?
                    .ok_or_else(|| not_found("User pool or client not found"))?
            };

            let user = load_user(&state.cognito, &resolved_pool_id, username)
                .await
                .map_err(|e| internal_err(&e.to_string()))?
                .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

            if !user.enabled {
                return Err(not_authorized("User is disabled"));
            }

            if user.password_hash != hash_password(password) {
                return Err(not_authorized("Incorrect username or password"));
            }

            if user.status == "FORCE_CHANGE_PASSWORD" {
                // Real Cognito encodes requiredAttributes and userAttributes as JSON strings
                let user_attrs_map: serde_json::Map<String, Value> = user.attributes.iter()
                    .map(|a| (a.name.clone(), json!(a.value)))
                    .collect();
                let user_attrs_str = serde_json::to_string(&user_attrs_map).unwrap_or_else(|_| "{}".to_string());
                // Return challenge
                return Ok((StatusCode::OK, axum::Json(json!({
                    "ChallengeName": "NEW_PASSWORD_REQUIRED",
                    "ChallengeParameters": {
                        "USER_ID_FOR_SRP": username,
                        "requiredAttributes": "[]",
                        "userAttributes": user_attrs_str,
                    },
                    "Session": format!("session-{}-{}", resolved_pool_id, username),
                }))));
            }

            let keys = get_keys(&state.cognito).await;
            let (access_token, id_token, refresh_token) = issue_tokens(&keys, &resolved_pool_id, client_id, &user)
                .map_err(|e| internal_err(&e.to_string()))?;

            // Save refresh token
            let rt_meta = RefreshTokenMeta {
                username: username.to_string(),
                pool_id: resolved_pool_id.clone(),
                client_id: client_id.to_string(),
                expires_at: now_secs() + 30.0 * 24.0 * 3600.0,
            };
            save_refresh_token(&state.cognito, &refresh_token, &rt_meta)
                .await
                .map_err(|e| internal_err(&e.to_string()))?;

            Ok((StatusCode::OK, axum::Json(json!({
                "AuthenticationResult": {
                    "AccessToken": access_token,
                    "IdToken": id_token,
                    "RefreshToken": refresh_token,
                    "TokenType": "Bearer",
                    "ExpiresIn": 3600,
                }
            }))))
        }
        "REFRESH_TOKEN_AUTH" | "REFRESH_TOKEN" => {
            let refresh_token = auth_params["REFRESH_TOKEN"].as_str()
                .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing REFRESH_TOKEN"))?;

            // Determine pool_id
            let resolved_pool_id = if !pool_id.is_empty() {
                pool_id.to_string()
            } else {
                find_pool_for_client(state, client_id).await
                    .map_err(|e| internal_err(&e.to_string()))?
                    .ok_or_else(|| not_found("User pool or client not found"))?
            };

            let rt_meta = load_refresh_token(&state.cognito, &resolved_pool_id, refresh_token)
                .await
                .map_err(|e| internal_err(&e.to_string()))?
                .ok_or_else(|| not_authorized("Refresh token not found or expired"))?;

            if rt_meta.expires_at < now_secs() {
                return Err(not_authorized("Refresh token has expired"));
            }

            let user = load_user(&state.cognito, &resolved_pool_id, &rt_meta.username)
                .await
                .map_err(|e| internal_err(&e.to_string()))?
                .ok_or_else(|| user_not_found("User not found"))?;

            let keys = get_keys(&state.cognito).await;
            let (access_token, id_token, _new_refresh) = issue_tokens(&keys, &resolved_pool_id, client_id, &user)
                .map_err(|e| internal_err(&e.to_string()))?;

            Ok((StatusCode::OK, axum::Json(json!({
                "AuthenticationResult": {
                    "AccessToken": access_token,
                    "IdToken": id_token,
                    "TokenType": "Bearer",
                    "ExpiresIn": 3600,
                }
            }))))
        }
        other => Err(cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", &format!("Unsupported AuthFlow: {other}"))),
    }
}

async fn find_pool_for_client(state: &Arc<AppState>, client_id: &str) -> anyhow::Result<Option<String>> {
    // Search through all pools for this client
    let pools = list_pools(&state.cognito).await?;
    for pool in pools {
        if load_client(&state.cognito, &pool.id, client_id).await?.is_some() {
            return Ok(Some(pool.id));
        }
    }
    Ok(None)
}

async fn handle_respond_to_auth_challenge(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let challenge_name = body["ChallengeName"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ChallengeName"))?;

    if challenge_name != "NEW_PASSWORD_REQUIRED" {
        return Err(cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", &format!("Unsupported challenge: {challenge_name}")));
    }

    let client_id = body["ClientId"].as_str().unwrap_or("");
    let pool_id_from_body = body["UserPoolId"].as_str().unwrap_or("");
    let responses = &body["ChallengeResponses"];
    let username = responses["USERNAME"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing USERNAME in ChallengeResponses"))?;
    let new_password = responses["NEW_PASSWORD"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing NEW_PASSWORD in ChallengeResponses"))?;

    let pool_id = if !pool_id_from_body.is_empty() {
        pool_id_from_body.to_string()
    } else {
        find_pool_for_client(state, client_id).await
            .map_err(|e| internal_err(&e.to_string()))?
            .ok_or_else(|| not_found("Client or pool not found"))?
    };

    let mut user = load_user(&state.cognito, &pool_id, username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

    if new_password.len() < 6 {
        return Err(invalid_password("Password too short"));
    }

    user.password_hash = hash_password(new_password);
    user.status = "CONFIRMED".to_string();
    user.updated_at = now_secs();
    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    let keys = get_keys(&state.cognito).await;
    let (access_token, id_token, refresh_token) = issue_tokens(&keys, &pool_id, client_id, &user)
        .map_err(|e| internal_err(&e.to_string()))?;

    // Save refresh token
    let rt_meta = RefreshTokenMeta {
        username: username.to_string(),
        pool_id: pool_id.clone(),
        client_id: client_id.to_string(),
        expires_at: now_secs() + 30.0 * 24.0 * 3600.0,
    };
    save_refresh_token(&state.cognito, &refresh_token, &rt_meta)
        .await
        .map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({
        "AuthenticationResult": {
            "AccessToken": access_token,
            "IdToken": id_token,
            "RefreshToken": refresh_token,
            "TokenType": "Bearer",
            "ExpiresIn": 3600,
        }
    }))))
}

// ── Self-service ───────────────────────────────────────────────────────────────

async fn handle_sign_up(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let client_id = body["ClientId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ClientId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;
    let password = body["Password"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Password"))?;

    let pool_id = find_pool_for_client(state, client_id).await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| not_found("Client or pool not found"))?;

    // Check user doesn't exist
    if load_user(&state.cognito, &pool_id, username).await.map_err(|e| internal_err(&e.to_string()))?.is_some() {
        return Err(username_exists(&format!("User {} already exists", username)));
    }

    let mut attributes = Vec::new();
    if let Some(attrs) = body["UserAttributes"].as_array() {
        for attr in attrs {
            if let (Some(name), Some(value)) = (attr["Name"].as_str(), attr["Value"].as_str()) {
                attributes.push(AttributeType { name: name.to_string(), value: value.to_string() });
            }
        }
    }

    let now = now_secs();
    let sub = Uuid::new_v4().to_string();
    let user = UserMeta {
        username: username.to_string(),
        pool_id: pool_id.clone(),
        password_hash: hash_password(password),
        status: "UNCONFIRMED".to_string(),
        attributes,
        enabled: true,
        created_at: now,
        updated_at: now,
        sub: sub.clone(),
    };

    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({
        "UserConfirmed": false,
        "UserSub": sub,
    }))))
}

async fn handle_confirm_sign_up(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let client_id = body["ClientId"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing ClientId"))?;
    let username = body["Username"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing Username"))?;
    // Accept any confirmation code
    let _code = body["ConfirmationCode"].as_str().unwrap_or("000000");

    let pool_id = find_pool_for_client(state, client_id).await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| not_found("Client or pool not found"))?;

    let mut user = load_user(&state.cognito, &pool_id, username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found(&format!("User {} not found", username)))?;

    user.status = "CONFIRMED".to_string();
    user.updated_at = now_secs();
    save_user(&state.cognito, &user).await.map_err(|e| internal_err(&e.to_string()))?;

    Ok((StatusCode::OK, axum::Json(json!({}))))
}

async fn handle_get_user(state: &Arc<AppState>, body: &Value) -> CognitoResult {
    let access_token = body["AccessToken"].as_str()
        .ok_or_else(|| cognito_error(StatusCode::BAD_REQUEST, "InvalidParameterException", "Missing AccessToken"))?;

    let keys = get_keys(&state.cognito).await;
    let claims = validate_access_token(&keys, access_token)
        .map_err(|e| not_authorized(&format!("Invalid access token: {e}")))?;

    if claims.token_use != "access" {
        return Err(not_authorized("Token is not an access token"));
    }

    // Extract pool_id from issuer: https://cognito-idp.{region}.amazonaws.com/{pool_id}
    let pool_id = claims.iss.split('/').last().unwrap_or("").to_string();

    let user = load_user(&state.cognito, &pool_id, &claims.username)
        .await
        .map_err(|e| internal_err(&e.to_string()))?
        .ok_or_else(|| user_not_found("User not found"))?;

    let attrs: Vec<Value> = user.attributes.iter().map(|a| json!({"Name": a.name, "Value": a.value})).collect();
    Ok((StatusCode::OK, axum::Json(json!({
        "Username": user.username,
        "UserAttributes": attrs,
    }))))
}
