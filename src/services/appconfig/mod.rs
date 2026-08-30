//! AppConfig service emulator.
//!
//! Wire format: REST/JSON. Uses real AWS AppConfig API paths (no prefix) — literal
//! routes take priority over S3's wildcard `/{bucket}` in axum's router.
//!
//! Management plane (aws-sdk-appconfig):
//!   Applications, Environments, Configuration Profiles, Hosted Config Versions,
//!   Deployment Strategies, Deployments.
//!
//! Data plane (aws-sdk-appconfigdata):
//!   POST /configurationsessions  → StartConfigurationSession
//!   GET  /configuration          → GetLatestConfiguration
//!
//! Storage layout (under data/appconfig/):
//!   applications/{app_id}.json
//!   environments/{app_id}/{env_id}.json
//!   profiles/{app_id}/{profile_id}.json
//!   hostedversions/{app_id}/{profile_id}/{version_number}.json
//!   strategies/{strategy_id}.json
//!   deployments/{app_id}/{env_id}/{deploy_num}.json
//!   deployed/{app_id}/{env_id}.json          — tracks the currently live version
//!   sessions/{token}.json                    — configuration sessions

use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{services::AppState, storage::Storage};

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Application {
    id: String,
    name: String,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Environment {
    id: String,
    application_id: String,
    name: String,
    description: String,
    state: String,
    /// Next deployment number (1-based counter).
    next_deployment_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigurationProfile {
    id: String,
    application_id: String,
    name: String,
    description: String,
    location_uri: String,
    #[serde(rename = "type")]
    profile_type: String,
    /// Highest hosted configuration version number (0 = none yet).
    latest_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HostedConfigurationVersion {
    version_number: u32,
    application_id: String,
    configuration_profile_id: String,
    /// Raw configuration content, base64-encoded for storage.
    content_b64: String,
    content_type: String,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeploymentStrategy {
    id: String,
    name: String,
    description: String,
    deployment_duration_in_minutes: u32,
    growth_type: String,
    growth_factor: f64,
    final_bake_time_in_minutes: u32,
    replicate_to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Deployment {
    deployment_number: u32,
    application_id: String,
    environment_id: String,
    configuration_profile_id: String,
    configuration_version: String,
    deployment_strategy_id: String,
    deployment_duration_in_minutes: u32,
    growth_type: String,
    growth_factor: f64,
    final_bake_time_in_minutes: u32,
    state: String,
    percentage_complete: f64,
    started_at: String,
    completed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeployedConfig {
    configuration_profile_id: String,
    configuration_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigSession {
    token: String,
    application_id: String,
    environment_id: String,
    configuration_profile_id: String,
    /// The configuration version the client last received.
    client_configuration_version: Option<u32>,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn json_ok(body: impl Serialize) -> impl IntoResponse {
    match serde_json::to_string(&body) {
        Ok(s) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], s).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn json_created(body: impl Serialize) -> impl IntoResponse {
    match serde_json::to_string(&body) {
        Ok(s) => {
            (StatusCode::CREATED, [(header::CONTENT_TYPE, "application/json")], s).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn err_not_found(msg: &str) -> impl IntoResponse {
    (StatusCode::NOT_FOUND, msg.to_string()).into_response()
}

fn err_bad(msg: &str) -> impl IntoResponse {
    (StatusCode::BAD_REQUEST, msg.to_string()).into_response()
}

fn err_internal(msg: impl std::fmt::Display) -> impl IntoResponse {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()).into_response()
}

async fn read_json<T: for<'de> Deserialize<'de>>(request: Request) -> Result<T, axum::response::Response> {
    let bytes = to_bytes(request.into_body(), 4 * 1024 * 1024)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()).into_response())?;
    serde_json::from_slice(&bytes)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")).into_response())
}

// ── Storage key helpers ───────────────────────────────────────────────────────

fn app_key(app_id: &str) -> String { format!("applications/{app_id}.json") }
fn env_key(app_id: &str, env_id: &str) -> String { format!("environments/{app_id}/{env_id}.json") }
fn profile_key(app_id: &str, profile_id: &str) -> String { format!("profiles/{app_id}/{profile_id}.json") }
fn hcv_key(app_id: &str, profile_id: &str, version: u32) -> String {
    format!("hostedversions/{app_id}/{profile_id}/{version:08}.json")
}
fn strategy_key(strategy_id: &str) -> String { format!("strategies/{strategy_id}.json") }
fn deployment_key(app_id: &str, env_id: &str, num: u32) -> String {
    format!("deployments/{app_id}/{env_id}/{num:08}.json")
}
fn deployed_key(app_id: &str, env_id: &str) -> String { format!("deployed/{app_id}/{env_id}.json") }
fn session_key(token: &str) -> String { format!("sessions/{token}.json") }

// ── Generic load/save helpers ─────────────────────────────────────────────────

async fn load<T: for<'de> Deserialize<'de>>(
    storage: &crate::storage::file::FileStorage,
    key: &str,
) -> Option<T> {
    let data = storage.get(key).await.ok()??;
    serde_json::from_slice(&data).ok()
}

async fn save<T: Serialize>(
    storage: &crate::storage::file::FileStorage,
    key: &str,
    value: &T,
) -> Result<(), axum::response::Response> {
    let data = serde_json::to_vec(value)
        .map_err(|e| err_internal(e).into_response())?;
    storage
        .put(key, data)
        .await
        .map_err(|e| err_internal(e).into_response())
}

// ── Built-in deployment strategies ───────────────────────────────────────────

fn builtin_strategies() -> Vec<DeploymentStrategy> {
    vec![
        DeploymentStrategy {
            id: "AppConfig.AllAtOnce".into(),
            name: "AppConfig.AllAtOnce".into(),
            description: "Quick. This strategy deploys the configuration to all targets immediately with zero bake time. Suitable for dev.".into(),
            deployment_duration_in_minutes: 0,
            growth_type: "LINEAR".into(),
            growth_factor: 100.0,
            final_bake_time_in_minutes: 0,
            replicate_to: "NONE".into(),
        },
        DeploymentStrategy {
            id: "AppConfig.Linear50PercentEvery30Seconds".into(),
            name: "AppConfig.Linear50PercentEvery30Seconds".into(),
            description: "Testing. A linear 50% every 30 seconds strategy.".into(),
            deployment_duration_in_minutes: 1,
            growth_type: "LINEAR".into(),
            growth_factor: 50.0,
            final_bake_time_in_minutes: 1,
            replicate_to: "NONE".into(),
        },
        DeploymentStrategy {
            id: "AppConfig.Canary10Percent20Minutes".into(),
            name: "AppConfig.Canary10Percent20Minutes".into(),
            description: "AWS Recommended. This strategy processes the first 10% of targets immediately, then deploys to the remaining 90% over 20 minutes.".into(),
            deployment_duration_in_minutes: 20,
            growth_type: "EXPONENTIAL".into(),
            growth_factor: 10.0,
            final_bake_time_in_minutes: 10,
            replicate_to: "NONE".into(),
        },
    ]
}

fn strategy_to_json(s: &DeploymentStrategy) -> serde_json::Value {
    serde_json::json!({
        "Id": s.id,
        "Name": s.name,
        "Description": s.description,
        "DeploymentDurationInMinutes": s.deployment_duration_in_minutes,
        "GrowthType": s.growth_type,
        "GrowthFactor": s.growth_factor,
        "FinalBakeTimeInMinutes": s.final_bake_time_in_minutes,
        "ReplicateTo": s.replicate_to,
    })
}

// ── Applications ─────────────────────────────────────────────────────────────

async fn list_applications(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let keys = match s.appconfig.list("applications/").await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items = Vec::new();
    for key in &keys {
        if let Some(app) = load::<Application>(&s.appconfig, key).await {
            items.push(serde_json::json!({
                "Id": app.id,
                "Name": app.name,
                "Description": app.description,
            }));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

async fn create_application(
    State(s): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    #[derive(Deserialize)]
    struct Body { #[serde(rename = "Name")] name: String, #[serde(rename = "Description", default)] description: String }

    let body: Body = match read_json(request).await {
        Ok(b) => b,
        Err(r) => return r,
    };

    let app = Application { id: Uuid::new_v4().to_string(), name: body.name, description: body.description };
    if let Err(r) = save(&s.appconfig, &app_key(&app.id), &app).await { return r; }

    json_created(serde_json::json!({ "Id": app.id, "Name": app.name, "Description": app.description })).into_response()
}

async fn get_application(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> impl IntoResponse {
    match load::<Application>(&s.appconfig, &app_key(&app_id)).await {
        Some(app) => json_ok(serde_json::json!({ "Id": app.id, "Name": app.name, "Description": app.description })).into_response(),
        None => err_not_found("application not found").into_response(),
    }
}

async fn update_application(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    request: Request,
) -> impl IntoResponse {
    #[derive(Deserialize)]
    struct Body { #[serde(rename = "Name")] name: Option<String>, #[serde(rename = "Description")] description: Option<String> }

    let mut app: Application = match load(&s.appconfig, &app_key(&app_id)).await {
        Some(a) => a,
        None => return err_not_found("application not found").into_response(),
    };
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };
    if let Some(n) = body.name { app.name = n; }
    if let Some(d) = body.description { app.description = d; }
    if let Err(r) = save(&s.appconfig, &app_key(&app_id), &app).await { return r; }

    json_ok(serde_json::json!({ "Id": app.id, "Name": app.name, "Description": app.description })).into_response()
}

async fn delete_application(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> impl IntoResponse {
    if load::<Application>(&s.appconfig, &app_key(&app_id)).await.is_none() {
        return err_not_found("application not found").into_response();
    }
    if let Err(e) = s.appconfig.delete(&app_key(&app_id)).await {
        return err_internal(e).into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

// ── Environments ──────────────────────────────────────────────────────────────

async fn list_environments(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> impl IntoResponse {
    let prefix = format!("environments/{app_id}/");
    let keys = match s.appconfig.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items = Vec::new();
    for key in &keys {
        if let Some(env) = load::<Environment>(&s.appconfig, key).await {
            items.push(env_to_json(&env));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

fn env_to_json(env: &Environment) -> serde_json::Value {
    serde_json::json!({
        "ApplicationId": env.application_id,
        "Id": env.id,
        "Name": env.name,
        "Description": env.description,
        "State": env.state,
    })
}

async fn create_environment(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    request: Request,
) -> impl IntoResponse {
    if load::<Application>(&s.appconfig, &app_key(&app_id)).await.is_none() {
        return err_not_found("application not found").into_response();
    }
    #[derive(Deserialize)]
    struct Body { #[serde(rename = "Name")] name: String, #[serde(rename = "Description", default)] description: String }
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };

    let env = Environment {
        id: Uuid::new_v4().to_string(),
        application_id: app_id.clone(),
        name: body.name,
        description: body.description,
        state: "READY_FOR_DEPLOYMENT".into(),
        next_deployment_number: 1,
    };
    if let Err(r) = save(&s.appconfig, &env_key(&app_id, &env.id), &env).await { return r; }
    json_created(env_to_json(&env)).into_response()
}

async fn get_environment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match load::<Environment>(&s.appconfig, &env_key(&app_id, &env_id)).await {
        Some(env) => json_ok(env_to_json(&env)).into_response(),
        None => err_not_found("environment not found").into_response(),
    }
}

async fn update_environment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id)): Path<(String, String)>,
    request: Request,
) -> impl IntoResponse {
    let mut env: Environment = match load(&s.appconfig, &env_key(&app_id, &env_id)).await {
        Some(e) => e,
        None => return err_not_found("environment not found").into_response(),
    };
    #[derive(Deserialize)]
    struct Body { #[serde(rename = "Name")] name: Option<String>, #[serde(rename = "Description")] description: Option<String> }
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };
    if let Some(n) = body.name { env.name = n; }
    if let Some(d) = body.description { env.description = d; }
    if let Err(r) = save(&s.appconfig, &env_key(&app_id, &env_id), &env).await { return r; }
    json_ok(env_to_json(&env)).into_response()
}

async fn delete_environment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id)): Path<(String, String)>,
) -> impl IntoResponse {
    if load::<Environment>(&s.appconfig, &env_key(&app_id, &env_id)).await.is_none() {
        return err_not_found("environment not found").into_response();
    }
    if let Err(e) = s.appconfig.delete(&env_key(&app_id, &env_id)).await {
        return err_internal(e).into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

// ── Configuration Profiles ────────────────────────────────────────────────────

fn profile_to_json(p: &ConfigurationProfile) -> serde_json::Value {
    serde_json::json!({
        "ApplicationId": p.application_id,
        "Id": p.id,
        "Name": p.name,
        "Description": p.description,
        "LocationUri": p.location_uri,
        "Type": p.profile_type,
        "Validators": [],
    })
}

async fn list_profiles(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> impl IntoResponse {
    let prefix = format!("profiles/{app_id}/");
    let keys = match s.appconfig.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items = Vec::new();
    for key in &keys {
        if let Some(p) = load::<ConfigurationProfile>(&s.appconfig, key).await {
            items.push(profile_to_json(&p));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

async fn create_profile(
    State(s): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    request: Request,
) -> impl IntoResponse {
    if load::<Application>(&s.appconfig, &app_key(&app_id)).await.is_none() {
        return err_not_found("application not found").into_response();
    }
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "Name")] name: String,
        #[serde(rename = "Description", default)] description: String,
        #[serde(rename = "LocationUri", default = "default_location_uri")] location_uri: String,
        #[serde(rename = "Type", default = "default_profile_type")] profile_type: String,
    }
    fn default_location_uri() -> String { "hosted".into() }
    fn default_profile_type() -> String { "AWS.Freeform".into() }

    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };
    let profile = ConfigurationProfile {
        id: Uuid::new_v4().to_string(),
        application_id: app_id.clone(),
        name: body.name,
        description: body.description,
        location_uri: body.location_uri,
        profile_type: body.profile_type,
        latest_version: 0,
    };
    if let Err(r) = save(&s.appconfig, &profile_key(&app_id, &profile.id), &profile).await { return r; }
    json_created(profile_to_json(&profile)).into_response()
}

async fn get_profile(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match load::<ConfigurationProfile>(&s.appconfig, &profile_key(&app_id, &profile_id)).await {
        Some(p) => json_ok(profile_to_json(&p)).into_response(),
        None => err_not_found("configuration profile not found").into_response(),
    }
}

async fn update_profile(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id)): Path<(String, String)>,
    request: Request,
) -> impl IntoResponse {
    let mut p: ConfigurationProfile = match load(&s.appconfig, &profile_key(&app_id, &profile_id)).await {
        Some(p) => p,
        None => return err_not_found("configuration profile not found").into_response(),
    };
    #[derive(Deserialize)]
    struct Body { #[serde(rename = "Name")] name: Option<String>, #[serde(rename = "Description")] description: Option<String> }
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };
    if let Some(n) = body.name { p.name = n; }
    if let Some(d) = body.description { p.description = d; }
    if let Err(r) = save(&s.appconfig, &profile_key(&app_id, &profile_id), &p).await { return r; }
    json_ok(profile_to_json(&p)).into_response()
}

async fn delete_profile(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id)): Path<(String, String)>,
) -> impl IntoResponse {
    if load::<ConfigurationProfile>(&s.appconfig, &profile_key(&app_id, &profile_id)).await.is_none() {
        return err_not_found("configuration profile not found").into_response();
    }
    if let Err(e) = s.appconfig.delete(&profile_key(&app_id, &profile_id)).await {
        return err_internal(e).into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

// ── Hosted Configuration Versions ─────────────────────────────────────────────

async fn create_hosted_config_version(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id)): Path<(String, String)>,
    request: Request,
) -> impl IntoResponse {
    let mut profile: ConfigurationProfile =
        match load(&s.appconfig, &profile_key(&app_id, &profile_id)).await {
            Some(p) => p,
            None => return err_not_found("configuration profile not found").into_response(),
        };

    // Content-Type from request headers
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let description = request
        .headers()
        .get("appconfig-configuration-description")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body_bytes = match to_bytes(request.into_body(), 4 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return err_bad(&format!("failed to read body: {e}")).into_response(),
    };

    profile.latest_version += 1;
    let version_number = profile.latest_version;

    let hcv = HostedConfigurationVersion {
        version_number,
        application_id: app_id.clone(),
        configuration_profile_id: profile_id.clone(),
        content_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &body_bytes),
        content_type: content_type.clone(),
        description,
    };

    if let Err(r) = save(&s.appconfig, &hcv_key(&app_id, &profile_id, version_number), &hcv).await { return r; }
    if let Err(r) = save(&s.appconfig, &profile_key(&app_id, &profile_id), &profile).await { return r; }

    // Respond with the raw content and AppConfig-specific headers (AWS SDK naming).
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_str(&content_type).unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")));
    headers.insert(
        HeaderName::from_static("version-number"),
        HeaderValue::from(version_number),
    );
    headers.insert(
        HeaderName::from_static("application-id"),
        HeaderValue::from_str(&app_id).unwrap(),
    );
    headers.insert(
        HeaderName::from_static("configuration-profile-id"),
        HeaderValue::from_str(&profile_id).unwrap(),
    );

    (StatusCode::CREATED, headers, body_bytes).into_response()
}

async fn list_hosted_config_versions(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let prefix = format!("hostedversions/{app_id}/{profile_id}/");
    let keys = match s.appconfig.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items = Vec::new();
    for key in &keys {
        if let Some(hcv) = load::<HostedConfigurationVersion>(&s.appconfig, key).await {
            items.push(serde_json::json!({
                "ApplicationId": hcv.application_id,
                "ConfigurationProfileId": hcv.configuration_profile_id,
                "VersionNumber": hcv.version_number,
                "ContentType": hcv.content_type,
                "Description": hcv.description,
            }));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

async fn get_hosted_config_version(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id, version_number)): Path<(String, String, u32)>,
) -> impl IntoResponse {
    let hcv: HostedConfigurationVersion =
        match load(&s.appconfig, &hcv_key(&app_id, &profile_id, version_number)).await {
            Some(h) => h,
            None => return err_not_found("hosted configuration version not found").into_response(),
        };

    let content = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &hcv.content_b64,
    )
    .unwrap_or_default();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&hcv.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        HeaderName::from_static("version-number"),
        HeaderValue::from(version_number),
    );
    headers.insert(
        HeaderName::from_static("application-id"),
        HeaderValue::from_str(&app_id).unwrap(),
    );
    headers.insert(
        HeaderName::from_static("configuration-profile-id"),
        HeaderValue::from_str(&profile_id).unwrap(),
    );

    (StatusCode::OK, headers, content).into_response()
}

async fn delete_hosted_config_version(
    State(s): State<Arc<AppState>>,
    Path((app_id, profile_id, version_number)): Path<(String, String, u32)>,
) -> impl IntoResponse {
    if load::<HostedConfigurationVersion>(
        &s.appconfig,
        &hcv_key(&app_id, &profile_id, version_number),
    )
    .await
    .is_none()
    {
        return err_not_found("hosted configuration version not found").into_response();
    }
    if let Err(e) = s.appconfig.delete(&hcv_key(&app_id, &profile_id, version_number)).await {
        return err_internal(e).into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

// ── Deployment Strategies ─────────────────────────────────────────────────────

async fn list_strategies(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let keys = match s.appconfig.list("strategies/").await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items: Vec<serde_json::Value> = builtin_strategies().iter().map(strategy_to_json).collect();
    for key in &keys {
        if let Some(st) = load::<DeploymentStrategy>(&s.appconfig, key).await {
            items.push(strategy_to_json(&st));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

async fn create_strategy(
    State(s): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "Name")] name: String,
        #[serde(rename = "Description", default)] description: String,
        #[serde(rename = "DeploymentDurationInMinutes", default)] deployment_duration_in_minutes: u32,
        #[serde(rename = "GrowthType", default = "default_growth_type")] growth_type: String,
        #[serde(rename = "GrowthFactor", default = "default_growth_factor")] growth_factor: f64,
        #[serde(rename = "FinalBakeTimeInMinutes", default)] final_bake_time_in_minutes: u32,
        #[serde(rename = "ReplicateTo", default = "default_replicate_to")] replicate_to: String,
    }
    fn default_growth_type() -> String { "LINEAR".into() }
    fn default_growth_factor() -> f64 { 100.0 }
    fn default_replicate_to() -> String { "NONE".into() }

    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };
    let strategy = DeploymentStrategy {
        id: Uuid::new_v4().to_string(),
        name: body.name,
        description: body.description,
        deployment_duration_in_minutes: body.deployment_duration_in_minutes,
        growth_type: body.growth_type,
        growth_factor: body.growth_factor,
        final_bake_time_in_minutes: body.final_bake_time_in_minutes,
        replicate_to: body.replicate_to,
    };
    if let Err(r) = save(&s.appconfig, &strategy_key(&strategy.id), &strategy).await { return r; }
    json_created(strategy_to_json(&strategy)).into_response()
}

async fn get_strategy(
    State(s): State<Arc<AppState>>,
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // Check built-in first.
    if let Some(st) = builtin_strategies().into_iter().find(|st| st.id == strategy_id) {
        return json_ok(strategy_to_json(&st)).into_response();
    }
    match load::<DeploymentStrategy>(&s.appconfig, &strategy_key(&strategy_id)).await {
        Some(st) => json_ok(strategy_to_json(&st)).into_response(),
        None => err_not_found("deployment strategy not found").into_response(),
    }
}

// ── Deployments ───────────────────────────────────────────────────────────────

fn deployment_to_json(d: &Deployment) -> serde_json::Value {
    serde_json::json!({
        "DeploymentNumber": d.deployment_number,
        "ApplicationId": d.application_id,
        "EnvironmentId": d.environment_id,
        "ConfigurationProfileId": d.configuration_profile_id,
        "ConfigurationVersion": d.configuration_version,
        "DeploymentStrategyId": d.deployment_strategy_id,
        "DeploymentDurationInMinutes": d.deployment_duration_in_minutes,
        "GrowthType": d.growth_type,
        "GrowthFactor": d.growth_factor,
        "FinalBakeTimeInMinutes": d.final_bake_time_in_minutes,
        "State": d.state,
        "PercentageComplete": d.percentage_complete,
        "StartedAt": d.started_at,
        "CompletedAt": d.completed_at,
    })
}

async fn start_deployment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id)): Path<(String, String)>,
    request: Request,
) -> impl IntoResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "ConfigurationProfileId")] configuration_profile_id: String,
        #[serde(rename = "ConfigurationVersion")] configuration_version: String,
        #[serde(rename = "DeploymentStrategyId")] deployment_strategy_id: String,
        #[serde(rename = "Description", default)] description: String,
    }
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };

    let mut env: Environment = match load(&s.appconfig, &env_key(&app_id, &env_id)).await {
        Some(e) => e,
        None => return err_not_found("environment not found").into_response(),
    };

    // Resolve strategy
    let strategy = if let Some(st) = builtin_strategies().into_iter().find(|st| st.id == body.deployment_strategy_id) {
        st
    } else {
        match load::<DeploymentStrategy>(&s.appconfig, &strategy_key(&body.deployment_strategy_id)).await {
            Some(st) => st,
            None => return err_not_found("deployment strategy not found").into_response(),
        }
    };

    let deploy_num = env.next_deployment_number;
    env.next_deployment_number += 1;
    env.state = "DEPLOYING".into();

    let now = now_iso8601();
    let deployment = Deployment {
        deployment_number: deploy_num,
        application_id: app_id.clone(),
        environment_id: env_id.clone(),
        configuration_profile_id: body.configuration_profile_id.clone(),
        configuration_version: body.configuration_version.clone(),
        deployment_strategy_id: body.deployment_strategy_id,
        deployment_duration_in_minutes: strategy.deployment_duration_in_minutes,
        growth_type: strategy.growth_type,
        growth_factor: strategy.growth_factor,
        final_bake_time_in_minutes: strategy.final_bake_time_in_minutes,
        state: "COMPLETE".into(),
        percentage_complete: 100.0,
        started_at: now.clone(),
        completed_at: now,
    };

    let version_number: u32 = body.configuration_version.parse().unwrap_or(0);
    let deployed = DeployedConfig {
        configuration_profile_id: body.configuration_profile_id,
        configuration_version: version_number,
    };

    env.state = "READY_FOR_DEPLOYMENT".into();

    if let Err(r) = save(&s.appconfig, &deployment_key(&app_id, &env_id, deploy_num), &deployment).await { return r; }
    if let Err(r) = save(&s.appconfig, &env_key(&app_id, &env_id), &env).await { return r; }
    if let Err(r) = save(&s.appconfig, &deployed_key(&app_id, &env_id), &deployed).await { return r; }

    json_created(deployment_to_json(&deployment)).into_response()
}

async fn list_deployments(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let prefix = format!("deployments/{app_id}/{env_id}/");
    let keys = match s.appconfig.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return err_internal(e).into_response(),
    };
    let mut items = Vec::new();
    for key in &keys {
        if let Some(d) = load::<Deployment>(&s.appconfig, key).await {
            items.push(deployment_to_json(&d));
        }
    }
    json_ok(serde_json::json!({ "Items": items })).into_response()
}

async fn get_deployment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id, deploy_num)): Path<(String, String, u32)>,
) -> impl IntoResponse {
    match load::<Deployment>(&s.appconfig, &deployment_key(&app_id, &env_id, deploy_num)).await {
        Some(d) => json_ok(deployment_to_json(&d)).into_response(),
        None => err_not_found("deployment not found").into_response(),
    }
}

async fn stop_deployment(
    State(s): State<Arc<AppState>>,
    Path((app_id, env_id, deploy_num)): Path<(String, String, u32)>,
) -> impl IntoResponse {
    let mut d: Deployment = match load(&s.appconfig, &deployment_key(&app_id, &env_id, deploy_num)).await {
        Some(d) => d,
        None => return err_not_found("deployment not found").into_response(),
    };
    d.state = "ROLLED_BACK".into();
    if let Err(r) = save(&s.appconfig, &deployment_key(&app_id, &env_id, deploy_num), &d).await { return r; }
    json_ok(deployment_to_json(&d)).into_response()
}

// ── Data plane: configuration sessions ───────────────────────────────────────

async fn start_configuration_session(
    State(s): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "ApplicationIdentifier")] application_identifier: String,
        #[serde(rename = "EnvironmentIdentifier")] environment_identifier: String,
        #[serde(rename = "ConfigurationProfileIdentifier")] configuration_profile_identifier: String,
        #[serde(rename = "RequiredMinimumPollIntervalInSeconds")] required_minimum_poll_interval_in_seconds: Option<u32>,
    }
    let body: Body = match read_json(request).await { Ok(b) => b, Err(r) => return r };

    let token = Uuid::new_v4().to_string();
    let session = ConfigSession {
        token: token.clone(),
        application_id: body.application_identifier,
        environment_id: body.environment_identifier,
        configuration_profile_id: body.configuration_profile_identifier,
        client_configuration_version: None,
    };
    if let Err(r) = save(&s.appconfig, &session_key(&token), &session).await { return r; }

    json_created(serde_json::json!({ "InitialConfigurationToken": token })).into_response()
}

async fn get_latest_configuration(
    State(s): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    // Token may be in query param `configuration_token` (SDK) or in headers (legacy).
    let token = {
        // Try query parameter first (SDK sends it this way).
        let from_query = request.uri().query().and_then(|q| {
            serde_urlencoded::from_str::<std::collections::HashMap<String, String>>(q).ok()
                .and_then(|m| m.get("configuration_token").cloned())
        });
        // Fall back to header.
        let from_header = request
            .headers()
            .get("aws-token")
            .or_else(|| request.headers().get("appconfig-configuration-token"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        from_query.or(from_header)
    };

    let Some(token) = token else {
        return err_bad("missing configuration token header").into_response();
    };

    let mut session: ConfigSession = match load(&s.appconfig, &session_key(&token)).await {
        Some(s) => s,
        None => return err_not_found("configuration session not found").into_response(),
    };

    // Look up the currently deployed version.
    let deployed: Option<DeployedConfig> =
        load(&s.appconfig, &deployed_key(&session.application_id, &session.environment_id)).await;

    // Determine what to return BEFORE rotating the token so we can persist the
    // updated client_configuration_version in the new session.
    let (content, content_type, new_client_version) = match &deployed {
        None => {
            // No deployment yet — empty body.
            (vec![], "application/octet-stream".to_string(), session.client_configuration_version)
        }
        Some(dep) => {
            let current_version = dep.configuration_version;
            let client_version = session.client_configuration_version.unwrap_or(0);
            if client_version >= current_version {
                // Up to date — empty body.
                (vec![], "application/octet-stream".to_string(), Some(current_version))
            } else {
                // New version available — return content.
                let hcv: Option<HostedConfigurationVersion> = load(
                    &s.appconfig,
                    &hcv_key(&session.application_id, &dep.configuration_profile_id, current_version),
                ).await;
                let (bytes, ct) = if let Some(hcv) = hcv {
                    let bytes = base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        &hcv.content_b64,
                    ).unwrap_or_default();
                    (bytes, hcv.content_type)
                } else {
                    (vec![], "application/octet-stream".to_string())
                };
                (bytes, ct, Some(current_version))
            }
        }
    };

    // Rotate the token and persist the updated client version.
    let new_token = Uuid::new_v4().to_string();
    let mut new_session = session.clone();
    new_session.token = new_token.clone();
    new_session.client_configuration_version = new_client_version;
    if let Err(r) = save(&s.appconfig, &session_key(&new_token), &new_session).await { return r; }
    let _ = s.appconfig.delete(&session_key(&token)).await;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("next-poll-configuration-token"),
        HeaderValue::from_str(&new_token).unwrap(),
    );
    response_headers.insert(
        HeaderName::from_static("next-poll-interval-in-seconds"),
        HeaderValue::from(30u32),
    );
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type).unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );

    (StatusCode::OK, response_headers, content).into_response()
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Applications
        .route("/applications", get(list_applications).post(create_application))
        .route("/applications/{app}", get(get_application).patch(update_application).delete(delete_application))
        // Environments
        .route("/applications/{app}/environments", get(list_environments).post(create_environment))
        .route("/applications/{app}/environments/{env}", get(get_environment).patch(update_environment).delete(delete_environment))
        // Configuration profiles
        .route("/applications/{app}/configurationprofiles", get(list_profiles).post(create_profile))
        .route("/applications/{app}/configurationprofiles/{profile}", get(get_profile).patch(update_profile).delete(delete_profile))
        // Hosted configuration versions
        .route(
            "/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions",
            get(list_hosted_config_versions).post(create_hosted_config_version),
        )
        .route(
            "/applications/{app}/configurationprofiles/{profile}/hostedconfigurationversions/{version}",
            get(get_hosted_config_version).delete(delete_hosted_config_version),
        )
        // Deployment strategies
        .route("/deploymentstrategies", get(list_strategies).post(create_strategy))
        .route("/deploymentstrategies/{strategy}", get(get_strategy))
        // Deployments
        .route(
            "/applications/{app}/environments/{env}/deployments",
            get(list_deployments).post(start_deployment),
        )
        .route(
            "/applications/{app}/environments/{env}/deployments/{num}",
            get(get_deployment).delete(stop_deployment),
        )
        // Data plane
        .route("/configurationsessions", post(start_configuration_session))
        .route("/configuration", get(get_latest_configuration))
}
