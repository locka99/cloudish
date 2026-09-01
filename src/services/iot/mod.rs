//! IoT control plane service implementation.
//!
//! Wire format: REST/JSON.
//! Routing: path-based; SigV4 credential scope service = "iot".

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::services::AppState;
use crate::storage::Storage;

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct StoredThing {
    thing_name: String,
    thing_arn: String,
    thing_id: String,
    thing_type_name: Option<String>,
    attributes: HashMap<String, String>,
    version: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredThingType {
    thing_type_name: String,
    thing_type_arn: String,
    thing_type_id: String,
    description: Option<String>,
    searchable_attributes: Vec<String>,
    creation_date: f64,
    deprecated: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredCertificate {
    certificate_id: String,
    certificate_arn: String,
    certificate_pem: String,
    public_key: String,
    private_key: String,
    owned_by: String,
    creation_date: f64,
    last_modified_date: f64,
    status: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredPolicy {
    policy_name: String,
    policy_arn: String,
    policy_id: String,
    default_version_id: String,
    creation_date: f64,
    last_modified_date: f64,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredPolicyVersion {
    version_id: String,
    policy_document: String,
    is_default_version: bool,
    create_date: f64,
}

// ── Storage path helpers ─────────────────────────────────────────────────────

fn thing_path(name: &str) -> String {
    format!("things/{name}.json")
}

fn thing_type_path(name: &str) -> String {
    format!("thing_types/{name}.json")
}

fn cert_path(cert_id: &str) -> String {
    format!("certificates/{cert_id}.json")
}

fn policy_path(name: &str) -> String {
    format!("policies/{name}.json")
}

fn policy_version_path(name: &str, version_id: &str) -> String {
    format!("policy_versions/{name}/{version_id}.json")
}

fn policy_target_path(policy_name: &str, encoded_target: &str) -> String {
    format!("policy_targets/{policy_name}/{encoded_target}.json")
}

fn thing_principal_path(thing_name: &str, encoded_principal: &str) -> String {
    format!("thing_principals/{thing_name}/{encoded_principal}.json")
}

// ── Storage helpers ──────────────────────────────────────────────────────────

type IotStorage = Arc<crate::storage::file::FileStorage>;

async fn load_thing(iot: &IotStorage, name: &str) -> anyhow::Result<Option<StoredThing>> {
    match iot.get(&thing_path(name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_thing(iot: &IotStorage, t: &StoredThing) -> anyhow::Result<()> {
    iot.put(&thing_path(&t.thing_name), serde_json::to_vec(t)?).await
}

async fn list_things_all(iot: &IotStorage) -> anyhow::Result<Vec<StoredThing>> {
    let keys = iot.list("things/").await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = iot.get(&key).await {
            if let Ok(t) = serde_json::from_slice::<StoredThing>(&b) {
                result.push(t);
            }
        }
    }
    Ok(result)
}

async fn load_thing_type(iot: &IotStorage, name: &str) -> anyhow::Result<Option<StoredThingType>> {
    match iot.get(&thing_type_path(name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_thing_type(iot: &IotStorage, tt: &StoredThingType) -> anyhow::Result<()> {
    iot.put(&thing_type_path(&tt.thing_type_name), serde_json::to_vec(tt)?).await
}

async fn list_thing_types_all(iot: &IotStorage) -> anyhow::Result<Vec<StoredThingType>> {
    let keys = iot.list("thing_types/").await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = iot.get(&key).await {
            if let Ok(tt) = serde_json::from_slice::<StoredThingType>(&b) {
                result.push(tt);
            }
        }
    }
    Ok(result)
}

async fn load_cert(iot: &IotStorage, cert_id: &str) -> anyhow::Result<Option<StoredCertificate>> {
    match iot.get(&cert_path(cert_id)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_cert(iot: &IotStorage, c: &StoredCertificate) -> anyhow::Result<()> {
    iot.put(&cert_path(&c.certificate_id), serde_json::to_vec(c)?).await
}

async fn list_certs_all(iot: &IotStorage) -> anyhow::Result<Vec<StoredCertificate>> {
    let keys = iot.list("certificates/").await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = iot.get(&key).await {
            if let Ok(c) = serde_json::from_slice::<StoredCertificate>(&b) {
                result.push(c);
            }
        }
    }
    Ok(result)
}

async fn load_policy(iot: &IotStorage, name: &str) -> anyhow::Result<Option<StoredPolicy>> {
    match iot.get(&policy_path(name)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_policy_meta(iot: &IotStorage, p: &StoredPolicy) -> anyhow::Result<()> {
    iot.put(&policy_path(&p.policy_name), serde_json::to_vec(p)?).await
}

async fn list_policies_all(iot: &IotStorage) -> anyhow::Result<Vec<StoredPolicy>> {
    let keys = iot.list("policies/").await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        // Only top-level: policies/{name}.json (no subdirectory)
        let rest = key.strip_prefix("policies/").unwrap_or(&key);
        if rest.contains('/') {
            continue;
        }
        if let Ok(Some(b)) = iot.get(&key).await {
            if let Ok(p) = serde_json::from_slice::<StoredPolicy>(&b) {
                result.push(p);
            }
        }
    }
    Ok(result)
}

async fn load_policy_version(
    iot: &IotStorage,
    name: &str,
    version_id: &str,
) -> anyhow::Result<Option<StoredPolicyVersion>> {
    match iot.get(&policy_version_path(name, version_id)).await? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn save_policy_version(
    iot: &IotStorage,
    name: &str,
    pv: &StoredPolicyVersion,
) -> anyhow::Result<()> {
    iot.put(&policy_version_path(name, &pv.version_id), serde_json::to_vec(pv)?).await
}

async fn list_policy_versions_all(
    iot: &IotStorage,
    name: &str,
) -> anyhow::Result<Vec<StoredPolicyVersion>> {
    let prefix = format!("policy_versions/{name}/");
    let keys = iot.list(&prefix).await?;
    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = iot.get(&key).await {
            if let Ok(pv) = serde_json::from_slice::<StoredPolicyVersion>(&b) {
                result.push(pv);
            }
        }
    }
    Ok(result)
}

// ── Certificate generation ───────────────────────────────────────────────────

fn generate_certificate() -> anyhow::Result<(String, String, String)> {
    let subject_alt_names = vec!["cloudish.iot.local".to_string()];
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names)?;
    let cert_pem = cert.pem();
    let private_key_pem = key_pair.serialize_pem();
    let public_key_pem = key_pair.public_key_pem();
    Ok((cert_pem, public_key_pem, private_key_pem))
}

fn cert_id_from_pem(cert_pem: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cert_pem.as_bytes());
    hex::encode(hasher.finalize())
}

// ── ARN helpers ──────────────────────────────────────────────────────────────

fn thing_arn(name: &str) -> String {
    format!("arn:aws:iot:{REGION}:{ACCOUNT_ID}:thing/{name}")
}

fn cert_arn(cert_id: &str) -> String {
    format!("arn:aws:iot:{REGION}:{ACCOUNT_ID}:cert/{cert_id}")
}

fn policy_arn(name: &str) -> String {
    format!("arn:aws:iot:{REGION}:{ACCOUNT_ID}:policy/{name}")
}

fn thing_type_arn(name: &str) -> String {
    format!("arn:aws:iot:{REGION}:{ACCOUNT_ID}:thingtype/{name}")
}

// ── Time helpers ─────────────────────────────────────────────────────────────

fn now_unix_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

// ── Response helpers ─────────────────────────────────────────────────────────

fn ok_json(body: impl Serialize) -> impl IntoResponse {
    (StatusCode::OK, axum::Json(body))
}

fn created_json(body: impl Serialize) -> impl IntoResponse {
    (StatusCode::CREATED, axum::Json(body))
}

fn no_content() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

fn not_found(msg: &str) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({"message": msg})),
    )
}

fn bad_request(msg: &str) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(serde_json::json!({"message": msg})),
    )
}

fn conflict(msg: &str) -> impl IntoResponse {
    (
        StatusCode::CONFLICT,
        axum::Json(serde_json::json!({"message": msg})),
    )
}

fn internal(msg: &str) -> impl IntoResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({"message": msg})),
    )
}

// ── Body helper ──────────────────────────────────────────────────────────────

async fn read_body(request: Request) -> serde_json::Value {
    let bytes = to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({}))
    }
}

// ── Query params ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct CertQueryParams {
    #[serde(rename = "setAsActive")]
    set_as_active: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct UpdateCertQueryParams {
    #[serde(rename = "newStatus")]
    new_status: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct EndpointQueryParams {
    #[serde(rename = "endpointType")]
    endpoint_type: Option<String>,
}

// ── Thing handlers ───────────────────────────────────────────────────────────

async fn list_things(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match list_things_all(&state.iot).await {
        Ok(things) => {
            let arr: Vec<_> = things
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "thingName": t.thing_name,
                        "thingArn": t.thing_arn,
                        "thingTypeName": t.thing_type_name,
                        "attributes": t.attributes,
                        "version": t.version,
                    })
                })
                .collect();
            ok_json(serde_json::json!({"things": arr})).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn create_thing(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    // Check conflict
    match load_thing(&state.iot, &name).await {
        Ok(Some(_)) => return conflict(&format!("Thing already exists: {name}")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(None) => {}
    }

    let thing_type_name = body
        .get("thingTypeName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let attributes: HashMap<String, String> = body
        .get("attributePayload")
        .and_then(|a| a.get("attributes"))
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let t = StoredThing {
        thing_name: name.clone(),
        thing_arn: thing_arn(&name),
        thing_id: Uuid::new_v4().to_string(),
        thing_type_name,
        attributes,
        version: 1,
    };

    if let Err(e) = save_thing(&state.iot, &t).await {
        return internal(&e.to_string()).into_response();
    }

    created_json(serde_json::json!({
        "thingName": t.thing_name,
        "thingArn": t.thing_arn,
        "thingId": t.thing_id,
    }))
    .into_response()
}

async fn describe_thing(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_thing(&state.iot, &name).await {
        Ok(Some(t)) => ok_json(serde_json::json!({
            "thingName": t.thing_name,
            "thingArn": t.thing_arn,
            "thingId": t.thing_id,
            "thingTypeName": t.thing_type_name,
            "attributes": t.attributes,
            "version": t.version,
        }))
        .into_response(),
        Ok(None) => not_found(&format!("Thing {name} not found")).into_response(),
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn delete_thing(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_thing(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Thing {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    let _ = state.iot.delete(&thing_path(&name)).await;
    no_content().into_response()
}

async fn update_thing(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut t = match load_thing(&state.iot, &name).await {
        Ok(Some(t)) => t,
        Ok(None) => return not_found(&format!("Thing {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    if let Some(ttn) = body.get("thingTypeName").and_then(|v| v.as_str()) {
        t.thing_type_name = Some(ttn.to_string());
    }

    if let Some(new_attrs) = body
        .get("attributePayload")
        .and_then(|a| a.get("attributes"))
        .and_then(|v| v.as_object())
    {
        let merge = body
            .get("attributePayload")
            .and_then(|a| a.get("merge"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if merge {
            for (k, v) in new_attrs {
                if let Some(s) = v.as_str() {
                    t.attributes.insert(k.clone(), s.to_string());
                }
            }
        } else {
            t.attributes = new_attrs
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
    }

    t.version += 1;

    if let Err(e) = save_thing(&state.iot, &t).await {
        return internal(&e.to_string()).into_response();
    }

    ok_json(serde_json::json!({})).into_response()
}

// ── Thing principal handlers ─────────────────────────────────────────────────

async fn attach_thing_principal(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let principal = match headers
        .get("x-amzn-principal")
        .and_then(|v| v.to_str().ok())
    {
        Some(p) => p.to_string(),
        None => return bad_request("x-amzn-principal header required").into_response(),
    };

    // Verify thing exists
    match load_thing(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Thing {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    let encoded = urlencoding::encode(&principal).into_owned();
    let path = thing_principal_path(&name, &encoded);
    let record = serde_json::json!({"principal": principal, "thingName": name});

    if let Err(e) = state.iot.put(&path, serde_json::to_vec(&record).unwrap()).await {
        return internal(&e.to_string()).into_response();
    }

    ok_json(serde_json::json!({})).into_response()
}

async fn detach_thing_principal(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let principal = match headers
        .get("x-amzn-principal")
        .and_then(|v| v.to_str().ok())
    {
        Some(p) => p.to_string(),
        None => return bad_request("x-amzn-principal header required").into_response(),
    };

    let encoded = urlencoding::encode(&principal).into_owned();
    let path = thing_principal_path(&name, &encoded);
    let _ = state.iot.delete(&path).await;

    ok_json(serde_json::json!({})).into_response()
}

async fn list_thing_principals(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let prefix = format!("thing_principals/{name}/");
    let keys = match state.iot.list(&prefix).await {
        Ok(k) => k,
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    let mut principals = Vec::new();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = state.iot.get(&key).await {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                if let Some(p) = v.get("principal").and_then(|p| p.as_str()) {
                    principals.push(p.to_string());
                }
            }
        }
    }

    ok_json(serde_json::json!({"principals": principals})).into_response()
}

async fn list_principal_things(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let principal = match headers
        .get("x-amzn-principal")
        .and_then(|v| v.to_str().ok())
    {
        Some(p) => p.to_string(),
        None => return bad_request("x-amzn-principal header required").into_response(),
    };

    // Scan all thing_principals directories
    let all_keys = match state.iot.list("thing_principals/").await {
        Ok(k) => k,
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    let mut things = Vec::new();
    for key in all_keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = state.iot.get(&key).await {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                if let (Some(p), Some(tn)) = (
                    v.get("principal").and_then(|p| p.as_str()),
                    v.get("thingName").and_then(|t| t.as_str()),
                ) {
                    if p == principal {
                        things.push(tn.to_string());
                    }
                }
            }
        }
    }

    ok_json(serde_json::json!({"things": things})).into_response()
}

// ── Thing type handlers ──────────────────────────────────────────────────────

async fn list_thing_types(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match list_thing_types_all(&state.iot).await {
        Ok(types) => {
            let arr: Vec<_> = types
                .iter()
                .map(|tt| {
                    serde_json::json!({
                        "thingTypeName": tt.thing_type_name,
                        "thingTypeArn": tt.thing_type_arn,
                        "thingTypeId": tt.thing_type_id,
                        "thingTypeProperties": {
                            "thingTypeDescription": tt.description,
                            "searchableAttributes": tt.searchable_attributes,
                        },
                        "thingTypeMetadata": {
                            "deprecated": tt.deprecated,
                            "creationDate": tt.creation_date,
                        },
                    })
                })
                .collect();
            ok_json(serde_json::json!({"thingTypes": arr})).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn create_thing_type(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    match load_thing_type(&state.iot, &name).await {
        Ok(Some(_)) => return conflict(&format!("Thing type already exists: {name}")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(None) => {}
    }

    let props = body.get("thingTypeProperties");
    let description = props
        .and_then(|p| p.get("thingTypeDescription"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let searchable_attributes: Vec<String> = props
        .and_then(|p| p.get("searchableAttributes"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let tt = StoredThingType {
        thing_type_name: name.clone(),
        thing_type_arn: thing_type_arn(&name),
        thing_type_id: Uuid::new_v4().to_string(),
        description,
        searchable_attributes,
        creation_date: now_unix_f64(),
        deprecated: false,
    };

    if let Err(e) = save_thing_type(&state.iot, &tt).await {
        return internal(&e.to_string()).into_response();
    }

    created_json(serde_json::json!({
        "thingTypeName": tt.thing_type_name,
        "thingTypeArn": tt.thing_type_arn,
        "thingTypeId": tt.thing_type_id,
    }))
    .into_response()
}

async fn describe_thing_type(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_thing_type(&state.iot, &name).await {
        Ok(Some(tt)) => ok_json(serde_json::json!({
            "thingTypeName": tt.thing_type_name,
            "thingTypeArn": tt.thing_type_arn,
            "thingTypeId": tt.thing_type_id,
            "thingTypeProperties": {
                "thingTypeDescription": tt.description,
                "searchableAttributes": tt.searchable_attributes,
            },
            "thingTypeMetadata": {
                "deprecated": tt.deprecated,
                "creationDate": tt.creation_date,
            },
        }))
        .into_response(),
        Ok(None) => not_found(&format!("Thing type {name} not found")).into_response(),
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn delete_thing_type(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_thing_type(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Thing type {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    let _ = state.iot.delete(&thing_type_path(&name)).await;
    no_content().into_response()
}

// ── Certificate handlers ─────────────────────────────────────────────────────

async fn create_keys_and_certificate(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CertQueryParams>,
) -> impl IntoResponse {
    let (cert_pem, public_key, private_key) = match generate_certificate() {
        Ok(v) => v,
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    let certificate_id = cert_id_from_pem(&cert_pem);
    let status = if params
        .set_as_active
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        "ACTIVE"
    } else {
        "INACTIVE"
    };

    let now = now_unix_f64();
    let cert = StoredCertificate {
        certificate_id: certificate_id.clone(),
        certificate_arn: cert_arn(&certificate_id),
        certificate_pem: cert_pem.clone(),
        public_key: public_key.clone(),
        private_key: private_key.clone(),
        owned_by: ACCOUNT_ID.to_string(),
        creation_date: now,
        last_modified_date: now,
        status: status.to_string(),
    };

    if let Err(e) = save_cert(&state.iot, &cert).await {
        return internal(&e.to_string()).into_response();
    }

    ok_json(serde_json::json!({
        "certificateArn": cert.certificate_arn,
        "certificateId": cert.certificate_id,
        "certificatePem": cert_pem,
        "keyPair": {
            "PublicKey": public_key,
            "PrivateKey": private_key,
        },
    }))
    .into_response()
}

async fn list_certificates(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match list_certs_all(&state.iot).await {
        Ok(certs) => {
            let arr: Vec<_> = certs
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "certificateArn": c.certificate_arn,
                        "certificateId": c.certificate_id,
                        "status": c.status,
                        "creationDate": c.creation_date,
                    })
                })
                .collect();
            ok_json(serde_json::json!({"certificates": arr})).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn describe_certificate(
    State(state): State<Arc<AppState>>,
    Path(cert_id): Path<String>,
) -> impl IntoResponse {
    match load_cert(&state.iot, &cert_id).await {
        Ok(Some(c)) => ok_json(serde_json::json!({
            "certificateDescription": {
                "certificateArn": c.certificate_arn,
                "certificateId": c.certificate_id,
                "status": c.status,
                "certificatePem": c.certificate_pem,
                "ownedBy": c.owned_by,
                "creationDate": c.creation_date,
                "lastModifiedDate": c.last_modified_date,
            }
        }))
        .into_response(),
        Ok(None) => not_found(&format!("Certificate {cert_id} not found")).into_response(),
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn delete_certificate(
    State(state): State<Arc<AppState>>,
    Path(cert_id): Path<String>,
) -> impl IntoResponse {
    match load_cert(&state.iot, &cert_id).await {
        Ok(None) => return not_found(&format!("Certificate {cert_id} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    let _ = state.iot.delete(&cert_path(&cert_id)).await;
    no_content().into_response()
}

async fn update_certificate(
    State(state): State<Arc<AppState>>,
    Path(cert_id): Path<String>,
    Query(params): Query<UpdateCertQueryParams>,
) -> impl IntoResponse {
    let mut c = match load_cert(&state.iot, &cert_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return not_found(&format!("Certificate {cert_id} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    if let Some(new_status) = params.new_status {
        c.status = new_status;
    }
    c.last_modified_date = now_unix_f64();

    if let Err(e) = save_cert(&state.iot, &c).await {
        return internal(&e.to_string()).into_response();
    }

    no_content().into_response()
}

// ── Policy handlers ──────────────────────────────────────────────────────────

async fn create_policy(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    match load_policy(&state.iot, &name).await {
        Ok(Some(_)) => return conflict(&format!("Policy already exists: {name}")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(None) => {}
    }

    let policy_document = body
        .get("policyDocument")
        .and_then(|v| v.as_str())
        .unwrap_or("{}")
        .to_string();

    let now = now_unix_f64();
    let p = StoredPolicy {
        policy_name: name.clone(),
        policy_arn: policy_arn(&name),
        policy_id: Uuid::new_v4().to_string(),
        default_version_id: "1".to_string(),
        creation_date: now,
        last_modified_date: now,
    };

    if let Err(e) = save_policy_meta(&state.iot, &p).await {
        return internal(&e.to_string()).into_response();
    }

    // Save version 1
    let pv = StoredPolicyVersion {
        version_id: "1".to_string(),
        policy_document: policy_document.clone(),
        is_default_version: true,
        create_date: now,
    };

    if let Err(e) = save_policy_version(&state.iot, &name, &pv).await {
        return internal(&e.to_string()).into_response();
    }

    created_json(serde_json::json!({
        "policyName": p.policy_name,
        "policyArn": p.policy_arn,
        "policyDocument": policy_document,
        "policyVersionId": "1",
    }))
    .into_response()
}

async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let p = match load_policy(&state.iot, &name).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    // Load default version document
    let doc = match load_policy_version(&state.iot, &name, &p.default_version_id).await {
        Ok(Some(pv)) => pv.policy_document,
        _ => "{}".to_string(),
    };

    ok_json(serde_json::json!({
        "policyName": p.policy_name,
        "policyArn": p.policy_arn,
        "policyDocument": doc,
        "defaultVersionId": p.default_version_id,
        "creationDate": p.creation_date,
        "lastModifiedDate": p.last_modified_date,
    }))
    .into_response()
}

async fn list_policies(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match list_policies_all(&state.iot).await {
        Ok(policies) => {
            let arr: Vec<_> = policies
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "policyName": p.policy_name,
                        "policyArn": p.policy_arn,
                    })
                })
                .collect();
            ok_json(serde_json::json!({"policies": arr})).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn delete_policy(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_policy(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    let _ = state.iot.delete(&policy_path(&name)).await;
    no_content().into_response()
}

async fn create_policy_version(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;

    let mut p = match load_policy(&state.iot, &name).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    let policy_document = body
        .get("policyDocument")
        .and_then(|v| v.as_str())
        .unwrap_or("{}")
        .to_string();

    let set_as_default = body
        .get("setAsDefault")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Compute next version ID
    let existing_versions = match list_policy_versions_all(&state.iot, &name).await {
        Ok(v) => v,
        Err(e) => return internal(&e.to_string()).into_response(),
    };
    let max_version: u64 = existing_versions
        .iter()
        .filter_map(|v| v.version_id.parse::<u64>().ok())
        .max()
        .unwrap_or(0);
    let new_version_id = (max_version + 1).to_string();

    let now = now_unix_f64();
    let pv = StoredPolicyVersion {
        version_id: new_version_id.clone(),
        policy_document: policy_document.clone(),
        is_default_version: set_as_default,
        create_date: now,
    };

    if let Err(e) = save_policy_version(&state.iot, &name, &pv).await {
        return internal(&e.to_string()).into_response();
    }

    if set_as_default {
        // Update old default
        let old_default = p.default_version_id.clone();
        if let Ok(Some(mut old_pv)) = load_policy_version(&state.iot, &name, &old_default).await {
            old_pv.is_default_version = false;
            let _ = save_policy_version(&state.iot, &name, &old_pv).await;
        }
        p.default_version_id = new_version_id.clone();
        p.last_modified_date = now;
        if let Err(e) = save_policy_meta(&state.iot, &p).await {
            return internal(&e.to_string()).into_response();
        }
    }

    created_json(serde_json::json!({
        "policyArn": p.policy_arn,
        "policyDocument": policy_document,
        "policyVersionId": new_version_id,
        "isDefaultVersion": set_as_default,
    }))
    .into_response()
}

async fn list_policy_versions(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match load_policy(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    match list_policy_versions_all(&state.iot, &name).await {
        Ok(versions) => {
            let arr: Vec<_> = versions
                .iter()
                .map(|pv| {
                    serde_json::json!({
                        "versionId": pv.version_id,
                        "isDefaultVersion": pv.is_default_version,
                        "createDate": pv.create_date,
                    })
                })
                .collect();
            ok_json(serde_json::json!({"policyVersions": arr})).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn get_policy_version(
    State(state): State<Arc<AppState>>,
    Path((name, version_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let p = match load_policy(&state.iot, &name).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    match load_policy_version(&state.iot, &name, &version_id).await {
        Ok(Some(pv)) => ok_json(serde_json::json!({
            "policyArn": p.policy_arn,
            "policyName": p.policy_name,
            "policyDocument": pv.policy_document,
            "policyVersionId": pv.version_id,
            "isDefaultVersion": pv.is_default_version,
            "creationDate": pv.create_date,
            "lastModifiedDate": pv.create_date,
            "generationId": pv.version_id,
        }))
        .into_response(),
        Ok(None) => {
            not_found(&format!("Policy version {version_id} not found")).into_response()
        }
        Err(e) => internal(&e.to_string()).into_response(),
    }
}

async fn delete_policy_version(
    State(state): State<Arc<AppState>>,
    Path((name, version_id)): Path<(String, String)>,
) -> impl IntoResponse {
    match load_policy(&state.iot, &name).await {
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(p)) => {
            if p.default_version_id == version_id {
                return bad_request("Cannot delete the default policy version").into_response();
            }
        }
    }

    match load_policy_version(&state.iot, &name, &version_id).await {
        Ok(None) => {
            return not_found(&format!("Policy version {version_id} not found")).into_response()
        }
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    let _ = state
        .iot
        .delete(&policy_version_path(&name, &version_id))
        .await;
    no_content().into_response()
}

async fn set_default_policy_version(
    State(state): State<Arc<AppState>>,
    Path((name, version_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut p = match load_policy(&state.iot, &name).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found(&format!("Policy {name} not found")).into_response(),
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    // Verify version exists
    match load_policy_version(&state.iot, &name, &version_id).await {
        Ok(None) => {
            return not_found(&format!("Policy version {version_id} not found")).into_response()
        }
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    // Unmark old default
    let old_default = p.default_version_id.clone();
    if old_default != version_id {
        if let Ok(Some(mut old_pv)) = load_policy_version(&state.iot, &name, &old_default).await {
            old_pv.is_default_version = false;
            let _ = save_policy_version(&state.iot, &name, &old_pv).await;
        }
        // Mark new default
        if let Ok(Some(mut new_pv)) = load_policy_version(&state.iot, &name, &version_id).await {
            new_pv.is_default_version = true;
            let _ = save_policy_version(&state.iot, &name, &new_pv).await;
        }
        p.default_version_id = version_id.clone();
        p.last_modified_date = now_unix_f64();
        if let Err(e) = save_policy_meta(&state.iot, &p).await {
            return internal(&e.to_string()).into_response();
        }
    }

    no_content().into_response()
}

// ── Policy attachment handlers ───────────────────────────────────────────────

async fn attach_policy(
    State(state): State<Arc<AppState>>,
    Path(policy_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;
    let target = match body.get("target").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return bad_request("target is required").into_response(),
    };

    match load_policy(&state.iot, &policy_name).await {
        Ok(None) => {
            return not_found(&format!("Policy {policy_name} not found")).into_response()
        }
        Err(e) => return internal(&e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }

    let encoded = urlencoding::encode(&target).into_owned();
    let path = policy_target_path(&policy_name, &encoded);
    let record = serde_json::json!({"policyName": policy_name, "target": target});

    if let Err(e) = state.iot.put(&path, serde_json::to_vec(&record).unwrap()).await {
        return internal(&e.to_string()).into_response();
    }

    ok_json(serde_json::json!({})).into_response()
}

async fn detach_policy(
    State(state): State<Arc<AppState>>,
    Path(policy_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    let body = read_body(request).await;
    let target = match body.get("target").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return bad_request("target is required").into_response(),
    };

    let encoded = urlencoding::encode(&target).into_owned();
    let path = policy_target_path(&policy_name, &encoded);
    let _ = state.iot.delete(&path).await;

    ok_json(serde_json::json!({})).into_response()
}

async fn list_attached_policies(
    State(state): State<Arc<AppState>>,
    Path(encoded_target): Path<String>,
) -> impl IntoResponse {
    let target = urlencoding::decode(&encoded_target)
        .unwrap_or_else(|_| std::borrow::Cow::Borrowed(&encoded_target))
        .into_owned();

    // Scan all policy_targets directories
    let all_keys = match state.iot.list("policy_targets/").await {
        Ok(k) => k,
        Err(e) => return internal(&e.to_string()).into_response(),
    };

    let mut policies = Vec::new();
    for key in all_keys {
        if !key.ends_with(".json") {
            continue;
        }
        if let Ok(Some(b)) = state.iot.get(&key).await {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                if v.get("target").and_then(|t| t.as_str()) == Some(&target) {
                    if let Some(pname) = v.get("policyName").and_then(|p| p.as_str()) {
                        policies.push(serde_json::json!({
                            "policyName": pname,
                            "policyArn": policy_arn(pname),
                        }));
                    }
                }
            }
        }
    }

    ok_json(serde_json::json!({"policies": policies})).into_response()
}

// ── Endpoint handler ─────────────────────────────────────────────────────────

async fn get_endpoint(
    State(_): State<Arc<AppState>>,
    Query(_params): Query<EndpointQueryParams>,
) -> impl IntoResponse {
    ok_json(serde_json::json!({"endpointAddress": "localhost:8883"}))
}

// ── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Things
        .route("/things", get(list_things))
        .route("/things/{name}", post(create_thing))
        .route("/things/{name}", get(describe_thing))
        .route("/things/{name}", delete(delete_thing))
        .route("/things/{name}", patch(update_thing))
        .route("/things/{name}/principals", put(attach_thing_principal))
        .route("/things/{name}/principals", delete(detach_thing_principal))
        .route("/things/{name}/principals", get(list_thing_principals))
        // Thing types
        .route("/thing-types", get(list_thing_types))
        .route("/thing-types/{name}", post(create_thing_type))
        .route("/thing-types/{name}", get(describe_thing_type))
        .route("/thing-types/{name}", delete(delete_thing_type))
        // Certificates
        .route("/keys-and-certificate", post(create_keys_and_certificate))
        .route("/certificates", get(list_certificates))
        .route("/certificates/{cert_id}", get(describe_certificate))
        .route("/certificates/{cert_id}", delete(delete_certificate))
        .route("/certificates/{cert_id}", put(update_certificate))
        // Policies
        .route("/policies", get(list_policies))
        .route("/policies/{name}", post(create_policy))
        .route("/policies/{name}", get(get_policy))
        .route("/policies/{name}", delete(delete_policy))
        .route("/policies/{name}/versions", post(create_policy_version))
        .route("/policies/{name}/versions", get(list_policy_versions))
        .route("/policies/{name}/versions/{version_id}", get(get_policy_version))
        .route("/policies/{name}/versions/{version_id}", delete(delete_policy_version))
        .route("/policies/{name}/versions/{version_id}", patch(set_default_policy_version))
        // Policy attachments
        .route("/target-policies/{policy_name}", put(attach_policy))
        .route("/target-policies/{policy_name}", post(detach_policy))
        .route("/attached-policies/{encoded_target}", post(list_attached_policies))
        // Principal things
        .route("/principal-things", get(list_principal_things))
        // Endpoint
        .route("/endpoint", get(get_endpoint))
}
