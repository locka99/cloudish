//! IAM service emulator (basics).
//!
//! Wire format: POST with `application/x-www-form-urlencoded` body,
//! `Action` field selects the operation. Responses are XML.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use serde::{Deserialize, Serialize};

use crate::{auth::Credentials, services::AppState, storage::Storage};

// ── Constants ───────────────────────────────────────────────────────────────

const ACCOUNT_ID: &str = "000000000000";
const NS: &str = "https://iam.amazonaws.com/doc/2010-05-08/";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

// ── Data structures ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tag {
    key: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IamUser {
    user_name: String,
    user_id: String,
    arn: String,
    path: String,
    created_date: String,
    tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccessKey {
    access_key_id: String,
    secret_access_key: String,
    status: String,
    user_name: String,
    created_date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IamRole {
    role_name: String,
    role_id: String,
    arn: String,
    path: String,
    assume_role_policy_document: String,
    created_date: String,
    description: Option<String>,
    tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedPolicy {
    policy_name: String,
    policy_id: String,
    arn: String,
    path: String,
    document: String,
    created_date: String,
    updated_date: String,
}

// ── ID / secret generation ──────────────────────────────────────────────────

fn random_id(prefix: &str) -> String {
    use rand::Rng;
    let chars: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let suffix: String = (0..16)
        .map(|_| chars[rng.gen_range(0..chars.len())] as char)
        .collect();
    format!("{prefix}{suffix}")
}

fn random_secret() -> String {
    use rand::Rng;
    let chars: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
    let mut rng = rand::thread_rng();
    (0..40)
        .map(|_| chars[rng.gen_range(0..chars.len())] as char)
        .collect()
}

fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

// ── XML helpers ──────────────────────────────────────────────────────────────

fn response_metadata() -> String {
    format!("<ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>")
}

type XmlResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn xml_response(op: &str, inner: &str) -> XmlResponse {
    let body = format!(
        r#"<{op}Response xmlns="{NS}">{inner}{meta}</{op}Response>"#,
        meta = response_metadata()
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_error(code: StatusCode, error_code: &str, message: &str) -> XmlResponse {
    let body = format!(
        r#"<ErrorResponse xmlns="{NS}"><Error><Code>{error_code}</Code><Message>{message}</Message></Error>{meta}</ErrorResponse>"#,
        meta = response_metadata()
    );
    (code, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn not_found(entity: &str) -> XmlResponse {
    xml_error(
        StatusCode::NOT_FOUND,
        "NoSuchEntity",
        &format!("{entity} not found"),
    )
}

fn already_exists(entity: &str) -> XmlResponse {
    xml_error(
        StatusCode::CONFLICT,
        "EntityAlreadyExists",
        &format!("{entity} already exists"),
    )
}

// ── XML element builders ─────────────────────────────────────────────────────

/// Returns the inner fields of a user XML element (without a wrapping tag).
fn user_fields_xml(u: &IamUser) -> String {
    let tags_xml = if u.tags.is_empty() {
        String::new()
    } else {
        let members: String = u
            .tags
            .iter()
            .map(|t| format!("<member><Key>{}</Key><Value>{}</Value></member>", t.key, t.value))
            .collect();
        format!("<Tags>{members}</Tags>")
    };
    format!(
        "<Path>{}</Path>\
          <UserName>{}</UserName>\
          <UserId>{}</UserId>\
          <Arn>{}</Arn>\
          <CreateDate>{}</CreateDate>\
          {tags_xml}",
        u.path, u.user_name, u.user_id, u.arn, u.created_date
    )
}

/// Returns `<User>...</User>` for single-user responses (CreateUser, GetUser).
fn user_xml(u: &IamUser) -> String {
    format!("<User>{}</User>", user_fields_xml(u))
}

/// Returns `<member>...</member>` for list responses (ListUsers).
fn user_member_xml(u: &IamUser) -> String {
    format!("<member>{}</member>", user_fields_xml(u))
}

fn role_fields_xml(r: &IamRole) -> String {
    let desc = r
        .description
        .as_deref()
        .map(|d| format!("<Description>{d}</Description>"))
        .unwrap_or_default();
    let encoded_doc = urlencoding::encode(&r.assume_role_policy_document).into_owned();
    format!(
        "<Path>{}</Path>\
          <RoleName>{}</RoleName>\
          <RoleId>{}</RoleId>\
          <Arn>{}</Arn>\
          <CreateDate>{}</CreateDate>\
          <AssumeRolePolicyDocument>{encoded_doc}</AssumeRolePolicyDocument>\
          {desc}",
        r.path, r.role_name, r.role_id, r.arn, r.created_date
    )
}

fn role_xml(r: &IamRole) -> String {
    format!("<Role>{}</Role>", role_fields_xml(r))
}

fn role_member_xml(r: &IamRole) -> String {
    format!("<member>{}</member>", role_fields_xml(r))
}

fn policy_fields_xml(p: &ManagedPolicy) -> String {
    format!(
        "<PolicyName>{}</PolicyName>\
          <PolicyId>{}</PolicyId>\
          <Arn>{}</Arn>\
          <Path>{}</Path>\
          <DefaultVersionId>v1</DefaultVersionId>\
          <AttachmentCount>0</AttachmentCount>\
          <IsAttachable>true</IsAttachable>\
          <CreateDate>{}</CreateDate>\
          <UpdateDate>{}</UpdateDate>",
        p.policy_name, p.policy_id, p.arn, p.path, p.created_date, p.updated_date
    )
}

fn policy_xml(p: &ManagedPolicy) -> String {
    format!("<Policy>{}</Policy>", policy_fields_xml(p))
}

fn policy_member_xml(p: &ManagedPolicy) -> String {
    format!("<member>{}</member>", policy_fields_xml(p))
}

// ── Tag parsing ──────────────────────────────────────────────────────────────

fn parse_tags(params: &HashMap<String, String>) -> Vec<Tag> {
    let mut tags = Vec::new();
    let mut i = 1;
    loop {
        let key = params.get(&format!("Tags.member.{i}.Key"));
        let val = params.get(&format!("Tags.member.{i}.Value"));
        match (key, val) {
            (Some(k), Some(v)) => tags.push(Tag {
                key: k.clone(),
                value: v.clone(),
            }),
            _ => break,
        }
        i += 1;
    }
    tags
}

fn parse_tag_keys(params: &HashMap<String, String>) -> Vec<String> {
    let mut keys = Vec::new();
    let mut i = 1;
    loop {
        match params.get(&format!("TagKeys.member.{i}")) {
            Some(k) => keys.push(k.clone()),
            None => break,
        }
        i += 1;
    }
    keys
}

// ── Storage path helpers ─────────────────────────────────────────────────────

fn user_meta_path(user_name: &str) -> String {
    format!("users/{user_name}/_meta.json")
}

fn user_key_path(user_name: &str, key_id: &str) -> String {
    format!("users/{user_name}/keys/{key_id}.json")
}

fn user_policy_path(user_name: &str, policy_name: &str) -> String {
    format!("users/{user_name}/policies/{policy_name}.json")
}

fn user_attached_path(user_name: &str, policy_arn: &str) -> String {
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        policy_arn,
    );
    format!("users/{user_name}/attached/{encoded}")
}

fn role_meta_path(role_name: &str) -> String {
    format!("roles/{role_name}/_meta.json")
}

fn role_policy_path(role_name: &str, policy_name: &str) -> String {
    format!("roles/{role_name}/policies/{policy_name}.json")
}

fn role_attached_path(role_name: &str, policy_arn: &str) -> String {
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        policy_arn,
    );
    format!("roles/{role_name}/attached/{encoded}")
}

fn policy_name_from_arn(policy_arn: &str) -> Option<&str> {
    policy_arn.split(':').last()?.split('/').last()
}

fn policy_meta_path(policy_name: &str) -> String {
    format!("policies/{policy_name}/_meta.json")
}

fn is_user_meta(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    parts.len() == 3 && parts[0] == "users" && parts[2] == "_meta.json"
}

fn is_role_meta(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    parts.len() == 3 && parts[0] == "roles" && parts[2] == "_meta.json"
}

fn is_policy_meta(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    parts.len() == 3 && parts[0] == "policies" && parts[2] == "_meta.json"
}

// ── Storage helpers ──────────────────────────────────────────────────────────

async fn load_user(
    iam: &Arc<crate::storage::file::FileStorage>,
    user_name: &str,
) -> anyhow::Result<Option<IamUser>> {
    match iam.get(&user_meta_path(user_name)).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

async fn save_user(
    iam: &Arc<crate::storage::file::FileStorage>,
    user: &IamUser,
) -> anyhow::Result<()> {
    iam.put(
        &user_meta_path(&user.user_name),
        serde_json::to_vec(user)?,
    )
    .await
}

async fn load_role(
    iam: &Arc<crate::storage::file::FileStorage>,
    role_name: &str,
) -> anyhow::Result<Option<IamRole>> {
    match iam.get(&role_meta_path(role_name)).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

async fn save_role(
    iam: &Arc<crate::storage::file::FileStorage>,
    role: &IamRole,
) -> anyhow::Result<()> {
    iam.put(
        &role_meta_path(&role.role_name),
        serde_json::to_vec(role)?,
    )
    .await
}

async fn load_policy(
    iam: &Arc<crate::storage::file::FileStorage>,
    policy_name: &str,
) -> anyhow::Result<Option<ManagedPolicy>> {
    match iam.get(&policy_meta_path(policy_name)).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

async fn save_policy(
    iam: &Arc<crate::storage::file::FileStorage>,
    policy: &ManagedPolicy,
) -> anyhow::Result<()> {
    iam.put(
        &policy_meta_path(&policy.policy_name),
        serde_json::to_vec(policy)?,
    )
    .await
}

// ── Handlers ─────────────────────────────────────────────────────────────────

// --- User operations ---

async fn create_user(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let path = params.get("Path").cloned().unwrap_or_else(|| "/".to_string());
    let tags = parse_tags(params);

    match load_user(&state.iam, &user_name).await {
        Ok(Some(_)) => return already_exists(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(None) => {}
    }

    let user = IamUser {
        user_name: user_name.clone(),
        user_id: random_id("AIDA"),
        arn: format!("arn:aws:iam::{ACCOUNT_ID}:user/{user_name}"),
        path,
        created_date: now_iso8601(),
        tags,
    };

    if let Err(e) = save_user(&state.iam, &user).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    let result = format!("<CreateUserResult>{}</CreateUserResult>", user_xml(&user));
    xml_response("CreateUser", &result)
}

async fn get_user(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
    creds: Option<&Credentials>,
) -> XmlResponse {
    let user_name = params
        .get("UserName")
        .cloned()
        .or_else(|| creds.map(|c| c.access_key.clone()))
        .unwrap_or_else(|| "test".to_string());

    // Synthetic user for "test" access key
    if user_name == "test" {
        let synthetic = IamUser {
            user_name: "test".to_string(),
            user_id: "AIDATESTUSER00001".to_string(),
            arn: format!("arn:aws:iam::{ACCOUNT_ID}:user/test"),
            path: "/".to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            tags: vec![],
        };
        let result = format!("<GetUserResult>{}</GetUserResult>", user_xml(&synthetic));
        return xml_response("GetUser", &result);
    }

    match load_user(&state.iam, &user_name).await {
        Ok(Some(user)) => {
            let result = format!("<GetUserResult>{}</GetUserResult>", user_xml(&user));
            xml_response("GetUser", &result)
        }
        Ok(None) => not_found(&format!("User {user_name}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

async fn list_users(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let path_prefix = params.get("PathPrefix").cloned().unwrap_or_else(|| "/".to_string());
    let max_items: usize = params
        .get("MaxItems")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let all_paths = match state.iam.list("users/").await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut users_xml = String::new();
    let mut count = 0;
    for path in all_paths.iter().filter(|p| is_user_meta(p)) {
        if count >= max_items {
            break;
        }
        if let Ok(Some(bytes)) = state.iam.get(path).await {
            if let Ok(user) = serde_json::from_slice::<IamUser>(&bytes) {
                if user.path.starts_with(&path_prefix) {
                    users_xml.push_str(&user_member_xml(&user));
                    count += 1;
                }
            }
        }
    }

    let result = format!("<ListUsersResult><Users>{users_xml}</Users><IsTruncated>false</IsTruncated></ListUsersResult>");
    xml_response("ListUsers", &result)
}

async fn update_user(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let new_user_name = params.get("NewUserName").cloned();
    let new_path = params.get("NewPath").cloned();

    let mut user = match load_user(&state.iam, &user_name).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    if let Some(new_name) = new_user_name {
        // Check new name doesn't exist
        match load_user(&state.iam, &new_name).await {
            Ok(Some(_)) => return already_exists(&format!("User {new_name}")),
            Err(e) => {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalFailure",
                    &e.to_string(),
                )
            }
            Ok(None) => {}
        }
        // Delete old entry
        let _ = state.iam.delete(&user_meta_path(&user.user_name)).await;
        user.user_name = new_name.clone();
        user.arn = format!("arn:aws:iam::{ACCOUNT_ID}:user/{new_name}");
    }

    if let Some(path) = new_path {
        user.path = path;
    }

    if let Err(e) = save_user(&state.iam, &user).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("UpdateUser", "")
}

async fn delete_user(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    match load_user(&state.iam, &user_name).await {
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    if let Err(e) = state.iam.delete(&user_meta_path(&user_name)).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeleteUser", "")
}

async fn tag_user(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let new_tags = parse_tags(params);

    let mut user = match load_user(&state.iam, &user_name).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    for new_tag in new_tags {
        if let Some(existing) = user.tags.iter_mut().find(|t| t.key == new_tag.key) {
            existing.value = new_tag.value;
        } else {
            user.tags.push(new_tag);
        }
    }

    if let Err(e) = save_user(&state.iam, &user).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("TagUser", "")
}

async fn untag_user(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let keys_to_remove = parse_tag_keys(params);

    let mut user = match load_user(&state.iam, &user_name).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    user.tags.retain(|t| !keys_to_remove.contains(&t.key));

    if let Err(e) = save_user(&state.iam, &user).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("UntagUser", "")
}

async fn list_user_tags(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    let user = match load_user(&state.iam, &user_name).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let tags_xml: String = user
        .tags
        .iter()
        .map(|t| format!("<member><Key>{}</Key><Value>{}</Value></member>", t.key, t.value))
        .collect();
    let result = format!("<ListUserTagsResult><Tags>{tags_xml}</Tags><IsTruncated>false</IsTruncated></ListUserTagsResult>");
    xml_response("ListUserTags", &result)
}

// --- Access key operations ---

async fn create_access_key(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    match load_user(&state.iam, &user_name).await {
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    let key = AccessKey {
        access_key_id: random_id("AKIA"),
        secret_access_key: random_secret(),
        status: "Active".to_string(),
        user_name: user_name.clone(),
        created_date: now_iso8601(),
    };

    let path = user_key_path(&user_name, &key.access_key_id);
    if let Err(e) = state.iam.put(&path, serde_json::to_vec(&key).unwrap()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    let result = format!(
        "<CreateAccessKeyResult><AccessKey>\
          <UserName>{}</UserName>\
          <AccessKeyId>{}</AccessKeyId>\
          <Status>{}</Status>\
          <SecretAccessKey>{}</SecretAccessKey>\
          <CreateDate>{}</CreateDate>\
        </AccessKey></CreateAccessKeyResult>",
        key.user_name, key.access_key_id, key.status, key.secret_access_key, key.created_date
    );
    xml_response("CreateAccessKey", &result)
}

async fn list_access_keys(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    let prefix = format!("users/{user_name}/keys/");
    let all_paths = match state.iam.list(&prefix).await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut keys_xml = String::new();
    for path in all_paths.iter().filter(|p| p.ends_with(".json")) {
        if let Ok(Some(bytes)) = state.iam.get(path).await {
            if let Ok(key) = serde_json::from_slice::<AccessKey>(&bytes) {
                keys_xml.push_str(&format!(
                    "<member>\
                      <UserName>{}</UserName>\
                      <AccessKeyId>{}</AccessKeyId>\
                      <Status>{}</Status>\
                      <CreateDate>{}</CreateDate>\
                    </member>",
                    key.user_name, key.access_key_id, key.status, key.created_date
                ));
            }
        }
    }

    let result = format!("<ListAccessKeysResult><AccessKeyMetadata>{keys_xml}</AccessKeyMetadata><IsTruncated>false</IsTruncated></ListAccessKeysResult>");
    xml_response("ListAccessKeys", &result)
}

async fn update_access_key(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let key_id = match params.get("AccessKeyId") {
        Some(k) => k.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "AccessKeyId is required",
            )
        }
    };
    let status = match params.get("Status") {
        Some(s) => s.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "Status is required"),
    };

    let path = user_key_path(&user_name, &key_id);
    let mut key: AccessKey = match state.iam.get(&path).await {
        Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
            Ok(k) => k,
            Err(e) => {
                return xml_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalFailure",
                    &e.to_string(),
                )
            }
        },
        Ok(None) => return not_found(&format!("AccessKey {key_id}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    key.status = status;
    if let Err(e) = state.iam.put(&path, serde_json::to_vec(&key).unwrap()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("UpdateAccessKey", "")
}

async fn delete_access_key(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let key_id = match params.get("AccessKeyId") {
        Some(k) => k.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "AccessKeyId is required",
            )
        }
    };

    let path = user_key_path(&user_name, &key_id);
    if let Err(e) = state.iam.delete(&path).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeleteAccessKey", "")
}

// --- Inline user policy operations ---

async fn put_user_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };
    let policy_doc = match params.get("PolicyDocument") {
        Some(d) => d.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyDocument is required",
            )
        }
    };

    // Validate JSON
    if serde_json::from_str::<serde_json::Value>(&policy_doc).is_err() {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedPolicyDocument",
            "PolicyDocument is not valid JSON",
        );
    }

    match load_user(&state.iam, &user_name).await {
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    let path = user_policy_path(&user_name, &policy_name);
    if let Err(e) = state.iam.put(&path, policy_doc.into_bytes()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("PutUserPolicy", "")
}

async fn get_user_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };

    let path = user_policy_path(&user_name, &policy_name);
    match state.iam.get(&path).await {
        Ok(Some(bytes)) => {
            let doc = String::from_utf8_lossy(&bytes);
            let encoded = urlencoding::encode(&doc).into_owned();
            let result = format!(
                "<GetUserPolicyResult>\
                  <UserName>{user_name}</UserName>\
                  <PolicyName>{policy_name}</PolicyName>\
                  <PolicyDocument>{encoded}</PolicyDocument>\
                </GetUserPolicyResult>"
            );
            xml_response("GetUserPolicy", &result)
        }
        Ok(None) => not_found(&format!("UserPolicy {policy_name} on User {user_name}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

async fn delete_user_policy(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };

    let path = user_policy_path(&user_name, &policy_name);
    if let Err(e) = state.iam.delete(&path).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeleteUserPolicy", "")
}

async fn list_user_policies(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    let prefix = format!("users/{user_name}/policies/");
    let all_paths = match state.iam.list(&prefix).await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let names_xml: String = all_paths
        .iter()
        .filter(|p| p.ends_with(".json"))
        .filter_map(|p| {
            p.split('/').last().and_then(|f| f.strip_suffix(".json"))
        })
        .map(|n| format!("<member>{n}</member>"))
        .collect();

    let result = format!("<ListUserPoliciesResult><PolicyNames>{names_xml}</PolicyNames><IsTruncated>false</IsTruncated></ListUserPoliciesResult>");
    xml_response("ListUserPolicies", &result)
}

// --- Attach/detach user policy ---

async fn attach_user_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };

    match load_user(&state.iam, &user_name).await {
        Ok(None) => return not_found(&format!("User {user_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    let path = user_attached_path(&user_name, &policy_arn);
    if let Err(e) = state.iam.put(&path, Vec::new()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("AttachUserPolicy", "")
}

async fn detach_user_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };

    let path = user_attached_path(&user_name, &policy_arn);
    if let Err(e) = state.iam.delete(&path).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DetachUserPolicy", "")
}

async fn list_attached_user_policies(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let user_name = match params.get("UserName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "UserName is required"),
    };

    let prefix = format!("users/{user_name}/attached/");
    let all_paths = match state.iam.list(&prefix).await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut policies_xml = String::new();
    for path in &all_paths {
        if let Some(file_name) = path.split('/').last() {
            // decode base64url to get the policy ARN
            if let Ok(decoded) = base64::Engine::decode(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                file_name,
            ) {
                if let Ok(policy_arn) = String::from_utf8(decoded) {
                    let policy_name = policy_name_from_arn(&policy_arn).unwrap_or(&policy_arn);
                    policies_xml.push_str(&format!(
                        "<member>\
                          <PolicyArn>{policy_arn}</PolicyArn>\
                          <PolicyName>{policy_name}</PolicyName>\
                        </member>"
                    ));
                }
            }
        }
    }

    let result = format!("<ListAttachedUserPoliciesResult><AttachedPolicies>{policies_xml}</AttachedPolicies><IsTruncated>false</IsTruncated></ListAttachedUserPoliciesResult>");
    xml_response("ListAttachedUserPolicies", &result)
}

// --- Role operations ---

async fn create_role(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let assume_policy_doc = match params.get("AssumeRolePolicyDocument") {
        Some(d) => d.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "AssumeRolePolicyDocument is required",
            )
        }
    };
    let path = params.get("Path").cloned().unwrap_or_else(|| "/".to_string());
    let description = params.get("Description").cloned();
    let tags = parse_tags(params);

    // Validate JSON
    if serde_json::from_str::<serde_json::Value>(&assume_policy_doc).is_err() {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedPolicyDocument",
            "AssumeRolePolicyDocument is not valid JSON",
        );
    }

    match load_role(&state.iam, &role_name).await {
        Ok(Some(_)) => return already_exists(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(None) => {}
    }

    let role = IamRole {
        role_name: role_name.clone(),
        role_id: random_id("AROA"),
        arn: format!("arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"),
        path,
        assume_role_policy_document: assume_policy_doc,
        created_date: now_iso8601(),
        description,
        tags,
    };

    if let Err(e) = save_role(&state.iam, &role).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    let result = format!("<CreateRoleResult>{}</CreateRoleResult>", role_xml(&role));
    xml_response("CreateRole", &result)
}

async fn get_role(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };

    match load_role(&state.iam, &role_name).await {
        Ok(Some(role)) => {
            let result = format!("<GetRoleResult>{}</GetRoleResult>", role_xml(&role));
            xml_response("GetRole", &result)
        }
        Ok(None) => not_found(&format!("Role {role_name}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

async fn list_roles(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let path_prefix = params.get("PathPrefix").cloned().unwrap_or_else(|| "/".to_string());

    let all_paths = match state.iam.list("roles/").await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut roles_xml = String::new();
    for path in all_paths.iter().filter(|p| is_role_meta(p)) {
        if let Ok(Some(bytes)) = state.iam.get(path).await {
            if let Ok(role) = serde_json::from_slice::<IamRole>(&bytes) {
                if role.path.starts_with(&path_prefix) {
                    roles_xml.push_str(&role_member_xml(&role));
                }
            }
        }
    }

    let result = format!("<ListRolesResult><Roles>{roles_xml}</Roles><IsTruncated>false</IsTruncated></ListRolesResult>");
    xml_response("ListRoles", &result)
}

async fn delete_role(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };

    match load_role(&state.iam, &role_name).await {
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    if let Err(e) = state.iam.delete(&role_meta_path(&role_name)).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeleteRole", "")
}

async fn tag_role(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let new_tags = parse_tags(params);

    let mut role = match load_role(&state.iam, &role_name).await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    for new_tag in new_tags {
        if let Some(existing) = role.tags.iter_mut().find(|t| t.key == new_tag.key) {
            existing.value = new_tag.value;
        } else {
            role.tags.push(new_tag);
        }
    }

    if let Err(e) = save_role(&state.iam, &role).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("TagRole", "")
}

async fn untag_role(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let keys_to_remove = parse_tag_keys(params);

    let mut role = match load_role(&state.iam, &role_name).await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    role.tags.retain(|t| !keys_to_remove.contains(&t.key));

    if let Err(e) = save_role(&state.iam, &role).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("UntagRole", "")
}

async fn list_role_tags(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };

    let role = match load_role(&state.iam, &role_name).await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let tags_xml: String = role
        .tags
        .iter()
        .map(|t| format!("<member><Key>{}</Key><Value>{}</Value></member>", t.key, t.value))
        .collect();
    let result = format!("<ListRoleTagsResult><Tags>{tags_xml}</Tags><IsTruncated>false</IsTruncated></ListRoleTagsResult>");
    xml_response("ListRoleTags", &result)
}

// --- Inline role policy operations ---

async fn put_role_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };
    let policy_doc = match params.get("PolicyDocument") {
        Some(d) => d.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyDocument is required",
            )
        }
    };

    if serde_json::from_str::<serde_json::Value>(&policy_doc).is_err() {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedPolicyDocument",
            "PolicyDocument is not valid JSON",
        );
    }

    match load_role(&state.iam, &role_name).await {
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    let path = role_policy_path(&role_name, &policy_name);
    if let Err(e) = state.iam.put(&path, policy_doc.into_bytes()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("PutRolePolicy", "")
}

async fn get_role_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };

    let path = role_policy_path(&role_name, &policy_name);
    match state.iam.get(&path).await {
        Ok(Some(bytes)) => {
            let doc = String::from_utf8_lossy(&bytes);
            let encoded = urlencoding::encode(&doc).into_owned();
            let result = format!(
                "<GetRolePolicyResult>\
                  <RoleName>{role_name}</RoleName>\
                  <PolicyName>{policy_name}</PolicyName>\
                  <PolicyDocument>{encoded}</PolicyDocument>\
                </GetRolePolicyResult>"
            );
            xml_response("GetRolePolicy", &result)
        }
        Ok(None) => not_found(&format!("RolePolicy {policy_name} on Role {role_name}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

async fn delete_role_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };

    let path = role_policy_path(&role_name, &policy_name);
    if let Err(e) = state.iam.delete(&path).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeleteRolePolicy", "")
}

async fn list_role_policies(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };

    let prefix = format!("roles/{role_name}/policies/");
    let all_paths = match state.iam.list(&prefix).await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let names_xml: String = all_paths
        .iter()
        .filter(|p| p.ends_with(".json"))
        .filter_map(|p| {
            p.split('/').last().and_then(|f| f.strip_suffix(".json"))
        })
        .map(|n| format!("<member>{n}</member>"))
        .collect();

    let result = format!("<ListRolePoliciesResult><PolicyNames>{names_xml}</PolicyNames><IsTruncated>false</IsTruncated></ListRolePoliciesResult>");
    xml_response("ListRolePolicies", &result)
}

// --- Attach/detach role policy ---

async fn attach_role_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };

    match load_role(&state.iam, &role_name).await {
        Ok(None) => return not_found(&format!("Role {role_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    let path = role_attached_path(&role_name, &policy_arn);
    if let Err(e) = state.iam.put(&path, Vec::new()).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("AttachRolePolicy", "")
}

async fn detach_role_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };

    let path = role_attached_path(&role_name, &policy_arn);
    if let Err(e) = state.iam.delete(&path).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DetachRolePolicy", "")
}

async fn list_attached_role_policies(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let role_name = match params.get("RoleName") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "RoleName is required"),
    };

    let prefix = format!("roles/{role_name}/attached/");
    let all_paths = match state.iam.list(&prefix).await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut policies_xml = String::new();
    for path in &all_paths {
        if let Some(file_name) = path.split('/').last() {
            if let Ok(decoded) = base64::Engine::decode(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                file_name,
            ) {
                if let Ok(policy_arn) = String::from_utf8(decoded) {
                    let policy_name = policy_name_from_arn(&policy_arn).unwrap_or(&policy_arn);
                    policies_xml.push_str(&format!(
                        "<member>\
                          <PolicyArn>{policy_arn}</PolicyArn>\
                          <PolicyName>{policy_name}</PolicyName>\
                        </member>"
                    ));
                }
            }
        }
    }

    let result = format!("<ListAttachedRolePoliciesResult><AttachedPolicies>{policies_xml}</AttachedPolicies><IsTruncated>false</IsTruncated></ListAttachedRolePoliciesResult>");
    xml_response("ListAttachedRolePolicies", &result)
}

// --- Managed policy operations ---

async fn create_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let policy_name = match params.get("PolicyName") {
        Some(n) => n.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyName is required",
            )
        }
    };
    let policy_doc = match params.get("PolicyDocument") {
        Some(d) => d.clone(),
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "PolicyDocument is required",
            )
        }
    };
    let path = params.get("Path").cloned().unwrap_or_else(|| "/".to_string());

    if serde_json::from_str::<serde_json::Value>(&policy_doc).is_err() {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedPolicyDocument",
            "PolicyDocument is not valid JSON",
        );
    }

    match load_policy(&state.iam, &policy_name).await {
        Ok(Some(_)) => return already_exists(&format!("Policy {policy_name}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(None) => {}
    }

    let now = now_iso8601();
    let policy = ManagedPolicy {
        policy_name: policy_name.clone(),
        policy_id: random_id("ANPA"),
        arn: format!("arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}"),
        path,
        document: policy_doc,
        created_date: now.clone(),
        updated_date: now,
    };

    if let Err(e) = save_policy(&state.iam, &policy).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    let result = format!(
        "<CreatePolicyResult>{}</CreatePolicyResult>",
        policy_xml(&policy)
    );
    xml_response("CreatePolicy", &result)
}

async fn get_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };
    let policy_name = match policy_name_from_arn(&policy_arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "Invalid PolicyArn"),
    };

    match load_policy(&state.iam, &policy_name).await {
        Ok(Some(policy)) => {
            let result = format!(
                "<GetPolicyResult>{}</GetPolicyResult>",
                policy_xml(&policy)
            );
            xml_response("GetPolicy", &result)
        }
        Ok(None) => not_found(&format!("Policy {policy_arn}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

async fn delete_policy(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };
    let policy_name = match policy_name_from_arn(&policy_arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "Invalid PolicyArn"),
    };

    match load_policy(&state.iam, &policy_name).await {
        Ok(None) => return not_found(&format!("Policy {policy_arn}")),
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
        Ok(Some(_)) => {}
    }

    if let Err(e) = state.iam.delete(&policy_meta_path(&policy_name)).await {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        );
    }

    xml_response("DeletePolicy", "")
}

async fn list_policies(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let path_prefix = params.get("PathPrefix").cloned().unwrap_or_else(|| "/".to_string());

    let all_paths = match state.iam.list("policies/").await {
        Ok(p) => p,
        Err(e) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalFailure",
                &e.to_string(),
            )
        }
    };

    let mut policies_xml = String::new();
    for path in all_paths.iter().filter(|p| is_policy_meta(p)) {
        if let Ok(Some(bytes)) = state.iam.get(path).await {
            if let Ok(policy) = serde_json::from_slice::<ManagedPolicy>(&bytes) {
                if policy.path.starts_with(&path_prefix) {
                    policies_xml.push_str(&policy_member_xml(&policy));
                }
            }
        }
    }

    let result = format!("<ListPoliciesResult><Policies>{policies_xml}</Policies><IsTruncated>false</IsTruncated></ListPoliciesResult>");
    xml_response("ListPolicies", &result)
}

async fn get_policy_version(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let policy_arn = match params.get("PolicyArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "PolicyArn is required"),
    };
    let policy_name = match policy_name_from_arn(&policy_arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidInput", "Invalid PolicyArn"),
    };

    match load_policy(&state.iam, &policy_name).await {
        Ok(Some(policy)) => {
            let encoded = urlencoding::encode(&policy.document).into_owned();
            let result = format!(
                "<GetPolicyVersionResult><PolicyVersion>\
                  <Document>{encoded}</Document>\
                  <VersionId>v1</VersionId>\
                  <IsDefaultVersion>true</IsDefaultVersion>\
                  <CreateDate>{}</CreateDate>\
                </PolicyVersion></GetPolicyVersionResult>",
                policy.created_date
            );
            xml_response("GetPolicyVersion", &result)
        }
        Ok(None) => not_found(&format!("Policy {policy_arn}")),
        Err(e) => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            &e.to_string(),
        ),
    }
}

// --- STS ---

async fn get_caller_identity(
    params: &HashMap<String, String>,
    creds: Option<&Credentials>,
) -> XmlResponse {
    let _ = params;
    let access_key = creds
        .map(|c| c.access_key.as_str())
        .unwrap_or("test");
    let user_id = if access_key == "test" {
        "AIDATESTUSER00001".to_string()
    } else {
        access_key.to_string()
    };
    let arn = format!("arn:aws:iam::{ACCOUNT_ID}:user/{access_key}");

    let result = format!(
        "<GetCallerIdentityResult>\
          <Account>{ACCOUNT_ID}</Account>\
          <UserId>{user_id}</UserId>\
          <Arn>{arn}</Arn>\
        </GetCallerIdentityResult>"
    );
    xml_response("GetCallerIdentity", &result)
}

// ── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/iam/", post(dispatch_handler))
}

async fn dispatch_handler(
    state: State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(state, request).await
}

pub(crate) async fn dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    // Extract credentials from extensions
    let creds = request.extensions().get::<Credentials>().cloned();

    // Try to get Action from query string first
    let query_action = request
        .uri()
        .query()
        .and_then(|q| {
            serde_urlencoded::from_str::<HashMap<String, String>>(q)
                .ok()
                .and_then(|m| m.get("Action").cloned())
        });

    // Read body
    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                &format!("failed to read body: {e}"),
            )
            .into_response()
        }
    };

    let mut params: HashMap<String, String> = if body_bytes.is_empty() {
        HashMap::new()
    } else {
        match serde_urlencoded::from_bytes(&body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return xml_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    &format!("failed to parse body: {e}"),
                )
                .into_response()
            }
        }
    };

    // Merge query action if body doesn't have it
    let action = params
        .get("Action")
        .cloned()
        .or(query_action)
        .unwrap_or_default();

    tracing::debug!("IAM/STS action={action}");

    let response = match action.as_str() {
        // User operations
        "CreateUser" => create_user(&state, &params).await,
        "GetUser" => get_user(&state, &params, creds.as_ref()).await,
        "ListUsers" => list_users(&state, &params).await,
        "UpdateUser" => update_user(&state, &params).await,
        "DeleteUser" => delete_user(&state, &params).await,
        "TagUser" => tag_user(&state, &params).await,
        "UntagUser" => untag_user(&state, &params).await,
        "ListUserTags" => list_user_tags(&state, &params).await,
        // Access key operations
        "CreateAccessKey" => create_access_key(&state, &params).await,
        "ListAccessKeys" => list_access_keys(&state, &params).await,
        "UpdateAccessKey" => update_access_key(&state, &params).await,
        "DeleteAccessKey" => delete_access_key(&state, &params).await,
        // Inline user policy operations
        "PutUserPolicy" => put_user_policy(&state, &params).await,
        "GetUserPolicy" => get_user_policy(&state, &params).await,
        "DeleteUserPolicy" => delete_user_policy(&state, &params).await,
        "ListUserPolicies" => list_user_policies(&state, &params).await,
        // Attach user policy operations
        "AttachUserPolicy" => attach_user_policy(&state, &params).await,
        "DetachUserPolicy" => detach_user_policy(&state, &params).await,
        "ListAttachedUserPolicies" => list_attached_user_policies(&state, &params).await,
        // Role operations
        "CreateRole" => create_role(&state, &params).await,
        "GetRole" => get_role(&state, &params).await,
        "ListRoles" => list_roles(&state, &params).await,
        "DeleteRole" => delete_role(&state, &params).await,
        "TagRole" => tag_role(&state, &params).await,
        "UntagRole" => untag_role(&state, &params).await,
        "ListRoleTags" => list_role_tags(&state, &params).await,
        // Inline role policy operations
        "PutRolePolicy" => put_role_policy(&state, &params).await,
        "GetRolePolicy" => get_role_policy(&state, &params).await,
        "DeleteRolePolicy" => delete_role_policy(&state, &params).await,
        "ListRolePolicies" => list_role_policies(&state, &params).await,
        // Attach role policy operations
        "AttachRolePolicy" => attach_role_policy(&state, &params).await,
        "DetachRolePolicy" => detach_role_policy(&state, &params).await,
        "ListAttachedRolePolicies" => list_attached_role_policies(&state, &params).await,
        // Managed policy operations
        "CreatePolicy" => create_policy(&state, &params).await,
        "GetPolicy" => get_policy(&state, &params).await,
        "DeletePolicy" => delete_policy(&state, &params).await,
        "ListPolicies" => list_policies(&state, &params).await,
        "GetPolicyVersion" => get_policy_version(&state, &params).await,
        // STS
        "GetCallerIdentity" => get_caller_identity(&params, creds.as_ref()).await,
        other => {
            tracing::warn!("unknown IAM/STS action: {other}");
            xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                &format!("unknown action: {other}"),
            )
        }
    };

    response.into_response()
}
