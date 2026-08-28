//! IAM service emulator (basics).
//!
//! Routing: POST `/iam/`, dispatched by `Action` query parameter.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Query, State},
    response::IntoResponse,
    routing::post,
};
use serde::Deserialize;

use crate::{error::Error, services::AppState};

#[derive(Deserialize)]
struct ActionQuery {
    #[serde(rename = "Action")]
    action: Option<String>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/iam/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    Query(q): Query<ActionQuery>,
) -> impl IntoResponse {
    let action = q.action.unwrap_or_default();
    tracing::debug!("IAM action={action}");

    match action.as_str() {
        "CreateUser" => Err::<(), _>(Error::NotImplemented),
        "DeleteUser" => Err::<(), _>(Error::NotImplemented),
        "GetUser" => Err::<(), _>(Error::NotImplemented),
        "ListUsers" => Err::<(), _>(Error::NotImplemented),
        "CreateAccessKey" => Err::<(), _>(Error::NotImplemented),
        "DeleteAccessKey" => Err::<(), _>(Error::NotImplemented),
        "ListAccessKeys" => Err::<(), _>(Error::NotImplemented),
        "CreateRole" => Err::<(), _>(Error::NotImplemented),
        "DeleteRole" => Err::<(), _>(Error::NotImplemented),
        "GetRole" => Err::<(), _>(Error::NotImplemented),
        "ListRoles" => Err::<(), _>(Error::NotImplemented),
        "PutUserPolicy" => Err::<(), _>(Error::NotImplemented),
        "GetUserPolicy" => Err::<(), _>(Error::NotImplemented),
        "ListUserPolicies" => Err::<(), _>(Error::NotImplemented),
        "PutRolePolicy" => Err::<(), _>(Error::NotImplemented),
        "AttachUserPolicy" => Err::<(), _>(Error::NotImplemented),
        "AttachRolePolicy" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown IAM action: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown action: {other}")))
        }
    }
}
