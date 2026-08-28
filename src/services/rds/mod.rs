//! RDS management-plane emulator.
//!
//! Note: The RDS data plane (PostgreSQL wire protocol) is served by a real
//! Postgres instance configured via the `RDS_DSN` environment variable.
//! This module only emulates the RDS management HTTP API.
//!
//! Routing: POST `/rds/`, dispatched by `Action` query parameter.

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
    Router::new().route("/rds/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    Query(q): Query<ActionQuery>,
) -> impl IntoResponse {
    let action = q.action.unwrap_or_default();
    tracing::debug!("RDS action={action}");

    match action.as_str() {
        "CreateDBInstance" => Err::<(), _>(Error::NotImplemented),
        "DeleteDBInstance" => Err::<(), _>(Error::NotImplemented),
        "DescribeDBInstances" => Err::<(), _>(Error::NotImplemented),
        "ModifyDBInstance" => Err::<(), _>(Error::NotImplemented),
        "CreateDBSnapshot" => Err::<(), _>(Error::NotImplemented),
        "DescribeDBSnapshots" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown RDS action: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown action: {other}")))
        }
    }
}
