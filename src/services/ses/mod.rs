//! SES service emulator.
//!
//! Routing: POST `/ses/`, dispatched by `Action` query/form parameter.

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
    Router::new().route("/ses/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    Query(q): Query<ActionQuery>,
) -> impl IntoResponse {
    let action = q.action.unwrap_or_default();
    tracing::debug!("SES action={action}");

    match action.as_str() {
        "SendEmail" => Err::<(), _>(Error::NotImplemented),
        "SendRawEmail" => Err::<(), _>(Error::NotImplemented),
        "VerifyEmailIdentity" => Err::<(), _>(Error::NotImplemented),
        "ListIdentities" => Err::<(), _>(Error::NotImplemented),
        "DeleteIdentity" => Err::<(), _>(Error::NotImplemented),
        "GetSendQuota" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown SES action: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown action: {other}")))
        }
    }
}
