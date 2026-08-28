//! SQS service emulator.
//!
//! Routing: POST `/sqs/`, dispatched by `Action` query parameter.

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
    Router::new().route("/sqs/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    Query(q): Query<ActionQuery>,
) -> impl IntoResponse {
    let action = q.action.unwrap_or_default();
    tracing::debug!("SQS action={action}");

    match action.as_str() {
        "CreateQueue" => Err::<(), _>(Error::NotImplemented),
        "DeleteQueue" => Err::<(), _>(Error::NotImplemented),
        "GetQueueUrl" => Err::<(), _>(Error::NotImplemented),
        "ListQueues" => Err::<(), _>(Error::NotImplemented),
        "SendMessage" => Err::<(), _>(Error::NotImplemented),
        "ReceiveMessage" => Err::<(), _>(Error::NotImplemented),
        "DeleteMessage" => Err::<(), _>(Error::NotImplemented),
        "ChangeMessageVisibility" => Err::<(), _>(Error::NotImplemented),
        "GetQueueAttributes" => Err::<(), _>(Error::NotImplemented),
        "SetQueueAttributes" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown SQS action: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown action: {other}")))
        }
    }
}
