//! SNS service stub.
//!
//! Wire format: POST with URL-encoded body, `Action` parameter selects the operation.
//! Routing: dispatched from top_level_dispatch when SigV4 credential scope service = "sns".

use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    response::IntoResponse,
};

use crate::{error::Error, services::AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
}

pub async fn dispatch(
    State(_state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let body = to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_bytes(&body).unwrap_or_default();
    let action = params.get("Action").cloned().unwrap_or_default();
    tracing::debug!("SNS action={action}");

    match action.as_str() {
        "CreateTopic" => Err::<(), _>(Error::NotImplemented),
        "DeleteTopic" => Err::<(), _>(Error::NotImplemented),
        "ListTopics" => Err::<(), _>(Error::NotImplemented),
        "GetTopicAttributes" => Err::<(), _>(Error::NotImplemented),
        "SetTopicAttributes" => Err::<(), _>(Error::NotImplemented),
        "Subscribe" => Err::<(), _>(Error::NotImplemented),
        "Unsubscribe" => Err::<(), _>(Error::NotImplemented),
        "ListSubscriptions" => Err::<(), _>(Error::NotImplemented),
        "ListSubscriptionsByTopic" => Err::<(), _>(Error::NotImplemented),
        "GetSubscriptionAttributes" => Err::<(), _>(Error::NotImplemented),
        "SetSubscriptionAttributes" => Err::<(), _>(Error::NotImplemented),
        "ConfirmSubscription" => Err::<(), _>(Error::NotImplemented),
        "Publish" => Err::<(), _>(Error::NotImplemented),
        "PublishBatch" => Err::<(), _>(Error::NotImplemented),
        "CreatePlatformApplication" => Err::<(), _>(Error::NotImplemented),
        "DeletePlatformApplication" => Err::<(), _>(Error::NotImplemented),
        "ListPlatformApplications" => Err::<(), _>(Error::NotImplemented),
        "TagResource" => Err::<(), _>(Error::NotImplemented),
        "UntagResource" => Err::<(), _>(Error::NotImplemented),
        "ListTagsForResource" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown SNS action: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown action: {other}")))
        }
    }
}
