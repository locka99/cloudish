//! DynamoDB service emulator.
//!
//! Routing: all requests POST to `/dynamodb/`, dispatched by `X-Amz-Target` header.
//! Target format: `DynamoDB_20120810.<Operation>`

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};

use crate::{error::Error, services::AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/dynamodb/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split('.').last())
        .unwrap_or("")
        .to_string();

    tracing::debug!("DynamoDB operation={target}");

    match target.as_str() {
        "CreateTable" => Err::<(), _>(Error::NotImplemented),
        "DeleteTable" => Err::<(), _>(Error::NotImplemented),
        "DescribeTable" => Err::<(), _>(Error::NotImplemented),
        "ListTables" => Err::<(), _>(Error::NotImplemented),
        "PutItem" => Err::<(), _>(Error::NotImplemented),
        "GetItem" => Err::<(), _>(Error::NotImplemented),
        "DeleteItem" => Err::<(), _>(Error::NotImplemented),
        "UpdateItem" => Err::<(), _>(Error::NotImplemented),
        "Query" => Err::<(), _>(Error::NotImplemented),
        "Scan" => Err::<(), _>(Error::NotImplemented),
        "BatchGetItem" => Err::<(), _>(Error::NotImplemented),
        "BatchWriteItem" => Err::<(), _>(Error::NotImplemented),
        "TransactGetItems" => Err::<(), _>(Error::NotImplemented),
        "TransactWriteItems" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown DynamoDB operation: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown operation: {other}")))
        }
    }
}
