use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::services::AppState;
use super::{
    presign::{is_presigned, validate_presigned},
    store::{ObjectMeta, S3Store},
};

/// Convert an ISO 8601 timestamp (e.g. "2026-08-28T17:31:53.000Z") to an
/// RFC 7231 HTTP-date (e.g. "Fri, 28 Aug 2026 17:31:53 GMT") for use in
/// `Last-Modified` headers.  Falls back to the original string on parse error.
fn to_http_date(iso: &str) -> String {
    use chrono::{DateTime, Utc};
    if let Ok(dt) = iso.parse::<DateTime<Utc>>() {
        dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
    } else {
        iso.to_string()
    }
}

fn object_response(data: Vec<u8>, meta: &ObjectMeta) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, &meta.content_type)
        .header(header::CONTENT_LENGTH, meta.content_length.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::LAST_MODIFIED, to_http_date(&meta.last_modified));
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{}", k), v);
    }
    builder.body(Body::from(data)).unwrap()
}

#[derive(Deserialize)]
pub struct GetQuery {
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
}

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<GetQuery>,
    request: Request,
) -> impl IntoResponse {
    if is_presigned(request.uri()) {
        if let Err(e) = validate_presigned(request.uri()) {
            tracing::warn!("presigned validation failed: {e}");
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let store = S3Store::new(state.s3.base_dir.clone());
    if let Some(version_id) = q.version_id {
        match store.get_object_version(&bucket, &key, &version_id).await {
            Ok(Some((data, meta))) => object_response(data, &meta).into_response(),
            Ok(None) => StatusCode::NOT_FOUND.into_response(),
            Err(e) => {
                tracing::error!("get_object_version: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    } else {
        let data = store.get_object_data(&bucket, &key).await;
        let meta = store.get_object_meta(&bucket, &key).await;
        match (data, meta) {
            (Ok(Some(d)), Ok(Some(m))) => object_response(d, &m).into_response(),
            (Ok(None), _) | (_, Ok(None)) => StatusCode::NOT_FOUND.into_response(),
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!("get_object: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

pub async fn head_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.get_object_meta(&bucket, &key).await {
        Ok(Some(meta)) => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, &meta.content_type)
                .header(header::CONTENT_LENGTH, meta.content_length.to_string())
                .header(header::ETAG, format!("\"{}\"", meta.etag))
                .header(header::LAST_MODIFIED, to_http_date(&meta.last_modified));
            for (k, v) in &meta.user_meta {
                builder = builder.header(format!("x-amz-meta-{}", k), v);
            }
            builder.body(Body::empty()).unwrap().into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("head_object: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct PutQuery {
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
}

pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<PutQuery>,
    request: Request,
) -> impl IntoResponse {
    // If `uploadId` and `partNumber` are present this is a multipart part upload.
    if let (Some(upload_id), Some(part_number)) = (q.upload_id, q.part_number) {
        return upload_part(state, upload_id, part_number, request).await.into_response();
    }

    if is_presigned(request.uri()) {
        if let Err(e) = validate_presigned(request.uri()) {
            tracing::warn!("presigned validation failed: {e}");
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let store = S3Store::new(state.s3.base_dir.clone());
    if !matches!(store.bucket_exists(&bucket).await, Ok(true)) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let headers = request.headers().clone();
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let user_meta: HashMap<String, String> = headers
        .iter()
        .filter_map(|(n, v)| {
            Some((
                n.as_str().strip_prefix("x-amz-meta-")?.to_string(),
                v.to_str().ok()?.to_string(),
            ))
        })
        .collect();

    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!("body read: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match store.put_object(&bucket, &key, body, &content_type, user_meta).await {
        Ok(meta) => Response::builder()
            .status(StatusCode::OK)
            .header(header::ETAG, format!("\"{}\"", meta.etag))
            .body(Body::empty())
            .unwrap()
            .into_response(),
        Err(e) => {
            tracing::error!("put_object: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn upload_part(
    state: Arc<AppState>,
    upload_id: String,
    part_number: u32,
    request: Request,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!("upload_part body: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    match store.upload_part(&upload_id, part_number, body).await {
        Ok(etag) => Response::builder()
            .status(StatusCode::OK)
            .header(header::ETAG, format!("\"{}\"", etag))
            .body(Body::empty())
            .unwrap()
            .into_response(),
        Err(e) => {
            tracing::error!("upload_part: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.delete_object(&bucket, &key).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!("delete_object: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
