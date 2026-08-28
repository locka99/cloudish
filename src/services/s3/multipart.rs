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
    store::S3Store,
    types::{
        CompleteMultipartUploadRequest, CompleteMultipartUploadResult,
        InitiateMultipartUploadResult, S3_XMLNS, from_xml, to_xml,
    },
};

fn xml_ok(xml: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

#[derive(Deserialize)]
pub struct PostQuery {
    pub uploads: Option<String>,
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
}

pub async fn post_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<PostQuery>,
    request: Request,
) -> impl IntoResponse {
    match (q.uploads.is_some(), q.upload_id, q.part_number) {
        (true, _, _) => {
            create_multipart_upload(state, bucket, key, request)
                .await
                .into_response()
        }
        (false, Some(id), Some(part)) => {
            upload_part(state, bucket, key, id, part, request)
                .await
                .into_response()
        }
        (false, Some(id), None) => {
            complete_multipart_upload(state, bucket, key, id, request)
                .await
                .into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn create_multipart_upload(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    request: Request,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
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

    match store
        .create_multipart(&bucket, &key, &content_type, user_meta)
        .await
    {
        Ok(upload_id) => match to_xml(&InitiateMultipartUploadResult {
            xmlns: S3_XMLNS,
            bucket,
            key,
            upload_id,
        }) {
            Ok(xml) => xml_ok(xml).into_response(),
            Err(e) => {
                tracing::error!("xml: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        Err(e) => {
            tracing::error!("create_multipart: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn upload_part(
    state: Arc<AppState>,
    _bucket: String,
    _key: String,
    upload_id: String,
    part_number: u32,
    request: Request,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!("body: {e}");
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

async fn complete_multipart_upload(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    upload_id: String,
    request: Request,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            tracing::error!("body: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let req: CompleteMultipartUploadRequest = match from_xml(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("xml parse: {e}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let parts: Vec<(u32, String)> = req.parts.into_iter().map(|p| (p.part_number, p.etag)).collect();

    match store.complete_multipart(&upload_id, &parts).await {
        Ok(meta) => match to_xml(&CompleteMultipartUploadResult {
            xmlns: S3_XMLNS,
            location: format!("http://localhost:4566/{}/{}", bucket, key),
            bucket,
            key,
            etag: format!("\"{}\"", meta.etag),
        }) {
            Ok(xml) => xml_ok(xml).into_response(),
            Err(e) => {
                tracing::error!("xml: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        Err(e) => {
            tracing::error!("complete_multipart: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
