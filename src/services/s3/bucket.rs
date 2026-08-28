use std::sync::Arc;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::services::AppState;
use super::{
    store::S3Store,
    types::{
        BucketEntry, BucketList, CommonPrefix, ListBucketsResult, ListObjectsResult,
        ObjectEntry, Owner, S3_XMLNS, DEFAULT_REGION, to_xml,
    },
};

fn xml_ok(xml: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

pub async fn list_buckets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.list_buckets().await {
        Ok(buckets) => {
            let result = ListBucketsResult {
                xmlns: S3_XMLNS,
                owner: Owner {
                    id: "cloudish".into(),
                    display_name: "cloudish".into(),
                },
                buckets: BucketList {
                    buckets: buckets
                        .into_iter()
                        .map(|b| BucketEntry {
                            name: b.name,
                            creation_date: b.creation_date,
                        })
                        .collect(),
                },
            };
            match to_xml(&result) {
                Ok(xml) => xml_ok(xml).into_response(),
                Err(e) => {
                    tracing::error!("xml error: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("list_buckets: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    request: axum::extract::Request,
) -> impl IntoResponse {
    // The AWS SDK sends a CreateBucketConfiguration XML body. We must drain it
    // before responding so the HTTP/1.1 keep-alive connection remains usable
    // for subsequent requests on the same socket.
    let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.create_bucket(&bucket, DEFAULT_REGION).await {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .header("Location", format!("/{}", bucket))
            .body(Body::empty())
            .unwrap()
            .into_response(),
        Err(e) => {
            tracing::error!("create_bucket: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.delete_bucket(&bucket).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!("delete_bucket: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn head_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    match store.bucket_exists(&bucket).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("head_bucket: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<usize>,
    /// `list-type=2` selects the ListObjectsV2 response format, which includes
    /// a `<KeyCount>` element.
    #[serde(rename = "list-type")]
    pub list_type: Option<u8>,
}

pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    let store = S3Store::new(state.s3.base_dir.clone());
    if !matches!(store.bucket_exists(&bucket).await, Ok(true)) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let prefix = q.prefix.unwrap_or_default();
    let delimiter = q.delimiter.unwrap_or_default();
    let max_keys = q.max_keys.unwrap_or(1000).min(1000);
    let v2 = q.list_type == Some(2);

    match store.list_objects(&bucket, &prefix, &delimiter, max_keys).await {
        Ok((objects, common_prefixes, truncated)) => {
            let object_count = objects.len() + common_prefixes.len();
            let result = ListObjectsResult {
                xmlns: S3_XMLNS,
                name: bucket,
                prefix: prefix.clone(),
                max_keys,
                is_truncated: truncated,
                key_count: if v2 { Some(object_count) } else { None },
                contents: objects
                    .into_iter()
                    .map(|(key, meta)| ObjectEntry {
                        key,
                        last_modified: meta.last_modified,
                        etag: format!("\"{}\"", meta.etag),
                        size: meta.content_length,
                        storage_class: "STANDARD".into(),
                    })
                    .collect(),
                common_prefixes: common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: p })
                    .collect(),
            };
            match to_xml(&result) {
                Ok(xml) => xml_ok(xml).into_response(),
                Err(e) => {
                    tracing::error!("xml error: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("list_objects: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
