mod bucket;
mod multipart;
mod object;
pub mod presign;
pub mod store;
pub mod types;

use std::sync::Arc;
use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
};
use crate::services::AppState;

/// Middleware: normalise the request URI for S3 path-style routing.
///
/// 1. If Host is `{bucket}.localhost[:{port}]`, prepend `/{bucket}` to the URI path.
/// 2. Strip a trailing slash from the path so that `PUT /bucket/` routes the same
///    as `PUT /bucket` (the AWS SDK appends a trailing slash for create-bucket).
pub async fn host_routing_middleware(mut request: Request, next: Next) -> Response {
    if let Some(host) = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
    {
        let host_part = host.split(':').next().unwrap_or(host);
        let bucket = if let Some(b) = host_part.strip_suffix(".s3.localhost") {
            Some(b.to_string())
        } else {
            host_part.strip_suffix(".localhost").map(|b| b.to_string())
        };

        if let Some(bucket) = bucket {
            let original = request.uri().clone();
            let new_path = format!("/{}{}", bucket, original.path());
            let pq = match original.query() {
                Some(q) => format!("{}?{}", new_path, q),
                None => new_path,
            };
            if let Ok(new_uri) = pq.parse::<axum::http::Uri>() {
                *request.uri_mut() = new_uri;
            }
        }
    }

    next.run(request).await
}

async fn fallback(request: Request) -> axum::response::Response {
    tracing::warn!(
        method = %request.method(),
        uri = %request.uri(),
        "s3 unmatched route"
    );
    axum::http::StatusCode::NOT_FOUND.into_response()
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(bucket::list_buckets))
        // With and without trailing slash to handle both curl and AWS SDK styles.
        .route("/{bucket}", get(bucket::list_objects))
        .route("/{bucket}/", get(bucket::list_objects))
        .route("/{bucket}", put(bucket::create_bucket))
        .route("/{bucket}/", put(bucket::create_bucket))
        .route("/{bucket}", delete(bucket::delete_bucket))
        .route("/{bucket}/", delete(bucket::delete_bucket))
        .route("/{bucket}", head(bucket::head_bucket))
        .route("/{bucket}/", head(bucket::head_bucket))
        .route("/{bucket}/{*key}", get(object::get_object))
        .route("/{bucket}/{*key}", put(object::put_object))
        .route("/{bucket}/{*key}", delete(object::delete_object))
        .route("/{bucket}/{*key}", head(object::head_object))
        .route("/{bucket}/{*key}", post(multipart::post_object))
        .fallback(fallback)
        .layer(middleware::from_fn(host_routing_middleware))
}
