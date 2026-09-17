use std::{sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{Method, StatusCode},
    routing::{delete, get, post},
    Json, Router,
};
use aws_sdk_iam::Client as IamClient;
use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    presigning::PresigningConfig,
    Client as S3Client,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

// ── Constants ─────────────────────────────────────────────────────────────────

const CLOUDISH_ENDPOINT: &str = "http://localhost:4566";
const BUCKET: &str = "photo-gallery";
const ROLE_NAME: &str = "photo-gallery-role";
const POLICY_NAME: &str = "photo-gallery-s3-policy";
const PRESIGN_EXPIRY: Duration = Duration::from_secs(3600); // 1 hour

// ── Shared state ──────────────────────────────────────────────────────────────

struct AppState {
    s3: S3Client,
    bucket: String,
}

// ── API types ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Photo {
    key: String,
    /// Presigned GET URL — embed directly in an <img src> tag.
    url: String,
    size: i64,
    last_modified: String,
}

#[derive(Deserialize)]
struct UploadUrlRequest {
    filename: String,
    content_type: String,
}

#[derive(Serialize)]
struct UploadUrlResponse {
    /// Presigned PUT URL — client PUTs the file body directly to S3.
    url: String,
    key: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let creds = Credentials::new("test", "test", None, None, "cloudish");

    // Build S3 client (path-style because cloudish uses path-style routing)
    let s3_conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(CLOUDISH_ENDPOINT)
        .credentials_provider(creds.clone())
        .region(Region::new("eu-west-1"))
        .force_path_style(true)
        .build();
    let s3 = S3Client::from_conf(s3_conf);

    // Build IAM client
    let iam_conf = aws_sdk_iam::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(CLOUDISH_ENDPOINT)
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    let iam = IamClient::from_conf(iam_conf);

    // Provision IAM role + S3 bucket on startup
    setup_iam(&iam).await;
    setup_bucket(&s3).await;

    let state = Arc::new(AppState {
        s3,
        bucket: BUCKET.to_string(),
    });

    // Allow the React dev-server origin; for production narrow this down.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/photos", get(list_photos))
        .route(
            "/api/photos",
            post(upload_photo).layer(DefaultBodyLimit::max(100 * 1024 * 1024)), // 100 MB
        )
        .route("/api/photos/presign-upload", post(presign_upload))
        .route("/api/photos/{key}", delete(delete_photo))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    tracing::info!("Photo-gallery backend listening on http://localhost:3001");
    axum::serve(listener, app).await.unwrap();
}

// ── IAM setup ─────────────────────────────────────────────────────────────────

/// Creates an IAM policy that grants S3 access to the photo bucket, then
/// creates a service role and attaches the policy to it.  Errors are ignored
/// because the resources may already exist across restarts.
async fn setup_iam(iam: &IamClient) {
    let policy_doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{BUCKET}")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": [format!("arn:aws:s3:::{BUCKET}/*")]
            }
        ]
    })
    .to_string();

    let _ = iam
        .create_policy()
        .policy_name(POLICY_NAME)
        .policy_document(&policy_doc)
        .send()
        .await;

    let assume_role_doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string();

    let _ = iam
        .create_role()
        .role_name(ROLE_NAME)
        .assume_role_policy_document(&assume_role_doc)
        .send()
        .await;

    let _ = iam
        .attach_role_policy()
        .role_name(ROLE_NAME)
        .policy_arn(format!(
            "arn:aws:iam::000000000000:policy/{POLICY_NAME}"
        ))
        .send()
        .await;

    tracing::info!("IAM: role '{ROLE_NAME}' ready with S3 policy '{POLICY_NAME}'");
}

// ── S3 setup ──────────────────────────────────────────────────────────────────

async fn setup_bucket(s3: &S3Client) {
    let result = s3
        .create_bucket()
        .bucket(BUCKET)
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .location_constraint(
                    aws_sdk_s3::types::BucketLocationConstraint::EuWest1,
                )
                .build(),
        )
        .send()
        .await;

    match result {
        Ok(_) => tracing::info!("S3: bucket '{BUCKET}' created"),
        Err(e) if e.to_string().contains("BucketAlreadyOwned") => {
            tracing::info!("S3: bucket '{BUCKET}' already exists")
        }
        Err(e) => tracing::warn!("S3: create_bucket warning: {e}"),
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /api/photos` — list all photos with presigned GET URLs for display.
async fn list_photos(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Photo>>, StatusCode> {
    let resp = state
        .s3
        .list_objects_v2()
        .bucket(&state.bucket)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("list_objects_v2 failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let presign_cfg =
        PresigningConfig::expires_in(PRESIGN_EXPIRY).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut photos = Vec::new();
    for obj in resp.contents() {
        let key = obj.key().unwrap_or_default();

        let presigned = state
            .s3
            .get_object()
            .bucket(&state.bucket)
            .key(key)
            .presigned(presign_cfg.clone())
            .await
            .map_err(|e| {
                tracing::error!("presign failed for '{key}': {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        photos.push(Photo {
            key: key.to_string(),
            url: presigned.uri().to_string(),
            size: obj.size().unwrap_or(0),
            last_modified: obj
                .last_modified()
                .map(|dt| dt.to_string())
                .unwrap_or_default(),
        });
    }

    Ok(Json(photos))
}

/// `POST /api/photos` — accepts a multipart upload, stores the file in S3.
///
/// This is the simpler upload path: the browser POSTs the file to this server,
/// which then writes it to S3 using the AWS SDK.  Useful when cloudish is
/// running without CORS headers.
async fn upload_photo(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("multipart read failed: {e}");
        StatusCode::BAD_REQUEST
    })? {
        let filename = field
            .file_name()
            .unwrap_or("photo.jpg")
            .to_string();
        let content_type = field
            .content_type()
            .unwrap_or("image/jpeg")
            .to_string();
        let data = field.bytes().await.map_err(|e| {
            tracing::error!("reading bytes for '{filename}' failed: {e}");
            StatusCode::BAD_REQUEST
        })?;

        // Use {uuid}.{ext} so the key never contains a slash and can be
        // used directly as a URL path segment.
        let ext = filename.rsplit('.').next().unwrap_or("jpg").to_lowercase();
        let key = format!("{}.{}", uuid::Uuid::new_v4(), ext);

        tracing::info!("uploading '{filename}' ({} bytes, {content_type}) as '{key}'", data.len());

        state
            .s3
            .put_object()
            .bucket(&state.bucket)
            .key(&key)
            .content_type(&content_type)
            .body(data.into())
            .send()
            .await
            .map_err(|e| {
                tracing::error!("upload failed for '{filename}' -> '{key}': {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        tracing::info!("upload succeeded: '{filename}' -> '{key}'");
        return Ok(Json(serde_json::json!({ "key": key })));
    }

    Err(StatusCode::BAD_REQUEST)
}

/// `POST /api/photos/presign-upload` — returns a presigned PUT URL.
///
/// The client can PUT the file body directly to S3 using this URL, bypassing
/// this server for the data transfer.  Requires cloudish (or the real S3) to
/// respond with CORS headers so the browser can make a cross-origin PUT.
async fn presign_upload(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UploadUrlRequest>,
) -> Result<Json<UploadUrlResponse>, StatusCode> {
    let ext = req.filename.rsplit('.').next().unwrap_or("jpg").to_lowercase();
    let key = format!("{}.{}", uuid::Uuid::new_v4(), ext);

    let presign_cfg = PresigningConfig::expires_in(Duration::from_secs(300))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tracing::info!("generating presigned PUT URL for '{}' ({})", req.filename, req.content_type);

    let presigned = state
        .s3
        .put_object()
        .bucket(&state.bucket)
        .key(&key)
        .content_type(&req.content_type)
        .presigned(presign_cfg)
        .await
        .map_err(|e| {
            tracing::error!("presign PUT failed for '{}': {e}", req.filename);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    tracing::info!("presigned PUT URL generated: '{}' -> '{key}'", req.filename);
    Ok(Json(UploadUrlResponse {
        url: presigned.uri().to_string(),
        key,
    }))
}

/// `DELETE /api/photos/:key` — deletes a photo from S3.
///
/// The key in the URL must be percent-encoded if it contains slashes.
async fn delete_photo(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state
        .s3
        .delete_object()
        .bucket(&state.bucket)
        .key(&key)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("delete_object failed for '{key}': {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    tracing::info!("Deleted '{key}'");
    Ok(StatusCode::NO_CONTENT)
}
