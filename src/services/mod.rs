pub mod appconfig;
pub mod cognito;
pub mod dynamodb;
pub mod iam;
pub mod lambda;
pub mod rds;
pub mod s3;
pub mod ses;
pub mod sns;
pub mod sqs;

use std::sync::Arc;

use anyhow::Result;
use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};

use crate::storage::file::FileStorage;

/// Shared application state available to all service handlers.
pub struct AppState {
    pub s3: Arc<FileStorage>,
    pub dynamodb: Arc<FileStorage>,
    pub cognito: Arc<FileStorage>,
    pub appconfig: Arc<FileStorage>,
    pub ses: Arc<FileStorage>,
    pub sqs: Arc<FileStorage>,
    pub iam: Arc<FileStorage>,
    pub sns: Arc<FileStorage>,
    pub lambda: Arc<FileStorage>,
    // RDS is backed by a real Postgres connection — config held separately.
    pub rds_dsn: String,
}

impl AppState {
    pub async fn new() -> Result<Self> {
        Self::new_with_data_dir("data").await
    }

    pub async fn new_with_data_dir(base: impl AsRef<std::path::Path> + Send) -> Result<Self> {
        let base = base.as_ref();
        Ok(Self {
            s3: Arc::new(FileStorage::new(base.join("s3")).await?),
            dynamodb: Arc::new(FileStorage::new(base.join("dynamodb")).await?),
            cognito: Arc::new(FileStorage::new(base.join("cognito")).await?),
            appconfig: Arc::new(FileStorage::new(base.join("appconfig")).await?),
            ses: Arc::new(FileStorage::new(base.join("ses")).await?),
            sqs: Arc::new(FileStorage::new(base.join("sqs")).await?),
            iam: Arc::new(FileStorage::new(base.join("iam")).await?),
            sns: Arc::new(FileStorage::new(base.join("sns")).await?),
            lambda: Arc::new(FileStorage::new(base.join("lambda")).await?),
            rds_dsn: std::env::var("RDS_DSN")
                .unwrap_or_else(|_| "postgresql://localhost/cloudish".into()),
        })
    }

    pub async fn new_with_config(config: &crate::config::Config) -> Result<Self> {
        let base = &config.storage.data_dir;
        Ok(Self {
            s3: Arc::new(FileStorage::new(base.join("s3")).await?),
            dynamodb: Arc::new(FileStorage::new(base.join("dynamodb")).await?),
            cognito: Arc::new(FileStorage::new(base.join("cognito")).await?),
            appconfig: Arc::new(FileStorage::new(base.join("appconfig")).await?),
            ses: Arc::new(FileStorage::new(base.join("ses")).await?),
            sqs: Arc::new(FileStorage::new(base.join("sqs")).await?),
            iam: Arc::new(FileStorage::new(base.join("iam")).await?),
            sns: Arc::new(FileStorage::new(base.join("sns")).await?),
            lambda: Arc::new(FileStorage::new(base.join("lambda")).await?),
            rds_dsn: config.rds.proxy_dsn.clone(),
        })
    }
}

/// Top-level POST / dispatcher: routes by X-Amz-Target to the right service.
async fn top_level_dispatch(
    state: State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if target.starts_with("DynamoDB_") || target.starts_with("DynamoDBStreams_") {
        dynamodb::dispatch(state, request).await.into_response()
    } else if target.starts_with("AmazonCognitoIdentityProvider.")
        || target.starts_with("AWSCognitoIdentityProviderService.")
    {
        cognito::dispatch(state, request).await.into_response()
    } else {
        // Route by service from SigV4 credential scope
        let service = request
            .extensions()
            .get::<crate::auth::Credentials>()
            .map(|c| c.service.clone())
            .unwrap_or_default();

        match service.as_str() {
            "iam" | "sts" => iam::dispatch(State(state.0.clone()), request).await.into_response(),
            "sqs" => sqs::service_dispatch(State(state.0.clone()), request).await.into_response(),
            "sns" => sns::dispatch(State(state.0.clone()), request).await.into_response(),
            _ => (
                StatusCode::BAD_REQUEST,
                format!("unknown target: {target}"),
            )
                .into_response(),
        }
    }
}

/// Builds the combined router for all services.
pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/", post(top_level_dispatch))
        .merge(s3::router())
        .merge(dynamodb::router(state))
        .merge(cognito::router())
        .merge(appconfig::router())
        .merge(rds::router())
        .merge(ses::router())
        .merge(sqs::router())
        .merge(iam::router())
        .merge(sns::router())
        .merge(lambda::router())
}
