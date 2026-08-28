pub mod appconfig;
pub mod cognito;
pub mod dynamodb;
pub mod iam;
pub mod rds;
pub mod s3;
pub mod ses;
pub mod sqs;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;

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
            rds_dsn: std::env::var("RDS_DSN")
                .unwrap_or_else(|_| "postgresql://localhost/cloudish".into()),
        })
    }
}

/// Builds the combined router for all services.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .merge(s3::router())
        .merge(dynamodb::router())
        .merge(cognito::router())
        .merge(appconfig::router())
        .merge(rds::router())
        .merge(ses::router())
        .merge(sqs::router())
        .merge(iam::router())
}
