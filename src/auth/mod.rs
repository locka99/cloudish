pub mod iam;
pub mod sigv4;

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use crate::services::AppState;

/// Parsed AWS credentials extracted from a request.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub region: String,
    pub service: String,
}

/// Axum middleware that extracts and validates AWS credentials.
pub async fn middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    match sigv4::extract_credentials(request.headers()) {
        Ok(Some(creds)) => {
            // TODO: verify SigV4 signature and apply IAM policy.
            tracing::debug!(access_key = %creds.access_key, "authenticated request");
            request.extensions_mut().insert(creds);
        }
        Ok(None) => {
            // Allow unauthenticated requests through for now; services can enforce auth.
            tracing::debug!("no credentials on request");
        }
        Err(e) => {
            tracing::warn!("credential extraction failed: {e}");
        }
    }
    next.run(request).await
}
