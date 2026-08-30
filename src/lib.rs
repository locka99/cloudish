pub mod auth;
pub mod config;
pub mod error;
pub mod services;
pub mod storage;

use std::sync::Arc;
use axum::{Router, middleware, extract::Request, response::Response};
use axum::middleware::Next;

pub use services::AppState;

async fn log_request_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    tracing::debug!("→ {} {}", method, uri);
    let response = next.run(request).await;
    tracing::debug!("← {} {} => {}", method, uri, response.status());
    response
}

pub async fn build_app(state: Arc<AppState>) -> anyhow::Result<Router> {
    let app = Router::new()
        .merge(services::router(state.clone()))
        .layer(middleware::from_fn(log_request_middleware))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::middleware,
        ))
        .with_state(state);
    Ok(app)
}
