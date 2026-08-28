pub mod auth;
pub mod error;
pub mod services;
pub mod storage;

use std::sync::Arc;
use axum::{Router, middleware};

pub use services::AppState;

pub async fn build_app(state: Arc<AppState>) -> anyhow::Result<Router> {
    let app = Router::new()
        .merge(services::router())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::middleware,
        ))
        .with_state(state);
    Ok(app)
}
