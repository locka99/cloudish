use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("cloudish=debug".parse()?))
        .init();

    let state = Arc::new(cloudish::AppState::new().await?);
    let app = cloudish::build_app(state).await?;

    let addr = "0.0.0.0:4566";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("cloudish listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
