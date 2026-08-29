use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Local AWS cloud emulator")]
struct Args {
    /// Path to the configuration file.
    /// Defaults to ./cloudish.yaml, then ~/.cloudish/config.yaml.
    #[arg(long, value_name = "FILE")]
    config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = match args.config {
        Some(ref path) => cloudish::config::Config::load(path)?,
        None => cloudish::config::Config::load_default()?,
    };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("cloudish=debug".parse()?))
        .init();

    let addr = format!("{}:{}", config.server.host, config.server.port);
    let state = Arc::new(cloudish::AppState::new_with_config(&config).await?);
    let app = cloudish::build_app(state).await?;

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("cloudish listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
