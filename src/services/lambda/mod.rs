//! Lambda service stub.
//!
//! Wire format: REST/JSON over paths rooted at /2015-03-31/.
//! Routing: path-based; SigV4 credential scope service = "lambda".

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, get, post, put},
};

use crate::{error::Error, services::AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Functions
        .route("/2015-03-31/functions", post(create_function))
        .route("/2015-03-31/functions", get(list_functions))
        .route("/2015-03-31/functions/{name}", get(get_function))
        .route("/2015-03-31/functions/{name}", delete(delete_function))
        .route("/2015-03-31/functions/{name}/code", put(update_function_code))
        .route(
            "/2015-03-31/functions/{name}/configuration",
            get(get_function_configuration),
        )
        .route(
            "/2015-03-31/functions/{name}/configuration",
            put(update_function_configuration),
        )
        .route(
            "/2015-03-31/functions/{name}/invocations",
            post(invoke_function),
        )
        // Aliases
        .route("/2015-03-31/functions/{name}/aliases", get(list_aliases))
        .route("/2015-03-31/functions/{name}/aliases", post(create_alias))
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            get(get_alias),
        )
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            put(update_alias),
        )
        .route(
            "/2015-03-31/functions/{name}/aliases/{alias}",
            delete(delete_alias),
        )
        // Resource-based policy
        .route("/2015-03-31/functions/{name}/policy", get(get_policy))
        .route(
            "/2015-03-31/functions/{name}/policy",
            post(add_permission),
        )
        .route(
            "/2015-03-31/functions/{name}/policy/{statement_id}",
            delete(remove_permission),
        )
        // Event source mappings
        .route(
            "/2015-03-31/event-source-mappings",
            get(list_event_source_mappings),
        )
        .route(
            "/2015-03-31/event-source-mappings",
            post(create_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            get(get_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            put(update_event_source_mapping),
        )
        .route(
            "/2015-03-31/event-source-mappings/{uuid}",
            delete(delete_event_source_mapping),
        )
        // Layers
        .route("/2015-03-31/layers", get(list_layers))
        .route(
            "/2015-03-31/layers/{layer_name}/versions",
            get(list_layer_versions),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions",
            post(publish_layer_version),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions/{version}",
            get(get_layer_version),
        )
        .route(
            "/2015-03-31/layers/{layer_name}/versions/{version}",
            delete(delete_layer_version),
        )
}

// ── Function handlers ────────────────────────────────────────────────────────

async fn create_function(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn list_functions(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn get_function(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn delete_function(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn update_function_code(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn get_function_configuration(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn update_function_configuration(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn invoke_function(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

// ── Alias handlers ───────────────────────────────────────────────────────────

async fn list_aliases(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn create_alias(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn get_alias(
    State(_): State<Arc<AppState>>,
    Path((_name, _alias)): Path<(String, String)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn update_alias(
    State(_): State<Arc<AppState>>,
    Path((_name, _alias)): Path<(String, String)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn delete_alias(
    State(_): State<Arc<AppState>>,
    Path((_name, _alias)): Path<(String, String)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

// ── Policy handlers ──────────────────────────────────────────────────────────

async fn get_policy(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn add_permission(
    State(_): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn remove_permission(
    State(_): State<Arc<AppState>>,
    Path((_name, _statement_id)): Path<(String, String)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

// ── Event source mapping handlers ────────────────────────────────────────────

async fn list_event_source_mappings(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn create_event_source_mapping(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn get_event_source_mapping(
    State(_): State<Arc<AppState>>,
    Path(_uuid): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn update_event_source_mapping(
    State(_): State<Arc<AppState>>,
    Path(_uuid): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn delete_event_source_mapping(
    State(_): State<Arc<AppState>>,
    Path(_uuid): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

// ── Layer handlers ───────────────────────────────────────────────────────────

async fn list_layers(State(_): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn list_layer_versions(
    State(_): State<Arc<AppState>>,
    Path(_layer_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn publish_layer_version(
    State(_): State<Arc<AppState>>,
    Path(_layer_name): Path<String>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn get_layer_version(
    State(_): State<Arc<AppState>>,
    Path((_layer_name, _version)): Path<(String, u64)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}

async fn delete_layer_version(
    State(_): State<Arc<AppState>>,
    Path((_layer_name, _version)): Path<(String, u64)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
