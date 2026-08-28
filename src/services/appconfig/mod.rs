//! AppConfig service emulator.
//!
//! Routing: REST paths under `/appconfig/applications/`.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
};

use crate::{error::Error, services::AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/appconfig/applications", get(list_applications).post(create_application))
        .route(
            "/appconfig/applications/{app_id}",
            get(get_application).delete(delete_application),
        )
        .route(
            "/appconfig/applications/{app_id}/environments",
            get(list_environments).post(create_environment),
        )
        .route(
            "/appconfig/applications/{app_id}/configurationprofiles",
            get(list_profiles).post(create_profile),
        )
        .route(
            "/appconfig/applications/{app_id}/environments/{env_id}/configurations/{profile_id}",
            get(get_configuration),
        )
}

async fn list_applications(State(_s): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn create_application(State(_s): State<Arc<AppState>>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn get_application(State(_s): State<Arc<AppState>>, Path(_id): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn delete_application(State(_s): State<Arc<AppState>>, Path(_id): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn list_environments(State(_s): State<Arc<AppState>>, Path(_app): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn create_environment(State(_s): State<Arc<AppState>>, Path(_app): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn list_profiles(State(_s): State<Arc<AppState>>, Path(_app): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn create_profile(State(_s): State<Arc<AppState>>, Path(_app): Path<String>) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
async fn get_configuration(
    State(_s): State<Arc<AppState>>,
    Path((_app, _env, _profile)): Path<(String, String, String)>,
) -> impl IntoResponse {
    Err::<(), _>(Error::NotImplemented)
}
