//! Cognito Identity Provider emulator.
//!
//! Routing: POST `/cognito/{version}/`, dispatched by `X-Amz-Target` header.
//! Target format: `AmazonCognitoIdentityProvider.<Operation>`

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    response::IntoResponse,
    routing::post,
};

use crate::{error::Error, services::AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/cognito/", post(dispatch))
}

async fn dispatch(
    State(_state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split('.').last())
        .unwrap_or("")
        .to_string();

    tracing::debug!("Cognito operation={target}");

    match target.as_str() {
        "CreateUserPool" => Err::<(), _>(Error::NotImplemented),
        "DeleteUserPool" => Err::<(), _>(Error::NotImplemented),
        "DescribeUserPool" => Err::<(), _>(Error::NotImplemented),
        "ListUserPools" => Err::<(), _>(Error::NotImplemented),
        "CreateUserPoolClient" => Err::<(), _>(Error::NotImplemented),
        "AdminCreateUser" => Err::<(), _>(Error::NotImplemented),
        "AdminDeleteUser" => Err::<(), _>(Error::NotImplemented),
        "AdminGetUser" => Err::<(), _>(Error::NotImplemented),
        "AdminInitiateAuth" => Err::<(), _>(Error::NotImplemented),
        "AdminRespondToAuthChallenge" => Err::<(), _>(Error::NotImplemented),
        "InitiateAuth" => Err::<(), _>(Error::NotImplemented),
        "RespondToAuthChallenge" => Err::<(), _>(Error::NotImplemented),
        "GetUser" => Err::<(), _>(Error::NotImplemented),
        "SignUp" => Err::<(), _>(Error::NotImplemented),
        "ConfirmSignUp" => Err::<(), _>(Error::NotImplemented),
        other => {
            tracing::warn!("unknown Cognito operation: {other}");
            Err::<(), _>(Error::InvalidRequest(format!("unknown operation: {other}")))
        }
    }
}
