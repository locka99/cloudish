//! SES service emulator.
//!
//! Wire format: POST with `application/x-www-form-urlencoded` body,
//! `Action` field selects the operation. Responses are XML.
//!
//! Routing:
//!   POST /ses/           — convenience path for direct calls
//!   POST /               — routed here via top_level_dispatch when SigV4 service = "email"
//!
//! Storage layout (under data/ses/):
//!   identities/{encoded_address}.json   — verified identities
//!   sent/{message_id}.json              — sent emails (headers + body)

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{services::AppState, storage::Storage};

// ── Constants ────────────────────────────────────────────────────────────────

const NS: &str = "https://email.amazonaws.com/doc/2010-12-01/";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Identity {
    address: String,
    verified: bool,
    created_at: String,
}

/// A sent email stored to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SentMail {
    message_id: String,
    source: String,
    destinations: Vec<String>,
    subject: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
    /// Base64-encoded raw MIME message (populated by SendRawEmail).
    raw_message: Option<String>,
    sent_at: String,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn response_metadata() -> String {
    format!("<ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>")
}

type XmlResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn xml_response(op: &str, inner: &str) -> XmlResponse {
    let body = format!(
        r#"<{op}Response xmlns="{NS}">{inner}{meta}</{op}Response>"#,
        meta = response_metadata()
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_error(code: StatusCode, error_code: &str, message: &str) -> XmlResponse {
    let body = format!(
        r#"<ErrorResponse xmlns="{NS}"><Error><Code>{error_code}</Code><Message>{message}</Message></Error>{meta}</ErrorResponse>"#,
        meta = response_metadata()
    );
    (code, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Read a numbered member list: `{prefix}.1`, `{prefix}.2`, …
fn member_list(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        match params.get(&format!("{prefix}.{i}")) {
            Some(v) => {
                out.push(v.clone());
                i += 1;
            }
            None => break,
        }
    }
    out
}

fn identity_key(address: &str) -> String {
    format!("identities/{}.json", urlencoding::encode(address))
}

fn sent_key(message_id: &str) -> String {
    format!("sent/{message_id}.json")
}

// ── Operation handlers ────────────────────────────────────────────────────────

async fn verify_email_identity(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(address) = params.get("EmailAddress") else {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "EmailAddress is required",
        );
    };

    let identity = Identity {
        address: address.clone(),
        verified: true,
        created_at: now_iso8601(),
    };
    let data = match serde_json::to_vec(&identity) {
        Ok(d) => d,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };
    if let Err(e) = state.ses.put(&identity_key(address), data).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    xml_response("VerifyEmailIdentity", "")
}

async fn list_identities(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let identity_type = params.get("IdentityType").map(|s| s.as_str()).unwrap_or("EmailAddress");
    let max_items: usize = params
        .get("MaxItems")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
        .min(1000);

    let keys = match state.ses.list("identities/").await {
        Ok(k) => k,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };

    let mut members = String::new();
    let mut count = 0;
    for key in &keys {
        if count >= max_items {
            break;
        }
        let data = match state.ses.get(key).await {
            Ok(Some(d)) => d,
            _ => continue,
        };
        let identity: Identity = match serde_json::from_slice(&data) {
            Ok(i) => i,
            Err(_) => continue,
        };

        let is_domain = !identity.address.contains('@');
        let matches = match identity_type {
            "Domain" => is_domain,
            "EmailAddress" => !is_domain,
            _ => true,
        };
        if !matches {
            continue;
        }

        members.push_str(&format!("<member>{}</member>", xml_escape(&identity.address)));
        count += 1;
    }

    xml_response(
        "ListIdentities",
        &format!("<ListIdentitiesResult><Identities>{members}</Identities></ListIdentitiesResult>"),
    )
}

async fn get_identity_verification_attributes(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let addresses = member_list(params, "Identities.member");

    let mut entries = String::new();
    for address in &addresses {
        let verified = match state.ses.get(&identity_key(address)).await {
            Ok(Some(data)) => serde_json::from_slice::<Identity>(&data)
                .map(|i| i.verified)
                .unwrap_or(false),
            _ => false,
        };
        let status = if verified { "Success" } else { "Pending" };
        entries.push_str(&format!(
            "<entry>\
                <key>{}</key>\
                <value><VerificationStatus>{status}</VerificationStatus></value>\
            </entry>",
            xml_escape(address)
        ));
    }

    xml_response(
        "GetIdentityVerificationAttributes",
        &format!(
            "<GetIdentityVerificationAttributesResult>\
                <VerificationAttributes>{entries}</VerificationAttributes>\
            </GetIdentityVerificationAttributesResult>"
        ),
    )
}

async fn delete_identity(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> XmlResponse {
    let Some(identity) = params.get("Identity") else {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Identity is required",
        );
    };
    if let Err(e) = state.ses.delete(&identity_key(identity)).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }
    xml_response("DeleteIdentity", "")
}

async fn send_email(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let Some(source) = params.get("Source") else {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Source is required",
        );
    };

    let mut destinations: Vec<String> = Vec::new();
    for prefix in &[
        "Destination.ToAddresses.member",
        "Destination.CcAddresses.member",
        "Destination.BccAddresses.member",
    ] {
        destinations.extend(member_list(params, prefix));
    }

    let message_id = Uuid::new_v4().to_string();
    let mail = SentMail {
        message_id: message_id.clone(),
        source: source.clone(),
        destinations,
        subject: params.get("Message.Subject.Data").cloned(),
        body_text: params.get("Message.Body.Text.Data").cloned(),
        body_html: params.get("Message.Body.Html.Data").cloned(),
        raw_message: None,
        sent_at: now_iso8601(),
    };

    let data = match serde_json::to_vec(&mail) {
        Ok(d) => d,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };
    if let Err(e) = state.ses.put(&sent_key(&message_id), data).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    tracing::info!(
        message_id = %message_id,
        source = %source,
        "SES SendEmail stored"
    );

    xml_response(
        "SendEmail",
        &format!(
            "<SendEmailResult>\
                <MessageId>{message_id}@email.amazonses.com</MessageId>\
            </SendEmailResult>"
        ),
    )
}

async fn send_raw_email(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let source = params.get("Source").cloned().unwrap_or_default();
    let raw_data = params.get("RawMessage.Data").cloned().unwrap_or_default();
    let destinations = member_list(params, "Destinations.member");

    let message_id = Uuid::new_v4().to_string();
    let mail = SentMail {
        message_id: message_id.clone(),
        source: source.clone(),
        destinations,
        subject: None,
        body_text: None,
        body_html: None,
        raw_message: Some(raw_data),
        sent_at: now_iso8601(),
    };

    let data = match serde_json::to_vec(&mail) {
        Ok(d) => d,
        Err(e) => {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string())
        }
    };
    if let Err(e) = state.ses.put(&sent_key(&message_id), data).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    tracing::info!(
        message_id = %message_id,
        source = %source,
        "SES SendRawEmail stored"
    );

    xml_response(
        "SendRawEmail",
        &format!(
            "<SendRawEmailResult>\
                <MessageId>{message_id}@email.amazonses.com</MessageId>\
            </SendRawEmailResult>"
        ),
    )
}

fn get_send_quota() -> XmlResponse {
    xml_response(
        "GetSendQuota",
        "<GetSendQuotaResult>\
            <Max24HourSend>50000.0</Max24HourSend>\
            <MaxSendRate>14.0</MaxSendRate>\
            <SentLast24Hours>0.0</SentLast24Hours>\
        </GetSendQuotaResult>",
    )
}

fn get_send_statistics() -> XmlResponse {
    xml_response(
        "GetSendStatistics",
        "<GetSendStatisticsResult><SendDataPoints/></GetSendStatisticsResult>",
    )
}

// ── Router and dispatcher ─────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/ses/", post(dispatch_handler))
}

async fn dispatch_handler(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(State(state), request).await
}

pub async fn dispatch(State(state): State<Arc<AppState>>, request: Request) -> impl IntoResponse {
    // Action may appear in the query string or the URL-encoded body.
    let query_action = request
        .uri()
        .query()
        .and_then(|q| serde_urlencoded::from_str::<HashMap<String, String>>(q).ok())
        .and_then(|m| m.get("Action").cloned());

    let body_bytes = match to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                &format!("failed to read body: {e}"),
            )
            .into_response()
        }
    };

    let params: HashMap<String, String> = if body_bytes.is_empty() {
        HashMap::new()
    } else {
        match serde_urlencoded::from_bytes(&body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return xml_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    &format!("failed to parse body: {e}"),
                )
                .into_response()
            }
        }
    };

    let action = params.get("Action").cloned().or(query_action).unwrap_or_default();
    tracing::debug!("SES action={action}");

    match action.as_str() {
        "SendEmail" => send_email(&state, &params).await.into_response(),
        "SendRawEmail" => send_raw_email(&state, &params).await.into_response(),
        "VerifyEmailIdentity" => verify_email_identity(&state, &params).await.into_response(),
        "ListIdentities" => list_identities(&state, &params).await.into_response(),
        "GetIdentityVerificationAttributes" => {
            get_identity_verification_attributes(&state, &params).await.into_response()
        }
        "DeleteIdentity" => delete_identity(&state, &params).await.into_response(),
        "GetSendQuota" => get_send_quota().into_response(),
        "GetSendStatistics" => get_send_statistics().into_response(),
        other => {
            tracing::warn!("unknown SES action: {other}");
            xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                &format!("unknown action: {other}"),
            )
            .into_response()
        }
    }
}
