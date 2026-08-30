//! SNS service emulator.
//!
//! Wire format: POST with `application/x-www-form-urlencoded` body,
//! `Action` field selects the operation. Responses are XML.
//!
//! Routing: dispatched from top_level_dispatch when SigV4 credential scope service = "sns".
//!
//! Storage layout (under data/sns/):
//!   topics/{topic_name}.json            — topic metadata
//!   subscriptions/{sub_arn_encoded}.json — subscription data
//!   tags/{arn_encoded}.json             — resource tags

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

const NS: &str = "https://sns.amazonaws.com/doc/2010-03-31/";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";
const ACCOUNT_ID: &str = "000000000000";
const REGION: &str = "eu-west-1";

// ── Data structures ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Topic {
    name: String,
    arn: String,
    display_name: String,
    delivery_policy: String,
    subscriptions_confirmed: u32,
    subscriptions_pending: u32,
    subscriptions_deleted: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Subscription {
    arn: String,
    topic_arn: String,
    protocol: String,
    endpoint: String,
    owner: String,
    /// "true" | "false"
    raw_message_delivery: String,
    /// "PendingConfirmation" | "Confirmed" | "Unsubscribed"
    subscription_status: String,
    filter_policy: String,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn response_metadata() -> String {
    format!("<ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>")
}

type XmlResponse = (StatusCode, [(axum::http::HeaderName, &'static str); 1], String);

fn xml_ok(op: &str, inner: &str) -> XmlResponse {
    let body = format!(
        r#"<{op}Response xmlns="{NS}">{inner}{meta}</{op}Response>"#,
        meta = response_metadata()
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn xml_error(code: StatusCode, error_code: &str, message: &str) -> XmlResponse {
    let body = format!(
        r#"<ErrorResponse xmlns="{NS}"><Error><Type>Sender</Type><Code>{error_code}</Code><Message>{message}</Message></Error>{meta}</ErrorResponse>"#,
        meta = response_metadata()
    );
    (code, [(header::CONTENT_TYPE, "text/xml")], body)
}

fn topic_arn(name: &str) -> String {
    format!("arn:aws:sns:{REGION}:{ACCOUNT_ID}:{name}")
}

fn topic_name_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit(':').next()
}

fn topic_key(name: &str) -> String {
    format!("topics/{name}.json")
}

fn sub_key(sub_arn: &str) -> String {
    let encoded = urlencoding::encode(sub_arn);
    format!("subscriptions/{encoded}.json")
}

fn tags_key(arn: &str) -> String {
    let encoded = urlencoding::encode(arn);
    format!("tags/{encoded}.json")
}

async fn load_topic(storage: &crate::storage::file::FileStorage, name: &str) -> Option<Topic> {
    storage.get(&topic_key(name)).await.ok()?.map(|b| serde_json::from_slice(&b).ok()).flatten()
}

async fn save_topic(storage: &crate::storage::file::FileStorage, topic: &Topic) -> anyhow::Result<()> {
    storage.put(&topic_key(&topic.name), serde_json::to_vec(topic)?).await
}

async fn load_sub(storage: &crate::storage::file::FileStorage, sub_arn: &str) -> Option<Subscription> {
    storage.get(&sub_key(sub_arn)).await.ok()?.map(|b| serde_json::from_slice(&b).ok()).flatten()
}

async fn save_sub(storage: &crate::storage::file::FileStorage, sub: &Subscription) -> anyhow::Result<()> {
    storage.put(&sub_key(&sub.arn), serde_json::to_vec(sub)?).await
}

async fn load_tags(storage: &crate::storage::file::FileStorage, arn: &str) -> HashMap<String, String> {
    storage.get(&tags_key(arn)).await.ok()
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

async fn save_tags(storage: &crate::storage::file::FileStorage, arn: &str, tags: &HashMap<String, String>) -> anyhow::Result<()> {
    storage.put(&tags_key(arn), serde_json::to_vec(tags)?).await
}

/// Parse `Prefix.entry.N.key` / `Prefix.entry.N.value` pairs from URL-encoded params.
fn parse_entry_map(params: &HashMap<String, String>, prefix: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut n = 1usize;
    loop {
        let key_param = format!("{prefix}.entry.{n}.key");
        let val_param = format!("{prefix}.entry.{n}.value");
        match (params.get(&key_param), params.get(&val_param)) {
            (Some(k), Some(v)) => { out.insert(k.clone(), v.clone()); }
            _ => break,
        }
        n += 1;
    }
    out
}

/// List all subscriptions stored under the subscriptions/ prefix.
async fn list_all_subs(storage: &crate::storage::file::FileStorage) -> Vec<Subscription> {
    let keys = storage.list("subscriptions/").await.unwrap_or_default();
    let mut out = Vec::new();
    for key in &keys {
        if let Ok(Some(b)) = storage.get(key).await {
            if let Ok(sub) = serde_json::from_slice::<Subscription>(&b) {
                out.push(sub);
            }
        }
    }
    out
}

// ── Numbered member list helper (for query-protocol responses) ───────────────

fn numbered_member<'a>(items: impl Iterator<Item = &'a str>, tag: &str) -> String {
    items.enumerate().map(|(i, v)| format!("<member><{tag}>{v}</{tag}></member>", )).collect::<Vec<_>>().join("")
}

// ── Actions ──────────────────────────────────────────────────────────────────

async fn create_topic(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let name = match params.get("Name") {
        Some(n) => n.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Name is required"),
    };

    // Idempotent: return existing topic if same name.
    let arn = topic_arn(&name);
    if load_topic(&state.sns, &name).await.is_none() {
        let topic = Topic {
            name: name.clone(),
            arn: arn.clone(),
            display_name: params.get("Attributes.entry.1.value")
                .or_else(|| params.get("Attributes.DisplayName"))
                .cloned()
                .unwrap_or_default(),
            delivery_policy: String::new(),
            subscriptions_confirmed: 0,
            subscriptions_pending: 0,
            subscriptions_deleted: 0,
        };
        if let Err(e) = save_topic(&state.sns, &topic).await {
            return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
        }
        // Initialise empty tags map.
        let _ = save_tags(&state.sns, &arn, &HashMap::new()).await;
    }

    xml_ok("CreateTopic", &format!("<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>"))
}

async fn delete_topic(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let name = match topic_name_from_arn(&arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };

    if load_topic(&state.sns, &name).await.is_none() {
        return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found");
    }

    let _ = state.sns.delete(&topic_key(&name)).await;
    let _ = state.sns.delete(&tags_key(&arn)).await;

    // Delete all subscriptions for this topic.
    let subs = list_all_subs(&state.sns).await;
    for sub in subs.iter().filter(|s| s.topic_arn == arn) {
        let _ = state.sns.delete(&sub_key(&sub.arn)).await;
    }

    xml_ok("DeleteTopic", "")
}

async fn list_topics(state: &Arc<AppState>, _params: &HashMap<String, String>) -> XmlResponse {
    let keys = state.sns.list("topics/").await.unwrap_or_default();
    let mut arns = Vec::new();
    for key in &keys {
        if let Ok(Some(b)) = state.sns.get(key).await {
            if let Ok(topic) = serde_json::from_slice::<Topic>(&b) {
                arns.push(topic.arn);
            }
        }
    }

    let members: String = arns.iter().map(|a| format!("<member><TopicArn>{a}</TopicArn></member>")).collect();
    xml_ok(
        "ListTopics",
        &format!("<ListTopicsResult><Topics>{members}</Topics></ListTopicsResult>"),
    )
}

fn topic_attributes_xml(topic: &Topic) -> String {
    let attrs = [
        ("TopicArn", topic.arn.as_str()),
        ("DisplayName", &topic.display_name),
        ("SubscriptionsConfirmed", &topic.subscriptions_confirmed.to_string()),
        ("SubscriptionsPending", &topic.subscriptions_pending.to_string()),
        ("SubscriptionsDeleted", &topic.subscriptions_deleted.to_string()),
        ("Owner", ACCOUNT_ID),
    ];
    let entries: String = attrs.iter().enumerate().map(|(i, (k, v))| {
        format!("<entry><key>{k}</key><value>{v}</value></entry>")
    }).collect();
    format!("<Attributes>{entries}</Attributes>")
}

async fn get_topic_attributes(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let name = match topic_name_from_arn(&arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };
    let topic = match load_topic(&state.sns, &name).await {
        Some(t) => t,
        None => return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found"),
    };
    xml_ok("GetTopicAttributes", &format!("<GetTopicAttributesResult>{}</GetTopicAttributesResult>", topic_attributes_xml(&topic)))
}

async fn set_topic_attributes(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let name = match topic_name_from_arn(&arn) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };
    let mut topic = match load_topic(&state.sns, &name).await {
        Some(t) => t,
        None => return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found"),
    };

    let attr_name = params.get("AttributeName").cloned().unwrap_or_default();
    let attr_value = params.get("AttributeValue").cloned().unwrap_or_default();
    match attr_name.as_str() {
        "DisplayName" => topic.display_name = attr_value,
        "DeliveryPolicy" => topic.delivery_policy = attr_value,
        _ => {}
    }
    if let Err(e) = save_topic(&state.sns, &topic).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }
    xml_ok("SetTopicAttributes", "")
}

async fn subscribe(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let topic_arn_val = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let protocol = match params.get("Protocol") {
        Some(p) => p.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Protocol is required"),
    };
    let endpoint = params.get("Endpoint").cloned().unwrap_or_default();

    let topic_name = match topic_name_from_arn(&topic_arn_val) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };

    if load_topic(&state.sns, &topic_name).await.is_none() {
        return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found");
    }

    // Check for existing subscription with same topic+protocol+endpoint.
    let existing = list_all_subs(&state.sns).await;
    if let Some(sub) = existing.iter().find(|s| {
        s.topic_arn == topic_arn_val && s.protocol == protocol && s.endpoint == endpoint
    }) {
        return xml_ok(
            "Subscribe",
            &format!("<SubscribeResult><SubscriptionArn>{}</SubscriptionArn></SubscribeResult>", sub.arn),
        );
    }

    let sub_attrs = parse_entry_map(params, "Attributes");
    let sub_arn = format!("{topic_arn_val}:{}", Uuid::new_v4());
    let sub = Subscription {
        arn: sub_arn.clone(),
        topic_arn: topic_arn_val.clone(),
        protocol,
        endpoint,
        owner: ACCOUNT_ID.to_string(),
        raw_message_delivery: sub_attrs.get("RawMessageDelivery").cloned().unwrap_or_else(|| "false".to_string()),
        // Auto-confirm all subscriptions (no real HTTP confirmation flow).
        subscription_status: "Confirmed".to_string(),
        filter_policy: sub_attrs.get("FilterPolicy").cloned().unwrap_or_default(),
    };

    if let Err(e) = save_sub(&state.sns, &sub).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }

    // Increment confirmed count on topic.
    if let Some(mut topic) = load_topic(&state.sns, &topic_name).await {
        topic.subscriptions_confirmed += 1;
        let _ = save_topic(&state.sns, &topic).await;
    }

    xml_ok(
        "Subscribe",
        &format!("<SubscribeResult><SubscriptionArn>{sub_arn}</SubscriptionArn></SubscribeResult>"),
    )
}

async fn unsubscribe(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let sub_arn = match params.get("SubscriptionArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "SubscriptionArn is required"),
    };

    let sub = match load_sub(&state.sns, &sub_arn).await {
        Some(s) => s,
        None => return xml_error(StatusCode::NOT_FOUND, "NotFound", "Subscription not found"),
    };

    let _ = state.sns.delete(&sub_key(&sub_arn)).await;

    // Decrement count on topic.
    if let Some(topic_name) = topic_name_from_arn(&sub.topic_arn) {
        if let Some(mut topic) = load_topic(&state.sns, topic_name).await {
            topic.subscriptions_confirmed = topic.subscriptions_confirmed.saturating_sub(1);
            topic.subscriptions_deleted += 1;
            let _ = save_topic(&state.sns, &topic).await;
        }
    }

    xml_ok("Unsubscribe", "")
}

async fn confirm_subscription(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    // In the real AWS API, ConfirmSubscription is used to confirm HTTP endpoint subscriptions.
    // We auto-confirm all subscriptions on Subscribe, so this is a no-op.
    // We still validate TopicArn exists.
    let topic_arn_val = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let topic_name = match topic_name_from_arn(&topic_arn_val) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };
    if load_topic(&state.sns, &topic_name).await.is_none() {
        return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found");
    }
    xml_ok(
        "ConfirmSubscription",
        &format!("<ConfirmSubscriptionResult><SubscriptionArn>{topic_arn_val}:confirmed</SubscriptionArn></ConfirmSubscriptionResult>"),
    )
}

async fn list_subscriptions(state: &Arc<AppState>, _params: &HashMap<String, String>) -> XmlResponse {
    let subs = list_all_subs(&state.sns).await;
    let members: String = subs.iter().map(|s| sub_member_xml(s)).collect();
    xml_ok(
        "ListSubscriptions",
        &format!("<ListSubscriptionsResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsResult>"),
    )
}

async fn list_subscriptions_by_topic(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let topic_arn_val = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let subs = list_all_subs(&state.sns).await;
    let members: String = subs.iter().filter(|s| s.topic_arn == topic_arn_val).map(|s| sub_member_xml(s)).collect();
    xml_ok(
        "ListSubscriptionsByTopic",
        &format!("<ListSubscriptionsByTopicResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsByTopicResult>"),
    )
}

fn sub_member_xml(s: &Subscription) -> String {
    format!(
        "<member><SubscriptionArn>{arn}</SubscriptionArn><Owner>{owner}</Owner>\
         <Protocol>{proto}</Protocol><Endpoint>{ep}</Endpoint><TopicArn>{topic}</TopicArn></member>",
        arn = s.arn,
        owner = s.owner,
        proto = s.protocol,
        ep = xml_escape(&s.endpoint),
        topic = s.topic_arn,
    )
}

fn sub_attributes_xml(s: &Subscription) -> String {
    let attrs = [
        ("SubscriptionArn", s.arn.as_str()),
        ("TopicArn", s.topic_arn.as_str()),
        ("Protocol", s.protocol.as_str()),
        ("Endpoint", s.endpoint.as_str()),
        ("Owner", s.owner.as_str()),
        ("RawMessageDelivery", s.raw_message_delivery.as_str()),
        ("SubscriptionStatus", s.subscription_status.as_str()),
        ("FilterPolicy", s.filter_policy.as_str()),
    ];
    let entries: String = attrs.iter().map(|(k, v)| {
        format!("<entry><key>{k}</key><value>{}</value></entry>", xml_escape(v))
    }).collect();
    format!("<Attributes>{entries}</Attributes>")
}

async fn get_subscription_attributes(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let sub_arn = match params.get("SubscriptionArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "SubscriptionArn is required"),
    };
    let sub = match load_sub(&state.sns, &sub_arn).await {
        Some(s) => s,
        None => return xml_error(StatusCode::NOT_FOUND, "NotFound", "Subscription not found"),
    };
    xml_ok("GetSubscriptionAttributes", &format!("<GetSubscriptionAttributesResult>{}</GetSubscriptionAttributesResult>", sub_attributes_xml(&sub)))
}

async fn set_subscription_attributes(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let sub_arn = match params.get("SubscriptionArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "SubscriptionArn is required"),
    };
    let mut sub = match load_sub(&state.sns, &sub_arn).await {
        Some(s) => s,
        None => return xml_error(StatusCode::NOT_FOUND, "NotFound", "Subscription not found"),
    };

    let attr_name = params.get("AttributeName").cloned().unwrap_or_default();
    let attr_value = params.get("AttributeValue").cloned().unwrap_or_default();
    match attr_name.as_str() {
        "RawMessageDelivery" => sub.raw_message_delivery = attr_value,
        "FilterPolicy" => sub.filter_policy = attr_value,
        _ => {}
    }

    if let Err(e) = save_sub(&state.sns, &sub).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }
    xml_ok("SetSubscriptionAttributes", "")
}

// ── Publish ───────────────────────────────────────────────────────────────────

async fn publish(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let topic_arn_val = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let message = match params.get("Message") {
        Some(m) => m.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Message is required"),
    };
    let subject = params.get("Subject").cloned().unwrap_or_default();
    let message_id = Uuid::new_v4().to_string();

    let topic_name = match topic_name_from_arn(&topic_arn_val) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };
    if load_topic(&state.sns, &topic_name).await.is_none() {
        return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found");
    }

    let subs = list_all_subs(&state.sns).await;
    let topic_subs: Vec<&Subscription> = subs.iter()
        .filter(|s| s.topic_arn == topic_arn_val && s.subscription_status == "Confirmed")
        .collect();

    for sub in topic_subs {
        deliver(state, sub, &topic_arn_val, &message_id, &message, &subject).await;
    }

    xml_ok(
        "Publish",
        &format!("<PublishResult><MessageId>{message_id}</MessageId></PublishResult>"),
    )
}

async fn publish_batch(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let topic_arn_val = match params.get("TopicArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "TopicArn is required"),
    };
    let topic_name = match topic_name_from_arn(&topic_arn_val) {
        Some(n) => n.to_string(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "Invalid TopicArn"),
    };
    if load_topic(&state.sns, &topic_name).await.is_none() {
        return xml_error(StatusCode::NOT_FOUND, "NotFound", "Topic not found");
    }

    let subs = list_all_subs(&state.sns).await;
    let topic_subs: Vec<&Subscription> = subs.iter()
        .filter(|s| s.topic_arn == topic_arn_val && s.subscription_status == "Confirmed")
        .collect();

    let mut successful = Vec::new();
    let mut n = 1usize;
    loop {
        let id_key = format!("PublishBatchRequestEntries.member.{n}.Id");
        let msg_key = format!("PublishBatchRequestEntries.member.{n}.Message");
        let id = match params.get(&id_key) {
            Some(v) => v.clone(),
            None => break,
        };
        let message = match params.get(&msg_key) {
            Some(v) => v.clone(),
            None => break,
        };
        let subject = params.get(&format!("PublishBatchRequestEntries.member.{n}.Subject"))
            .cloned()
            .unwrap_or_default();
        let message_id = Uuid::new_v4().to_string();

        for sub in &topic_subs {
            deliver(state, sub, &topic_arn_val, &message_id, &message, &subject).await;
        }
        successful.push(format!(
            "<member><Id>{id}</Id><MessageId>{message_id}</MessageId></member>"
        ));
        n += 1;
    }

    let succ_xml: String = successful.join("");
    xml_ok(
        "PublishBatch",
        &format!("<PublishBatchResult><Successful>{succ_xml}</Successful><Failed></Failed></PublishBatchResult>"),
    )
}

/// Deliver one SNS message to a single subscription.
async fn deliver(
    state: &Arc<AppState>,
    sub: &Subscription,
    topic_arn_val: &str,
    message_id: &str,
    message: &str,
    subject: &str,
) {
    let raw = sub.raw_message_delivery == "true";

    match sub.protocol.as_str() {
        "sqs" => {
            // Extract queue name from SQS ARN (arn:aws:sqs:region:account:queue_name).
            let queue_name = match sub.endpoint.rsplit(':').next() {
                Some(n) => n.to_string(),
                None => {
                    tracing::warn!("SNS: invalid SQS endpoint ARN: {}", sub.endpoint);
                    return;
                }
            };

            let body = if raw {
                message.to_string()
            } else {
                sns_envelope(topic_arn_val, message_id, message, subject)
            };

            if let Err(e) = crate::services::sqs::enqueue_to_queue(state, &queue_name, &body).await {
                tracing::warn!("SNS: failed to deliver to SQS queue {queue_name}: {e}");
            }
        }
        "http" | "https" => {
            let endpoint = sub.endpoint.clone();
            let body = if raw {
                message.to_string()
            } else {
                sns_envelope(topic_arn_val, message_id, message, subject)
            };
            // Fire-and-forget HTTP delivery in a background task.
            tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_default();
                if let Err(e) = client.post(&endpoint).body(body).send().await {
                    tracing::warn!("SNS: HTTP delivery to {endpoint} failed: {e}");
                }
            });
        }
        "email" | "email-json" => {
            // Just log — no real email sending.
            tracing::info!(
                message_id,
                to = sub.endpoint.as_str(),
                "SNS email delivery (not sent in emulator)"
            );
        }
        other => {
            tracing::warn!("SNS: unsupported protocol {other}, skipping delivery");
        }
    }
}

/// Build the standard SNS notification envelope JSON.
fn sns_envelope(topic_arn_val: &str, message_id: &str, message: &str, subject: &str) -> String {
    serde_json::json!({
        "Type": "Notification",
        "MessageId": message_id,
        "TopicArn": topic_arn_val,
        "Subject": subject,
        "Message": message,
        "Timestamp": now_iso8601(),
        "SignatureVersion": "1",
        "Signature": "EXAMPLE",
        "SigningCertURL": format!("https://sns.{REGION}.amazonaws.com/SimpleNotificationService-example.pem"),
        "UnsubscribeURL": format!("https://sns.{REGION}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=example"),
    })
    .to_string()
}

// ── Tags ──────────────────────────────────────────────────────────────────────

async fn tag_resource(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("ResourceArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "ResourceArn is required"),
    };
    let mut tags = load_tags(&state.sns, &arn).await;
    let mut n = 1usize;
    loop {
        let k = format!("Tags.member.{n}.Key");
        let v = format!("Tags.member.{n}.Value");
        match (params.get(&k), params.get(&v)) {
            (Some(key), Some(val)) => { tags.insert(key.clone(), val.clone()); }
            _ => break,
        }
        n += 1;
    }
    if let Err(e) = save_tags(&state.sns, &arn, &tags).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }
    xml_ok("TagResource", "")
}

async fn untag_resource(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("ResourceArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "ResourceArn is required"),
    };
    let mut tags = load_tags(&state.sns, &arn).await;
    let mut n = 1usize;
    loop {
        let k = format!("TagKeys.member.{n}");
        match params.get(&k) {
            Some(key) => { tags.remove(key); }
            None => break,
        }
        n += 1;
    }
    if let Err(e) = save_tags(&state.sns, &arn, &tags).await {
        return xml_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
    }
    xml_ok("UntagResource", "")
}

async fn list_tags_for_resource(state: &Arc<AppState>, params: &HashMap<String, String>) -> XmlResponse {
    let arn = match params.get("ResourceArn") {
        Some(a) => a.clone(),
        None => return xml_error(StatusCode::BAD_REQUEST, "InvalidParameter", "ResourceArn is required"),
    };
    let tags = load_tags(&state.sns, &arn).await;
    let entries: String = tags.iter().map(|(k, v)| {
        format!("<member><Key>{k}</Key><Value>{v}</Value></member>")
    }).collect();
    xml_ok(
        "ListTagsForResource",
        &format!("<ListTagsForResourceResult><Tags>{entries}</Tags></ListTagsForResourceResult>"),
    )
}

// ── Platform applications (stub — return empty lists) ────────────────────────

fn platform_app_stub(op: &str) -> XmlResponse {
    xml_ok(op, &format!("<{op}Result><PlatformApplications/></{op}Result>"))
}

// ── XML helpers ───────────────────────────────────────────────────────────────

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

// ── Router and dispatch ───────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/sns/", post(sns_direct))
}

async fn sns_direct(
    state: State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    dispatch(state, request).await
}

pub async fn dispatch(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let body = to_bytes(request.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let params: HashMap<String, String> =
        serde_urlencoded::from_bytes(&body).unwrap_or_default();
    let action = params.get("Action").cloned().unwrap_or_default();
    tracing::debug!("SNS action={action}");

    match action.as_str() {
        "CreateTopic"                => create_topic(&state, &params).await.into_response(),
        "DeleteTopic"                => delete_topic(&state, &params).await.into_response(),
        "ListTopics"                 => list_topics(&state, &params).await.into_response(),
        "GetTopicAttributes"         => get_topic_attributes(&state, &params).await.into_response(),
        "SetTopicAttributes"         => set_topic_attributes(&state, &params).await.into_response(),
        "Subscribe"                  => subscribe(&state, &params).await.into_response(),
        "Unsubscribe"                => unsubscribe(&state, &params).await.into_response(),
        "ConfirmSubscription"        => confirm_subscription(&state, &params).await.into_response(),
        "ListSubscriptions"          => list_subscriptions(&state, &params).await.into_response(),
        "ListSubscriptionsByTopic"   => list_subscriptions_by_topic(&state, &params).await.into_response(),
        "GetSubscriptionAttributes"  => get_subscription_attributes(&state, &params).await.into_response(),
        "SetSubscriptionAttributes"  => set_subscription_attributes(&state, &params).await.into_response(),
        "Publish"                    => publish(&state, &params).await.into_response(),
        "PublishBatch"               => publish_batch(&state, &params).await.into_response(),
        "TagResource"                => tag_resource(&state, &params).await.into_response(),
        "UntagResource"              => untag_resource(&state, &params).await.into_response(),
        "ListTagsForResource"        => list_tags_for_resource(&state, &params).await.into_response(),
        "CreatePlatformApplication"  => platform_app_stub("CreatePlatformApplication").into_response(),
        "DeletePlatformApplication"  => xml_ok("DeletePlatformApplication", "").into_response(),
        "ListPlatformApplications"   => platform_app_stub("ListPlatformApplications").into_response(),
        other => {
            tracing::warn!("unknown SNS action: {other}");
            xml_error(StatusCode::BAD_REQUEST, "InvalidAction", &format!("unknown action: {other}")).into_response()
        }
    }
}
