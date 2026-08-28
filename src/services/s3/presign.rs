use anyhow::{bail, Result};
use axum::http::Uri;
use chrono::{NaiveDateTime, Utc};
use std::collections::HashMap;

pub fn is_presigned(uri: &Uri) -> bool {
    uri.query()
        .map(|q| q.contains("X-Amz-Signature"))
        .unwrap_or(false)
}

pub fn validate_presigned(uri: &Uri) -> Result<String> {
    let query: HashMap<String, String> = uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| {
            let mut it = kv.splitn(2, '=');
            Some((decode(it.next()?), decode(it.next().unwrap_or(""))))
        })
        .collect();

    let algo = query.get("X-Amz-Algorithm").map(String::as_str).unwrap_or("");
    if algo != "AWS4-HMAC-SHA256" {
        bail!("missing or unsupported X-Amz-Algorithm");
    }

    let cred = query
        .get("X-Amz-Credential")
        .ok_or_else(|| anyhow::anyhow!("missing X-Amz-Credential"))?;
    let access_key = cred.split('/').next().unwrap_or("").to_string();
    if access_key.is_empty() {
        bail!("empty access key");
    }

    let date_str = query
        .get("X-Amz-Date")
        .ok_or_else(|| anyhow::anyhow!("missing X-Amz-Date"))?;
    let expires: u64 = query
        .get("X-Amz-Expires")
        .ok_or_else(|| anyhow::anyhow!("missing X-Amz-Expires"))?
        .parse()?;

    let signed_at = NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ")
        .map(|d| d.and_utc())?;
    let expiry = signed_at + chrono::Duration::seconds(expires as i64);
    if Utc::now() > expiry {
        bail!("presigned URL has expired");
    }

    Ok(access_key)
}

fn decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => {
                let h1 = chars.next().unwrap_or('0');
                let h2 = chars.next().unwrap_or('0');
                if let Ok(b) = u8::from_str_radix(&format!("{h1}{h2}"), 16) {
                    out.push(b as char);
                }
            }
            '+' => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}
