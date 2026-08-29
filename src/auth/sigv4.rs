use axum::http::HeaderMap;

use super::Credentials;

/// Extracts AWS credentials from the `Authorization` header.
///
/// Expected format:
/// `AWS4-HMAC-SHA256 Credential=<key>/<date>/<region>/<service>/aws4_request, ...`
pub fn extract_credentials(headers: &HeaderMap) -> anyhow::Result<Option<Credentials>> {
    let Some(auth) = headers.get("authorization") else {
        return Ok(None);
    };
    let auth = auth.to_str()?;
    if !auth.starts_with("AWS4-HMAC-SHA256 ") {
        return Ok(None);
    }

    // Parse Credential= component.
    // The Authorization header format is:
    // AWS4-HMAC-SHA256 Credential=KEY/date/region/service/aws4_request, SignedHeaders=..., Signature=...
    // The first segment is "AWS4-HMAC-SHA256 Credential=..." so we need to find "Credential=" anywhere.
    let credential = auth
        .split(',')
        .find_map(|part| {
            let trimmed = part.trim();
            // Strip the algorithm prefix if present (first component)
            let after_algo = if let Some(idx) = trimmed.find("Credential=") {
                &trimmed[idx..]
            } else {
                trimmed
            };
            after_algo.strip_prefix("Credential=")
        })
        .ok_or_else(|| anyhow::anyhow!("missing Credential in Authorization header"))?
        .trim();

    // credential = <access_key>/<date>/<region>/<service>/aws4_request
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() < 5 {
        anyhow::bail!("malformed Credential scope: {credential}");
    }

    Ok(Some(Credentials {
        access_key: parts[0].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    }))
}

/// Verifies a SigV4 signature. Stub — always returns Ok for now.
pub fn verify_signature(
    _headers: &HeaderMap,
    _method: &str,
    _uri: &str,
    _body: &[u8],
    _creds: &Credentials,
    _secret_key: &str,
) -> anyhow::Result<()> {
    // TODO: implement full HMAC-SHA256 SigV4 verification.
    Ok(())
}
