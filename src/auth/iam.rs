use serde::{Deserialize, Serialize};

/// Minimal IAM policy document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: StringOrList,
    #[serde(rename = "Resource")]
    pub resource: StringOrList,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrList {
    Single(String),
    Multiple(Vec<String>),
}

impl StringOrList {
    pub fn as_slice(&self) -> Vec<&str> {
        match self {
            StringOrList::Single(s) => vec![s.as_str()],
            StringOrList::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

/// Evaluates whether a given action on a resource is allowed by the policy.
/// Returns `true` if allowed.
pub fn is_allowed(policy: &Policy, action: &str, resource: &str) -> bool {
    let mut allowed = false;
    for stmt in &policy.statements {
        let action_match = stmt.action.as_slice().iter().any(|a| matches_pattern(a, action));
        let resource_match = stmt
            .resource
            .as_slice()
            .iter()
            .any(|r| matches_pattern(r, resource));
        if action_match && resource_match {
            if stmt.effect == Effect::Deny {
                return false;
            }
            allowed = true;
        }
    }
    allowed
}

/// Matches an IAM action/resource pattern (supports `*` wildcard).
fn matches_pattern(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Simple prefix wildcard matching.
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern.eq_ignore_ascii_case(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_wildcard() {
        let policy = Policy {
            version: "2012-10-17".into(),
            statements: vec![Statement {
                effect: Effect::Allow,
                action: StringOrList::Single("s3:*".into()),
                resource: StringOrList::Single("*".into()),
            }],
        };
        assert!(is_allowed(&policy, "s3:GetObject", "arn:aws:s3:::my-bucket/key"));
    }

    #[test]
    fn deny_overrides_allow() {
        let policy = Policy {
            version: "2012-10-17".into(),
            statements: vec![
                Statement {
                    effect: Effect::Allow,
                    action: StringOrList::Single("*".into()),
                    resource: StringOrList::Single("*".into()),
                },
                Statement {
                    effect: Effect::Deny,
                    action: StringOrList::Single("s3:DeleteObject".into()),
                    resource: StringOrList::Single("*".into()),
                },
            ],
        };
        assert!(!is_allowed(&policy, "s3:DeleteObject", "arn:aws:s3:::bucket/key"));
        assert!(is_allowed(&policy, "s3:GetObject", "arn:aws:s3:::bucket/key"));
    }
}
