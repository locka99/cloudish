//! Integration tests for the IAM service.
//!
//! Run with: `cargo test --test iam -- --nocapture`

use aws_sdk_iam::{
    Client as IamClient,
    config::{BehaviorVersion, Credentials, Region},
};
use uuid::Uuid;

// ── Server startup ─────────────────────────────────────────────────────────────

fn start_server_sync() -> u16 {
    use std::net::TcpListener as StdListener;

    let std_listener = StdListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();

    let data_dir = format!("data/test_{port}");

    if std::path::Path::new(&data_dir).exists() {
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            let state = std::sync::Arc::new(
                cloudish::AppState::new_with_data_dir(&data_dir)
                    .await
                    .unwrap(),
            );
            let app = cloudish::build_app(state).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });

    for _ in 0..20 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    port
}

async fn port() -> u16 {
    static INIT: std::sync::Once = std::sync::Once::new();
    static PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

    let p = PORT.load(std::sync::atomic::Ordering::Acquire);
    if p != 0 {
        return p;
    }

    tokio::task::spawn_blocking(|| {
        INIT.call_once(|| {
            let port = start_server_sync();
            PORT.store(port, std::sync::atomic::Ordering::Release);
        });
        PORT.load(std::sync::atomic::Ordering::Acquire)
    })
    .await
    .unwrap()
}

fn iam_client(port: u16) -> IamClient {
    let creds = Credentials::new("test", "test", None, None, "cloudish");
    let conf = aws_sdk_iam::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("eu-west-1"))
        .build();
    IamClient::from_conf(conf)
}

// ── Drop guard ─────────────────────────────────────────────────────────────────

struct UserGuard {
    client: IamClient,
    user_name: String,
}

impl Drop for UserGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let user_name = self.user_name.clone();
        // Best-effort cleanup: run in a dedicated thread with its own runtime to avoid
        // "cannot block the current thread from within a Rust async context" panic.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                // Delete inline policies
                if let Ok(resp) = client.list_user_policies().user_name(&user_name).send().await {
                    for pname in resp.policy_names() {
                        let _ = client
                            .delete_user_policy()
                            .user_name(&user_name)
                            .policy_name(pname)
                            .send()
                            .await;
                    }
                }
                // Delete access keys
                if let Ok(resp) = client.list_access_keys().user_name(&user_name).send().await {
                    for km in resp.access_key_metadata() {
                        if let Some(kid) = km.access_key_id() {
                            let _ = client
                                .delete_access_key()
                                .user_name(&user_name)
                                .access_key_id(kid)
                                .send()
                                .await;
                        }
                    }
                }
                // Detach managed policies
                if let Ok(resp) = client
                    .list_attached_user_policies()
                    .user_name(&user_name)
                    .send()
                    .await
                {
                    for ap in resp.attached_policies() {
                        if let Some(arn) = ap.policy_arn() {
                            let _ = client
                                .detach_user_policy()
                                .user_name(&user_name)
                                .policy_arn(arn)
                                .send()
                                .await;
                        }
                    }
                }
                let _ = client.delete_user().user_name(&user_name).send().await;
            });
        })
        .join()
        .ok();
    }
}

struct RoleGuard {
    client: IamClient,
    role_name: String,
}

impl Drop for RoleGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let role_name = self.role_name.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                // Delete inline policies
                if let Ok(resp) = client.list_role_policies().role_name(&role_name).send().await {
                    for pname in resp.policy_names() {
                        let _ = client
                            .delete_role_policy()
                            .role_name(&role_name)
                            .policy_name(pname)
                            .send()
                            .await;
                    }
                }
                // Detach managed policies
                if let Ok(resp) = client
                    .list_attached_role_policies()
                    .role_name(&role_name)
                    .send()
                    .await
                {
                    for ap in resp.attached_policies() {
                        if let Some(arn) = ap.policy_arn() {
                            let _ = client
                                .detach_role_policy()
                                .role_name(&role_name)
                                .policy_arn(arn)
                                .send()
                                .await;
                        }
                    }
                }
                let _ = client.delete_role().role_name(&role_name).send().await;
            });
        })
        .join()
        .ok();
    }
}

struct PolicyGuard {
    client: IamClient,
    policy_arn: String,
}

impl Drop for PolicyGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let policy_arn = self.policy_arn.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let _ = client.delete_policy().policy_arn(&policy_arn).send().await;
            });
        })
        .join()
        .ok();
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_user_lifecycle() {
    let port = port().await;
    let client = iam_client(port);

    let user_name = format!("test-user-{}", Uuid::new_v4().simple());
    let _guard = UserGuard {
        client: client.clone(),
        user_name: user_name.clone(),
    };

    // Create user
    let create_resp = client
        .create_user()
        .user_name(&user_name)
        .path("/")
        .send()
        .await
        .expect("CreateUser failed");

    let user = create_resp.user().expect("user missing from response");
    assert_eq!(user.user_name(), &user_name);
    assert!(user.arn().contains(&user_name));

    // Get user
    let get_resp = client
        .get_user()
        .user_name(&user_name)
        .send()
        .await
        .expect("GetUser failed");

    let fetched = get_resp.user().expect("user missing");
    assert_eq!(fetched.user_name(), &user_name);

    // List users — user should appear
    let list_resp = client
        .list_users()
        .send()
        .await
        .expect("ListUsers failed");
    let found = list_resp
        .users()
        .iter()
        .any(|u| u.user_name() == user_name);
    assert!(found, "user not in list");

    // Update user name
    let new_name = format!("renamed-user-{}", Uuid::new_v4().simple());
    client
        .update_user()
        .user_name(&user_name)
        .new_user_name(&new_name)
        .send()
        .await
        .expect("UpdateUser failed");

    // Verify renamed user exists
    let get_renamed = client
        .get_user()
        .user_name(&new_name)
        .send()
        .await
        .expect("GetUser after rename failed");
    assert_eq!(get_renamed.user().unwrap().user_name(), &new_name);

    // Delete the renamed user (update guard to clean up new name)
    client
        .delete_user()
        .user_name(&new_name)
        .send()
        .await
        .expect("DeleteUser failed");

    // Verify deleted
    let err = client.get_user().user_name(&new_name).send().await;
    assert!(err.is_err(), "user should be deleted");
}

#[tokio::test]
async fn test_access_keys() {
    let port = port().await;
    let client = iam_client(port);

    let user_name = format!("ak-user-{}", Uuid::new_v4().simple());
    let _guard = UserGuard {
        client: client.clone(),
        user_name: user_name.clone(),
    };

    client
        .create_user()
        .user_name(&user_name)
        .send()
        .await
        .expect("CreateUser failed");

    // CreateAccessKey — secret is returned once
    let create_resp = client
        .create_access_key()
        .user_name(&user_name)
        .send()
        .await
        .expect("CreateAccessKey failed");

    let ak = create_resp.access_key().expect("access_key missing from CreateAccessKey response");
    let key_id = ak.access_key_id().to_string();
    let secret = ak.secret_access_key().to_string();
    assert!(key_id.starts_with("AKIA"), "key id should start with AKIA");
    assert!(!secret.is_empty(), "secret should be non-empty");
    assert_eq!(ak.status().as_str(), "Active");

    // ListAccessKeys
    let list_resp = client
        .list_access_keys()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListAccessKeys failed");
    let found = list_resp
        .access_key_metadata()
        .iter()
        .any(|k| k.access_key_id().unwrap_or("") == key_id);
    assert!(found, "key not found in list");

    // UpdateAccessKey — deactivate
    client
        .update_access_key()
        .user_name(&user_name)
        .access_key_id(&key_id)
        .status(aws_sdk_iam::types::StatusType::Inactive)
        .send()
        .await
        .expect("UpdateAccessKey failed");

    let list_resp2 = client
        .list_access_keys()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListAccessKeys failed");
    let updated = list_resp2
        .access_key_metadata()
        .iter()
        .find(|k| k.access_key_id().unwrap_or("") == key_id)
        .expect("key not found");
    assert_eq!(updated.status().unwrap().as_str(), "Inactive");

    // DeleteAccessKey
    client
        .delete_access_key()
        .user_name(&user_name)
        .access_key_id(&key_id)
        .send()
        .await
        .expect("DeleteAccessKey failed");

    let list_resp3 = client
        .list_access_keys()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListAccessKeys failed");
    let still_found = list_resp3
        .access_key_metadata()
        .iter()
        .any(|k| k.access_key_id().unwrap_or("") == key_id);
    assert!(!still_found, "key should be deleted");
}

#[tokio::test]
async fn test_user_inline_policies() {
    let port = port().await;
    let client = iam_client(port);

    let user_name = format!("policy-user-{}", Uuid::new_v4().simple());
    let _guard = UserGuard {
        client: client.clone(),
        user_name: user_name.clone(),
    };

    client
        .create_user()
        .user_name(&user_name)
        .send()
        .await
        .expect("CreateUser failed");

    let policy_name = "TestInlinePolicy";
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;

    // PutUserPolicy
    client
        .put_user_policy()
        .user_name(&user_name)
        .policy_name(policy_name)
        .policy_document(policy_doc)
        .send()
        .await
        .expect("PutUserPolicy failed");

    // GetUserPolicy
    let get_resp = client
        .get_user_policy()
        .user_name(&user_name)
        .policy_name(policy_name)
        .send()
        .await
        .expect("GetUserPolicy failed");
    // SDK returns URL-decoded doc
    let returned_doc = get_resp.policy_document();
    assert!(!returned_doc.is_empty());
    // Should contain the version
    let decoded = urlencoding::decode(returned_doc).unwrap_or(returned_doc.into());
    assert!(decoded.contains("2012-10-17"), "policy doc should contain version");

    // ListUserPolicies
    let list_resp = client
        .list_user_policies()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListUserPolicies failed");
    assert!(
        list_resp.policy_names().contains(&policy_name.to_string()),
        "policy not in list"
    );

    // DeleteUserPolicy
    client
        .delete_user_policy()
        .user_name(&user_name)
        .policy_name(policy_name)
        .send()
        .await
        .expect("DeleteUserPolicy failed");

    let list_resp2 = client
        .list_user_policies()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListUserPolicies failed");
    assert!(
        !list_resp2.policy_names().contains(&policy_name.to_string()),
        "policy should be deleted"
    );
}

#[tokio::test]
async fn test_role_lifecycle() {
    let port = port().await;
    let client = iam_client(port);

    let role_name = format!("test-role-{}", Uuid::new_v4().simple());
    let _guard = RoleGuard {
        client: client.clone(),
        role_name: role_name.clone(),
    };

    let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

    // CreateRole
    let create_resp = client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(trust_policy)
        .send()
        .await
        .expect("CreateRole failed");

    let role = create_resp.role().expect("role missing from CreateRole response");
    assert_eq!(role.role_name(), &role_name);
    assert!(role.arn().contains(&role_name));

    // GetRole
    let get_resp = client
        .get_role()
        .role_name(&role_name)
        .send()
        .await
        .expect("GetRole failed");
    assert_eq!(get_resp.role().expect("role missing from GetRole response").role_name(), &role_name);

    // ListRoles
    let list_resp = client.list_roles().send().await.expect("ListRoles failed");
    let found = list_resp.roles().iter().any(|r| r.role_name() == role_name);
    assert!(found, "role not in list");

    // DeleteRole
    client
        .delete_role()
        .role_name(&role_name)
        .send()
        .await
        .expect("DeleteRole failed");

    let err = client.get_role().role_name(&role_name).send().await;
    assert!(err.is_err(), "role should be deleted");
}

#[tokio::test]
async fn test_role_inline_policies() {
    let port = port().await;
    let client = iam_client(port);

    let role_name = format!("policy-role-{}", Uuid::new_v4().simple());
    let _guard = RoleGuard {
        client: client.clone(),
        role_name: role_name.clone(),
    };

    let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(trust_policy)
        .send()
        .await
        .expect("CreateRole failed");

    let policy_name = "TestRoleInlinePolicy";
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}"#;

    // PutRolePolicy
    client
        .put_role_policy()
        .role_name(&role_name)
        .policy_name(policy_name)
        .policy_document(policy_doc)
        .send()
        .await
        .expect("PutRolePolicy failed");

    // GetRolePolicy
    let get_resp = client
        .get_role_policy()
        .role_name(&role_name)
        .policy_name(policy_name)
        .send()
        .await
        .expect("GetRolePolicy failed");
    let returned_doc = get_resp.policy_document();
    let decoded = urlencoding::decode(returned_doc).unwrap_or(returned_doc.into());
    assert!(decoded.contains("2012-10-17"));

    // ListRolePolicies
    let list_resp = client
        .list_role_policies()
        .role_name(&role_name)
        .send()
        .await
        .expect("ListRolePolicies failed");
    assert!(list_resp.policy_names().contains(&policy_name.to_string()));

    // DeleteRolePolicy
    client
        .delete_role_policy()
        .role_name(&role_name)
        .policy_name(policy_name)
        .send()
        .await
        .expect("DeleteRolePolicy failed");

    let list_resp2 = client
        .list_role_policies()
        .role_name(&role_name)
        .send()
        .await
        .expect("ListRolePolicies failed");
    assert!(!list_resp2.policy_names().contains(&policy_name.to_string()));
}

#[tokio::test]
async fn test_managed_policies() {
    let port = port().await;
    let client = iam_client(port);

    let policy_name = format!("test-policy-{}", Uuid::new_v4().simple());
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

    // CreatePolicy
    let create_resp = client
        .create_policy()
        .policy_name(&policy_name)
        .policy_document(policy_doc)
        .send()
        .await
        .expect("CreatePolicy failed");

    let policy = create_resp.policy().expect("policy missing");
    let policy_arn = policy.arn().expect("policy arn missing").to_string();

    let _guard = PolicyGuard {
        client: client.clone(),
        policy_arn: policy_arn.clone(),
    };

    assert!(policy_arn.contains(&policy_name));

    // GetPolicy
    let get_resp = client
        .get_policy()
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("GetPolicy failed");
    assert_eq!(get_resp.policy().unwrap().policy_name().unwrap_or(""), &policy_name);

    // ListPolicies
    let list_resp = client.list_policies().send().await.expect("ListPolicies failed");
    let found = list_resp
        .policies()
        .iter()
        .any(|p| p.policy_name().unwrap_or("") == policy_name);
    assert!(found, "policy not in list");

    // GetPolicyVersion
    let ver_resp = client
        .get_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v1")
        .send()
        .await
        .expect("GetPolicyVersion failed");
    let pv = ver_resp.policy_version().expect("policy version missing");
    let doc = pv.document().unwrap_or("");
    let decoded = urlencoding::decode(doc).unwrap_or(doc.into());
    assert!(decoded.contains("2012-10-17"));

    // DeletePolicy
    client
        .delete_policy()
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("DeletePolicy failed");

    let err = client.get_policy().policy_arn(&policy_arn).send().await;
    assert!(err.is_err(), "policy should be deleted");
}

#[tokio::test]
async fn test_attach_detach_user_policy() {
    let port = port().await;
    let client = iam_client(port);

    let user_name = format!("attach-user-{}", Uuid::new_v4().simple());
    let policy_name = format!("attach-policy-{}", Uuid::new_v4().simple());

    let _user_guard = UserGuard {
        client: client.clone(),
        user_name: user_name.clone(),
    };

    client
        .create_user()
        .user_name(&user_name)
        .send()
        .await
        .expect("CreateUser failed");

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let create_resp = client
        .create_policy()
        .policy_name(&policy_name)
        .policy_document(policy_doc)
        .send()
        .await
        .expect("CreatePolicy failed");
    let policy_arn = create_resp.policy().unwrap().arn().expect("policy arn missing").to_string();

    let _policy_guard = PolicyGuard {
        client: client.clone(),
        policy_arn: policy_arn.clone(),
    };

    // AttachUserPolicy
    client
        .attach_user_policy()
        .user_name(&user_name)
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("AttachUserPolicy failed");

    // ListAttachedUserPolicies
    let list_resp = client
        .list_attached_user_policies()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListAttachedUserPolicies failed");
    let found = list_resp
        .attached_policies()
        .iter()
        .any(|p| p.policy_arn().unwrap_or("") == policy_arn);
    assert!(found, "policy not in attached list");

    // DetachUserPolicy
    client
        .detach_user_policy()
        .user_name(&user_name)
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("DetachUserPolicy failed");

    let list_resp2 = client
        .list_attached_user_policies()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListAttachedUserPolicies failed");
    let still_found = list_resp2
        .attached_policies()
        .iter()
        .any(|p| p.policy_arn().unwrap_or("") == policy_arn);
    assert!(!still_found, "policy should be detached");
}

#[tokio::test]
async fn test_attach_detach_role_policy() {
    let port = port().await;
    let client = iam_client(port);

    let role_name = format!("attach-role-{}", Uuid::new_v4().simple());
    let policy_name = format!("role-attach-policy-{}", Uuid::new_v4().simple());

    let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

    let _role_guard = RoleGuard {
        client: client.clone(),
        role_name: role_name.clone(),
    };

    client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(trust_policy)
        .send()
        .await
        .expect("CreateRole failed");

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:*","Resource":"*"}]}"#;
    let create_resp = client
        .create_policy()
        .policy_name(&policy_name)
        .policy_document(policy_doc)
        .send()
        .await
        .expect("CreatePolicy failed");
    let policy_arn = create_resp.policy().unwrap().arn().expect("policy arn missing").to_string();

    let _policy_guard = PolicyGuard {
        client: client.clone(),
        policy_arn: policy_arn.clone(),
    };

    // AttachRolePolicy
    client
        .attach_role_policy()
        .role_name(&role_name)
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("AttachRolePolicy failed");

    // ListAttachedRolePolicies
    let list_resp = client
        .list_attached_role_policies()
        .role_name(&role_name)
        .send()
        .await
        .expect("ListAttachedRolePolicies failed");
    let found = list_resp
        .attached_policies()
        .iter()
        .any(|p| p.policy_arn().unwrap_or("") == policy_arn);
    assert!(found, "policy not in attached list");

    // DetachRolePolicy
    client
        .detach_role_policy()
        .role_name(&role_name)
        .policy_arn(&policy_arn)
        .send()
        .await
        .expect("DetachRolePolicy failed");

    let list_resp2 = client
        .list_attached_role_policies()
        .role_name(&role_name)
        .send()
        .await
        .expect("ListAttachedRolePolicies failed");
    let still_found = list_resp2
        .attached_policies()
        .iter()
        .any(|p| p.policy_arn().unwrap_or("") == policy_arn);
    assert!(!still_found, "policy should be detached");
}

#[tokio::test]
async fn test_user_tags() {
    let port = port().await;
    let client = iam_client(port);

    let user_name = format!("tag-user-{}", Uuid::new_v4().simple());
    let _guard = UserGuard {
        client: client.clone(),
        user_name: user_name.clone(),
    };

    client
        .create_user()
        .user_name(&user_name)
        .send()
        .await
        .expect("CreateUser failed");

    // TagUser
    client
        .tag_user()
        .user_name(&user_name)
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("Env")
                .value("prod")
                .build()
                .unwrap(),
        )
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("Team")
                .value("ops")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("TagUser failed");

    // ListUserTags
    let list_resp = client
        .list_user_tags()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListUserTags failed");
    let tags = list_resp.tags();
    let env_tag = tags.iter().find(|t| t.key() == "Env");
    assert!(env_tag.is_some(), "Env tag not found");
    assert_eq!(env_tag.unwrap().value(), "prod");

    // UntagUser
    client
        .untag_user()
        .user_name(&user_name)
        .tag_keys("Env")
        .send()
        .await
        .expect("UntagUser failed");

    let list_resp2 = client
        .list_user_tags()
        .user_name(&user_name)
        .send()
        .await
        .expect("ListUserTags failed");
    let env_gone = list_resp2.tags().iter().all(|t| t.key() != "Env");
    assert!(env_gone, "Env tag should be removed");
    let team_still = list_resp2.tags().iter().any(|t| t.key() == "Team");
    assert!(team_still, "Team tag should remain");
}

#[tokio::test]
async fn test_get_caller_identity() {
    let port = port().await;

    // Use raw reqwest since the STS endpoint format is the same form-encoded / XML
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("X-Amz-Date", "20240101T000000Z")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/eu-west-1/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .body("Action=GetCallerIdentity&Version=2011-06-15")
        .send()
        .await
        .expect("request failed");

    assert!(resp.status().is_success(), "expected 200, got {}", resp.status());

    let body = resp.text().await.expect("failed to read body");
    assert!(body.contains("000000000000"), "account ID missing from response");
    assert!(body.contains("GetCallerIdentityResponse"), "response element missing");
}
