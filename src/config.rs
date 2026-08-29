use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub credentials: Vec<Credential>,
    pub rds: RdsConfig,
    pub s3: S3Config,
    pub dynamodb: DynamoDbConfig,
    pub sqs: SqsConfig,
    pub cognito: CognitoConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub region: String,
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Credential {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RdsConfig {
    pub proxy_dsn: String,
    pub proxy_port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub max_object_size: u64,
    pub max_presign_expiry: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DynamoDbConfig {
    pub ttl_sweep_interval: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct SqsConfig {
    pub max_wait_time: u64,
    pub default_retention: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct CognitoConfig {
    pub access_token_ttl: u64,
    pub refresh_token_ttl: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            credentials: vec![Credential {
                access_key: "test".into(),
                secret_key: "test".into(),
            }],
            rds: RdsConfig::default(),
            s3: S3Config::default(),
            dynamodb: DynamoDbConfig::default(),
            sqs: SqsConfig::default(),
            cognito: CognitoConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 4566,
            region: "eu-west-1".into(),
            account_id: "000000000000".into(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
        }
    }
}

impl Default for RdsConfig {
    fn default() -> Self {
        Self {
            proxy_dsn: "postgresql://localhost/cloudish".into(),
            proxy_port: 5433,
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            max_object_size: 5 * 1024 * 1024 * 1024,
            max_presign_expiry: 604800,
        }
    }
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            ttl_sweep_interval: 60,
        }
    }
}

impl Default for SqsConfig {
    fn default() -> Self {
        Self {
            max_wait_time: 20,
            default_retention: 345600,
        }
    }
}

impl Default for CognitoConfig {
    fn default() -> Self {
        Self {
            access_token_ttl: 3600,
            refresh_token_ttl: 2592000,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
        }
    }
}

impl Config {
    /// Load config from an explicit path.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse config file: {}", path.display()))
    }

    /// Load from the default search paths (`./cloudish.yaml`, then
    /// `~/.cloudish/config.yaml`), returning built-in defaults if neither exists.
    pub fn load_default() -> Result<Self> {
        let local = Path::new("cloudish.yaml");
        if local.exists() {
            return Self::load(local);
        }

        if let Some(home) = home_dir() {
            let fallback = home.join(".cloudish/config.yaml");
            if fallback.exists() {
                return Self::load(&fallback);
            }
        }

        Ok(Config::default())
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}
