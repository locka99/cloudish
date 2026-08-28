use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use super::Storage;

/// File-backed storage. Keys are used as relative file paths under `base_dir`.
pub struct FileStorage {
    pub base_dir: PathBuf,
}

impl FileStorage {
    pub async fn new(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)
            .await
            .with_context(|| format!("creating storage dir {:?}", base_dir))?;
        Ok(Self { base_dir })
    }

    fn resolve(&self, key: &str) -> PathBuf {
        // Prevent path traversal.
        let key = key.trim_start_matches('/');
        self.base_dir.join(key)
    }
}

impl Storage for FileStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key);
        match fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let path = self.resolve(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, value).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.resolve(key);
        match fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(prefix);
        let mut keys = Vec::new();
        if !dir.exists() {
            return Ok(keys);
        }
        let mut stack = vec![dir.clone()];
        while let Some(current) = stack.pop() {
            let mut entries = fs::read_dir(&current).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let relative = path
                        .strip_prefix(&self.base_dir)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .replace('\\', "/");
                    keys.push(relative);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}
