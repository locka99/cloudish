use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub content_type: String,
    pub content_length: u64,
    pub etag: String,
    pub last_modified: String,
    pub user_meta: HashMap<String, String>,
    pub versions: Vec<VersionEntry>,
    pub delete_marker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntry {
    pub version_id: String,
    pub etag: String,
    pub last_modified: String,
    pub size: u64,
    pub is_latest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketMeta {
    pub name: String,
    pub creation_date: String,
    pub versioning_enabled: bool,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartMeta {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub user_meta: HashMap<String, String>,
    pub initiated: String,
}

pub struct S3Store {
    pub base: PathBuf,
}

impl S3Store {
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    fn bucket_dir(&self, bucket: &str) -> PathBuf {
        self.base.join(bucket)
    }

    fn bucket_meta_path(&self, bucket: &str) -> PathBuf {
        self.bucket_dir(bucket).join("_bucket.json")
    }

    fn object_data_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_dir(bucket).join(key)
    }

    fn object_meta_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_dir(bucket).join(format!("{}.meta", key))
    }

    fn version_data_path(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.bucket_dir(bucket)
            .join(".versions")
            .join(key)
            .join(version_id)
    }

    fn version_meta_path(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.bucket_dir(bucket)
            .join(".versions")
            .join(key)
            .join(format!("{}.meta", version_id))
    }

    fn multipart_dir(&self, upload_id: &str) -> PathBuf {
        self.base.join(".multipart").join(upload_id)
    }

    fn multipart_meta_path(&self, upload_id: &str) -> PathBuf {
        self.multipart_dir(upload_id).join("_meta.json")
    }

    fn part_path(&self, upload_id: &str, part_number: u32) -> PathBuf {
        self.multipart_dir(upload_id)
            .join(format!("part_{:05}", part_number))
    }

    async fn read_json<T: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &Path,
    ) -> Result<Option<T>> {
        match fs::read(path).await {
            Ok(data) => Ok(Some(serde_json::from_slice(&data)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_json<T: serde::Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, serde_json::to_vec_pretty(value)?).await?;
        Ok(())
    }

    // ── Buckets ──────────────────────────────────────────────────────────────

    pub async fn create_bucket(&self, name: &str, region: &str) -> Result<()> {
        fs::create_dir_all(self.bucket_dir(name)).await?;
        let meta = BucketMeta {
            name: name.to_string(),
            creation_date: now_iso(),
            versioning_enabled: false,
            region: region.to_string(),
        };
        self.write_json(&self.bucket_meta_path(name), &meta).await
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        Ok(self.bucket_meta_path(name).exists())
    }

    pub async fn get_bucket_meta(&self, name: &str) -> Result<Option<BucketMeta>> {
        self.read_json(&self.bucket_meta_path(name)).await
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        let dir = self.bucket_dir(name);
        if dir.exists() {
            fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketMeta>> {
        let mut out = Vec::new();
        if !self.base.exists() {
            return Ok(out);
        }
        let mut rd = fs::read_dir(&self.base).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            if let Some(meta) = self.get_bucket_meta(&name).await? {
                out.push(meta);
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    // ── Objects ───────────────────────────────────────────────────────────────

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
        user_meta: HashMap<String, String>,
    ) -> Result<ObjectMeta> {
        let etag = format!("{:x}", Md5::digest(&data));
        let now = now_iso();
        let size = data.len() as u64;

        let versioning_enabled = self
            .get_bucket_meta(bucket)
            .await?
            .map(|m| m.versioning_enabled)
            .unwrap_or(false);

        let mut meta = self
            .get_object_meta(bucket, key)
            .await?
            .unwrap_or_else(|| ObjectMeta {
                content_type: content_type.to_string(),
                content_length: size,
                etag: etag.clone(),
                last_modified: now.clone(),
                user_meta: user_meta.clone(),
                versions: Vec::new(),
                delete_marker: false,
            });

        if versioning_enabled {
            if let Some(existing) = self.get_object_data(bucket, key).await? {
                let version_id = Uuid::new_v4().to_string();
                let ver_data = self.version_data_path(bucket, key, &version_id);
                if let Some(p) = ver_data.parent() {
                    fs::create_dir_all(p).await?;
                }
                fs::write(&ver_data, existing).await?;
                let ver_meta_snapshot = meta.clone();
                self.write_json(
                    &self.version_meta_path(bucket, key, &version_id),
                    &ver_meta_snapshot,
                )
                .await?;
                for v in &mut meta.versions {
                    v.is_latest = false;
                }
                meta.versions.push(VersionEntry {
                    version_id,
                    etag: meta.etag.clone(),
                    last_modified: meta.last_modified.clone(),
                    size: meta.content_length,
                    is_latest: false,
                });
            }
        }

        let data_path = self.object_data_path(bucket, key);
        if let Some(p) = data_path.parent() {
            fs::create_dir_all(p).await?;
        }
        fs::write(&data_path, &data).await?;

        meta.content_type = content_type.to_string();
        meta.content_length = size;
        meta.etag = etag;
        meta.last_modified = now;
        meta.user_meta = user_meta;
        meta.delete_marker = false;

        self.write_json(&self.object_meta_path(bucket, key), &meta)
            .await?;
        Ok(meta)
    }

    pub async fn get_object_data(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
        match fs::read(self.object_data_path(bucket, key)).await {
            Ok(d) => Ok(Some(d)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_object_meta(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        self.read_json(&self.object_meta_path(bucket, key)).await
    }

    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        let data_path = self.version_data_path(bucket, key, version_id);
        let meta_path = self.version_meta_path(bucket, key, version_id);
        let data = match fs::read(&data_path).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let meta: ObjectMeta = match self.read_json(&meta_path).await? {
            Some(m) => m,
            None => return Ok(None),
        };
        Ok(Some((data, meta)))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        for path in [
            self.object_data_path(bucket, key),
            self.object_meta_path(bucket, key),
        ] {
            match fs::remove_file(&path).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: usize,
    ) -> Result<(Vec<(String, ObjectMeta)>, Vec<String>, bool)> {
        let bucket_dir = self.bucket_dir(bucket);
        let mut objects: Vec<(String, ObjectMeta)> = Vec::new();
        let mut common_prefixes: std::collections::HashSet<String> = Default::default();

        self.walk_objects(
            &bucket_dir.clone(),
            &bucket_dir,
            prefix,
            delimiter,
            max_keys + 1,
            &mut objects,
            &mut common_prefixes,
        )
        .await?;

        let truncated = objects.len() > max_keys;
        if truncated {
            objects.truncate(max_keys);
        }
        let mut prefixes: Vec<String> = common_prefixes.into_iter().collect();
        prefixes.sort();
        Ok((objects, prefixes, truncated))
    }

    fn walk_objects<'a>(
        &'a self,
        base: &'a Path,
        dir: &'a Path,
        prefix: &'a str,
        delimiter: &'a str,
        limit: usize,
        objects: &'a mut Vec<(String, ObjectMeta)>,
        common_prefixes: &'a mut std::collections::HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if objects.len() >= limit {
                return Ok(());
            }
            let mut rd = match fs::read_dir(dir).await {
                Ok(r) => r,
                Err(_) => return Ok(()),
            };
            let mut entries = Vec::new();
            while let Some(e) = rd.next_entry().await? {
                entries.push(e);
            }
            entries.sort_by_key(|e| e.file_name());

            for entry in entries {
                if objects.len() >= limit {
                    break;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with('_') || name.starts_with('.') || name.ends_with(".meta") {
                    continue;
                }
                let path = entry.path();
                let relative = path
                    .strip_prefix(base)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");

                if path.is_dir() {
                    if !delimiter.is_empty() {
                        // Treat the directory as a prefix key ending with the delimiter.
                        // e.g. for directory "a" with delimiter "/", the virtual key is "a/".
                        let dir_key = format!("{}{}", relative, delimiter);
                        if dir_key.starts_with(prefix) {
                            let after = &dir_key[prefix.len()..];
                            if let Some(idx) = after.find(delimiter) {
                                let cp = format!(
                                    "{}{}{}",
                                    prefix,
                                    &after[..idx],
                                    delimiter
                                );
                                common_prefixes.insert(cp);
                                continue;
                            }
                        }
                    }
                    self.walk_objects(
                        base,
                        &path,
                        prefix,
                        delimiter,
                        limit,
                        objects,
                        common_prefixes,
                    )
                    .await?;
                } else if path.is_file() {
                    if !relative.starts_with(prefix) {
                        continue;
                    }
                    // Meta file is stored as "{filename}.meta" (appended, not replacing extension).
                    let meta_path = {
                        let mut p = path.clone().into_os_string();
                        p.push(".meta");
                        std::path::PathBuf::from(p)
                    };
                    if let Some(meta) = self.read_json::<ObjectMeta>(&meta_path).await? {
                        if !meta.delete_marker {
                            objects.push((relative, meta));
                        }
                    }
                }
            }
            Ok(())
        })
    }

    // ── Multipart ─────────────────────────────────────────────────────────────

    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: &str,
        user_meta: HashMap<String, String>,
    ) -> Result<String> {
        let upload_id = Uuid::new_v4().to_string();
        let meta = MultipartMeta {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.to_string(),
            user_meta,
            initiated: now_iso(),
        };
        fs::create_dir_all(self.multipart_dir(&upload_id)).await?;
        self.write_json(&self.multipart_meta_path(&upload_id), &meta)
            .await?;
        Ok(upload_id)
    }

    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<String> {
        let etag = format!("{:x}", Md5::digest(&data));
        let path = self.part_path(upload_id, part_number);
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).await?;
        }
        fs::write(&path, data).await?;
        Ok(etag)
    }

    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<ObjectMeta> {
        let upload_meta: MultipartMeta = self
            .read_json(&self.multipart_meta_path(upload_id))
            .await?
            .context("multipart upload not found")?;

        let mut combined = Vec::new();
        for (part_number, _etag) in parts {
            let data = fs::read(self.part_path(upload_id, *part_number))
                .await
                .with_context(|| format!("part {part_number} not found"))?;
            combined.extend(data);
        }

        let meta = self
            .put_object(
                &upload_meta.bucket,
                &upload_meta.key,
                combined,
                &upload_meta.content_type,
                upload_meta.user_meta,
            )
            .await?;

        let _ = fs::remove_dir_all(self.multipart_dir(upload_id)).await;
        Ok(meta)
    }

    pub async fn abort_multipart(&self, upload_id: &str) -> Result<()> {
        let dir = self.multipart_dir(upload_id);
        if dir.exists() {
            fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }
}

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}
