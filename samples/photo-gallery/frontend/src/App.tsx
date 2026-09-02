import { useCallback, useEffect, useState } from "react";
import { Photo, deletePhoto, listPhotos, uploadPhoto } from "./api";
import { PhotoGrid } from "./components/PhotoGrid";
import { UploadZone } from "./components/UploadZone";

export default function App() {
  const [photos, setPhotos] = useState<Photo[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      const data = await listPhotos();
      setPhotos(data);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function handleUpload(files: File[]) {
    setUploading(true);
    setError(null);
    try {
      await Promise.all(files.map((f) => uploadPhoto(f)));
      await refresh();
    } catch (e) {
      setError(`Upload failed: ${e}`);
    } finally {
      setUploading(false);
    }
  }

  async function handleDelete(key: string) {
    setDeleting(key);
    setError(null);
    try {
      await deletePhoto(key);
      setPhotos((prev) => prev.filter((p) => p.key !== key));
    } catch (e) {
      setError(`Delete failed: ${e}`);
      setDeleting(null);
    } finally {
      setDeleting(null);
    }
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">Photo Gallery</h1>
        <p className="app-subtitle">
          Backed by <strong>S3</strong> via{" "}
          <a href="https://github.com/adamlock/cloudish" target="_blank" rel="noreferrer">
            Cloudish
          </a>
        </p>
      </header>

      <main className="app-main">
        <UploadZone onUpload={handleUpload} uploading={uploading} />

        {error && (
          <div className="error-banner" role="alert">
            {error}
          </div>
        )}

        {loading ? (
          <div className="loading">Loading gallery…</div>
        ) : (
          <PhotoGrid photos={photos} onDelete={handleDelete} deleting={deleting} />
        )}
      </main>
    </div>
  );
}
