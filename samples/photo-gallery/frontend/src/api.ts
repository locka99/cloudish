const BASE = "/api";

export interface Photo {
  key: string;
  /** Presigned S3 GET URL — safe to use directly as an <img src>. */
  url: string;
  size: number;
  last_modified: string;
}

/** Fetch the current list of photos with their presigned view URLs. */
export async function listPhotos(): Promise<Photo[]> {
  const res = await fetch(`${BASE}/photos`);
  if (!res.ok) throw new Error(`list failed: ${res.status}`);
  return res.json();
}

/**
 * Upload a file via the backend (backend → S3).
 * This avoids any CORS issue with cloudish.
 */
export async function uploadPhoto(file: File): Promise<{ key: string }> {
  const form = new FormData();
  form.append("file", file, file.name);

  const res = await fetch(`${BASE}/photos`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) throw new Error(`upload failed: ${res.status}`);
  return res.json();
}

/** Delete a photo by its S3 key (e.g. `550e8400-e29b-41d4-a716-446655440000.jpg`). */
export async function deletePhoto(key: string): Promise<void> {
  const res = await fetch(`${BASE}/photos/${encodeURIComponent(key)}`, {
    method: "DELETE",
  });
  if (!res.ok && res.status !== 204) throw new Error(`delete failed: ${res.status}`);
}
