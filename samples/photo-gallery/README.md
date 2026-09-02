# Photo Gallery

A minimal full-stack photo gallery that stores images in S3-compatible storage
via [Cloudish](../../README.md).

```
Browser (React + Vite)
    │  upload (multipart POST)
    │  delete  (DELETE)
    ▼
Rust backend  ──── AWS SDK ────▶  Cloudish  (S3 + IAM)
    │
    │  presigned GET URLs
    ▼
Browser renders <img src="http://localhost:4566/...?X-Amz-Signature=...">
```

## Features

- **Upload** — drag & drop or click to select one or more images; the browser
  POSTs them to the Rust backend, which writes them to S3.
- **Gallery grid** — photos are listed via `ListObjectsV2`; each is displayed
  using a presigned S3 GET URL so the browser fetches images directly from
  Cloudish without proxying through the backend.
- **Delete** — removes the object from S3 and updates the grid instantly.
- **IAM role** — on startup the backend creates an IAM policy scoped to the
  `photo-gallery` bucket and attaches it to a service role, demonstrating the
  pattern used in production with EC2 instance profiles or ECS task roles.

## Prerequisites

| Dependency | Purpose |
|-----------|---------|
| Cloudish running on `localhost:4566` | S3 + IAM emulation |
| Rust + Cargo | Backend |
| Node 18+ + npm | Frontend |

## Running

### 1. Start Cloudish

From the repo root:
```bash
cargo run
```

### 2. Start the backend

```bash
cd samples/photo-gallery/backend
cargo run
```

The backend starts on **http://localhost:3001**.  On first start it:
- Creates the IAM policy `photo-gallery-s3-policy`
- Creates the IAM role `photo-gallery-role` and attaches the policy
- Creates the S3 bucket `photo-gallery`

### 3. Start the frontend

```bash
cd samples/photo-gallery/frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser.

## API reference

The Vite dev server proxies `/api` requests to `localhost:3001`, so the
browser always stays on the same origin.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/photos` | List all photos; returns presigned GET URLs |
| `POST` | `/api/photos` | Upload via multipart form; stores file in S3 |
| `DELETE` | `/api/photos/:key` | Delete a photo by its S3 key |
| `POST` | `/api/photos/presign-upload` | Get a presigned PUT URL for direct-to-S3 upload |

### Presigned upload alternative

The backend also exposes `POST /api/photos/presign-upload`, which returns a
short-lived presigned **PUT** URL.  The client can then upload the file body
directly to Cloudish, bypassing the backend for the data transfer entirely:

```ts
// 1. Get a presigned PUT URL from the backend
const { url, key } = await fetch("/api/photos/presign-upload", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ filename: file.name, content_type: file.type }),
}).then(r => r.json());

// 2. PUT the file directly to Cloudish
await fetch(url, {
  method: "PUT",
  headers: { "Content-Type": file.type },
  body: file,
});
```

> **Note:** Direct browser-to-S3 PUT requires Cloudish to respond with CORS
> headers (`Access-Control-Allow-Origin`, etc.).  Cloudish does not yet
> implement S3 CORS configuration, so the multipart proxy path
> (`POST /api/photos`) is used by the React app by default.

## Project structure

```
samples/photo-gallery/
├── README.md
├── backend/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs          # axum server — IAM setup, S3 ops, presigned URLs
└── frontend/
    ├── package.json
    ├── tsconfig.json
    ├── vite.config.ts        # proxies /api → localhost:3001
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx           # root component — state, upload/delete logic
        ├── App.css           # styles
        ├── api.ts            # typed fetch wrappers
        └── components/
            ├── UploadZone.tsx  # drag-and-drop upload area
            └── PhotoGrid.tsx   # responsive grid of photo cards
```
