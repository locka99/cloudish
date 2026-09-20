# tenant-demo

A multi-tenant notice-board application that demonstrates Cognito authentication,
tenant resolution via DynamoDB, and per-tenant data isolation.

## Features

- **Cognito authentication** — users log in with username/password; the backend
  validates credentials using the `USER_PASSWORD_AUTH` flow and returns a JWT
  access token.
- **Tenant resolution** — on login the backend looks up the user in a `user-config`
  DynamoDB table to find their `tenant_id`, then reads the tenant's display name
  and database connection string from `tenant-config`.
- **Per-tenant notice board** — each tenant has its own set of notices stored in a
  shared `notices` DynamoDB table partitioned by `tenant_id`.  Users can post new
  messages and see all messages posted by their colleagues.
- **Sign out** — invalidates the Cognito session via `GlobalSignOut` and clears
  the stored token.

## Default users

| Username | Password    | Tenant       |
|----------|-------------|--------------|
| `fred`   | `Password1!`| Acme Corp    |
| `bob`    | `Password1!`| Globex Corp  |

Both users and their tenant records are seeded automatically on backend startup.

## DynamoDB tables

| Table           | Partition key | Sort key    | Purpose                              |
|-----------------|---------------|-------------|--------------------------------------|
| `user-config`   | `username`    | —           | Maps a user to their `tenant_id`     |
| `tenant-config` | `tenant_id`   | —           | Tenant display name + DB connection  |
| `notices`       | `tenant_id`   | `notice_id` | Tenant-scoped notice board entries   |

## Prerequisites

- [Rust](https://rustup.rs/) (stable, 2021 edition)
- [Node.js](https://nodejs.org/) 18+
- Cloudish running on `http://localhost:4566`

## Running

**Terminal 1 — start Cloudish**

```bash
cd /path/to/cloudish
cargo run
```

**Terminal 2 — start the backend**

```bash
cd samples/tenant-demo/backend
cargo run
```

The backend binds to `http://localhost:3002`.  On first run it:

1. Creates (or finds) a Cognito user pool named `tenant-demo`.
2. Creates (or finds) an app client `tenant-demo-client`.
3. Creates users `fred` and `bob` with permanent passwords.
4. Creates the three DynamoDB tables (idempotent).
5. Seeds `user-config` and `tenant-config` with the default data above.

**Terminal 3 — start the frontend**

```bash
cd samples/tenant-demo/frontend
npm install
npm run dev
```

Open `http://localhost:5173` in your browser.

## API reference

All endpoints are served by the backend on port `3002`.  The Vite dev server
proxies `/api/*` there automatically.

### `POST /api/auth/login`

Authenticate a user.

**Request body**

```json
{ "username": "fred", "password": "Password1!" }
```

**Response `200`**

```json
{
  "access_token": "<jwt>",
  "username": "fred",
  "tenant_name": "Acme Corp"
}
```

**Response `401`** — bad credentials.

---

### `POST /api/auth/logout`

Invalidate the current session (best-effort `GlobalSignOut`).

**Headers** `Authorization: Bearer <token>`

**Response `204`**

---

### `GET /api/notices`

List all notices for the authenticated user's tenant, newest first.

**Headers** `Authorization: Bearer <token>`

**Response `200`**

```json
[
  {
    "id": "001726000000000_<uuid>",
    "author": "fred",
    "message": "All hands meeting Friday at 10 am.",
    "created_at": "2025-09-20T14:32:00+00:00"
  }
]
```

---

### `POST /api/notices`

Post a new notice.

**Headers** `Authorization: Bearer <token>`

**Request body**

```json
{ "message": "Reminder: submit timesheets by EOD." }
```

**Response `200`** — the created notice object (same shape as above).

## Project structure

```
tenant-demo/
├── README.md
├── backend/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs          # Axum server: Cognito auth, DynamoDB CRUD
└── frontend/
    ├── package.json
    ├── tsconfig.json
    ├── vite.config.ts        # Proxies /api → localhost:3002
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx           # Auth state, routing between login / board
        ├── App.css
        ├── api.ts            # Typed fetch wrappers
        └── components/
            ├── LoginForm.tsx  # Username/password form
            └── NoticeBoard.tsx # Notice list + post form + sign-out
```
