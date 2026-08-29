# Cloudish

A local AWS emulator written in Rust. Provides HTTP API-compatible endpoints for common AWS services on a single port, making it easy to develop and test AWS-backed applications without a real AWS account.

## Services

| Service | Status |
|---------|--------|
| S3 | Implemented |
| DynamoDB | Implemented |
| Cognito | Implemented |
| AppConfig | Stub |
| RDS | Proxy (Postgres wire protocol) |
| SES | Stub |
| SQS | Stub |
| IAM | Implemented |
| CloudWatch | Not started |

## Requirements

- Rust 1.81+ (for building from source)
- PostgreSQL 12+ (only required if using timplehe RDS proxy feature)

## Building

```bash
cargo build --release
```

The binary will be at `target/release/cloudish`.

## Running

```bash
# From source
cargo run

# Or run the compiled binary
./target/release/cloudish
```

Cloudish listens on `0.0.0.0:4566` by default and loads configuration from `cloudish.yaml` in the current directory. If no local config is found it falls back to `~/.cloudish/config.yaml`.

## Configuration

All settings are controlled via `cloudish.yaml`. Copy and adjust the example below:

```yaml
server:
  host: "0.0.0.0"
  port: 4566
  region: "eu-west-1"
  account_id: "000000000000"

storage:
  data_dir: "data"             # Root directory for persisted service data

credentials:
  - access_key: "test"
    secret_key: "test"
  # Add more credentials as needed

rds:
  proxy_dsn: "postgresql://postgres:postgres@localhost:5432/cloudish"
  proxy_port: 5433             # Cloudish listens on this port for Postgres connections

s3:
  max_object_size: 5368709120  # 5 GiB
  max_presign_expiry: 604800   # 7 days (seconds)

dynamodb:
  ttl_sweep_interval: 60       # How often to expire TTL items (seconds)

sqs:
  max_wait_time: 20            # Long-poll max wait (seconds; AWS max is 20)
  default_retention: 345600    # 4 days (seconds)

cognito:
  access_token_ttl: 3600       # 1 hour (seconds)
  refresh_token_ttl: 2592000   # 30 days (seconds)

logging:
  level: "info"                # error | warn | info | debug | trace
```

### Configuration resolution order

1. `./cloudish.yaml` (current working directory)
2. `~/.cloudish/config.yaml`

### Logging

Log verbosity can also be set with the `RUST_LOG` environment variable, which takes precedence over the config file:

```bash
RUST_LOG=debug cargo run
RUST_LOG=cloudish=trace,info cargo run
```

## Connecting AWS SDKs

Configure any AWS SDK client to point at `http://localhost:4566` and use the default credentials:

| Setting | Value |
|---------|-------|
| Endpoint URL | `http://localhost:4566` |
| Access key | `test` |
| Secret key | `test` |
| Region | `eu-west-1` |
| Account ID | `000000000000` |

**Python (boto3):**
```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="eu-west-1",
)
```

**AWS CLI:**
```bash
aws --endpoint-url http://localhost:4566 s3 ls
```

**AWS CLI profile (`~/.aws/config` and `~/.aws/credentials`):**
```ini
# ~/.aws/config
[profile cloudish]
endpoint_url = http://localhost:4566
region = eu-west-1

# ~/.aws/credentials
[cloudish]
aws_access_key_id = test
aws_secret_access_key = test
```

## Data persistence

All service data is written under the `storage.data_dir` directory (default: `./data`), organised by service:

```
data/
├── s3/          # Buckets and objects (metadata stored in .metadata sibling files)
├── dynamodb/    # Tables and items
├── cognito/     # User pools and RSA keypair for JWT signing
├── appconfig/   # Application configurations
├── ses/sent/    # Sent emails as JSON
├── sqs/         # Queues and messages
└── iam/         # IAM data
```

Delete the `data/` directory to reset all state.

## S3-specific notes

Both URL styles are supported:

- Path-style: `http://localhost:4566/bucket-name/object-key`
- Virtual-hosted-style: `http://bucket-name.localhost:4566/object-key`

Features: multipart upload, presigned GET/PUT URLs, object versioning, object metadata (Content-Type, ETag, user metadata).

## DynamoDB-specific notes

All requests POST to `http://localhost:4566/dynamodb/` with the `X-Amz-Target` header, which is exactly what the AWS SDK sends when you configure a custom endpoint.

**Supported operations:** CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, PutItem, GetItem, DeleteItem, UpdateItem, BatchGetItem, BatchWriteItem, TransactGetItems, TransactWriteItems, Query, Scan, UpdateTimeToLive, DescribeTimeToLive, ListStreams, DescribeStream, GetShardIterator, GetRecords.

**Expressions:** FilterExpression, ConditionExpression, KeyConditionExpression, UpdateExpression (SET/REMOVE/ADD/DELETE), and ProjectionExpression are all supported. Expression attribute names (`#name`) and values (`:val`) are substituted correctly.

**Streams:** Enable with `StreamSpecification: {StreamEnabled: true, StreamViewType: NEW_AND_OLD_IMAGES}` at table creation or via UpdateTable. Records are available via the standard Streams API (GetShardIterator / GetRecords). Each table has a single shard.

**TTL:** Enable per-table with UpdateTimeToLive. Items whose TTL attribute (an N-type Unix timestamp) has elapsed are automatically deleted by a background sweep that runs every `dynamodb.ttl_sweep_interval` seconds (default: 60).

**Not supported:** GSI and LSI (deferred).

## Cognito-specific notes

All requests POST to `http://localhost:4566/` with the `X-Amz-Target` header (e.g. `AmazonCognitoIdentityProvider.InitiateAuth`), which is exactly what the AWS SDK sends.

**Supported operations:** CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, AdminCreateUser, AdminDeleteUser, AdminGetUser, AdminSetUserPassword, AdminUpdateUserAttributes, ListUsers, AdminInitiateAuth, InitiateAuth, AdminRespondToAuthChallenge, RespondToAuthChallenge, SignUp, ConfirmSignUp, GetUser.

**Auth flows:** `USER_PASSWORD_AUTH` and `REFRESH_TOKEN_AUTH`. SRP is not supported.

**Tokens:** Real RS256-signed JWTs. An RSA-2048 keypair is generated on first run and persisted under `data/cognito/`. Access tokens expire after 1 hour; refresh tokens after 30 days (configured via `cognito.access_token_ttl` / `cognito.refresh_token_ttl`).

**JWKS:** The public key is served at `GET /cognito/{pool_id}/.well-known/jwks.json` so token verification libraries can fetch it.

**Not supported:** MFA, SRP auth, advanced security features.

## IAM-specific notes

IAM uses a different wire format from the JSON-based services. Requests are `POST /` with `Content-Type: application/x-www-form-urlencoded` and an `Action=` parameter in the body (e.g. `Action=CreateUser&UserName=alice`). Responses are XML.

**Supported operations:**

| Category | Operations |
|----------|-----------|
| Users | CreateUser, DeleteUser, GetUser, UpdateUser, ListUsers, TagUser, UntagUser, ListUserTags |
| Access keys | CreateAccessKey, DeleteAccessKey, ListAccessKeys, UpdateAccessKey |
| Inline policies | PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies, PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies |
| Managed policies | CreatePolicy, DeletePolicy, GetPolicy, ListPolicies, GetPolicyVersion |
| Attach/detach | AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies, AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies |
| Roles | CreateRole, DeleteRole, GetRole, ListRoles, TagRole, UntagRole, ListRoleTags |
| STS | GetCallerIdentity |

**STS:** `GetCallerIdentity` is handled by the IAM dispatcher (same form-encoded XML format). The SigV4 credential scope `service=sts` routes it automatically when using the AWS SDK with a custom endpoint.

## RDS proxy

When the RDS proxy is enabled, Cloudish accepts Postgres wire-protocol connections on `rds.proxy_port` (default: `5433`) and forwards them to the database at `rds.proxy_dsn`. The management-plane API (CreateDBInstance, etc.) returns `NotImplemented`.

Connect your application to `localhost:5433` as if it were a normal Postgres server.

## Running with Docker

A Dockerfile is included that bundles Cloudish with PostgreSQL 16 into a single image, suitable for CI or local development.

**Build the image:**
```bash
docker build -t cloudish:latest .
```

**Run:**
```bash
docker run -d \
  -v "$(pwd)/data:/data" \
  -p 4566:4566 \
  -p 5433:5433 \
  --name cloudish \
  cloudish:latest
```

| Port | Purpose                |
|------|------------------------|
| 4566 | AWS HTTP API           |
| 5433 | RDS Postgres proxy     |

The container starts its own PostgreSQL instance and waits for it to be ready before starting Cloudish. Data is persisted to the `/data` volume.

To use a custom config inside the container, mount it at `/etc/cloudish/cloudish.yaml`:
```bash
docker run -d \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/my-config.yaml:/etc/cloudish/cloudish.yaml" \
  -p 4566:4566 \
  cloudish:latest
```

## Running tests

Integration tests start an isolated Cloudish server on a random port and clean up after themselves.

```bash
# Run all integration tests
cargo test

# Run tests for a specific service
cargo test --test s3
cargo test --test dynamodb
cargo test --test cognito
cargo test --test iam

# Show log output
cargo test --test cognito -- --nocapture

# Run a specific test
cargo test --test dynamodb test_streams -- --nocapture
```

Test data is written to `data/test_{port}/` and wiped at the start of each run, so it never interferes with your local `data/` directory.
