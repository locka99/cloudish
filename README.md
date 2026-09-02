# Cloudish

Welcome to Cloudish. This is a mostly AI generated emulation of AWS services written in Rust. It would be
similar in purpose to LocalStack but open sourced under MIT license.

It provides HTTP API-compatible endpoints for common AWS services on a single port, making it easy to develop and test AWS-backed applications without a real AWS account.

## Services

| Service | Status |
|---------|--------|
| S3 | Implemented |
| DynamoDB | Implemented |
| Cognito | Implemented |
| AppConfig | Implemented |
| RDS | Proxy (Postgres wire protocol) |
| SES | Implemented |
| SQS | Implemented |
| IAM | Implemented |
| SNS | Implemented |
| Lambda | Implemented |
| IoT | Implemented (control plane; MQTT not supported) |
| CloudWatch | Implemented (metrics, alarms, logs) |

## Requirements

- Rust 1.81+ (for building from source)
- PostgreSQL 12+ (only required if using the RDS proxy feature)
- Docker (only required if using the Lambda executor)

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

# Point at a specific config file
./target/release/cloudish --config /path/to/my-config.yaml
cargo run -- --config /path/to/my-config.yaml
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

lambda:
  executor: "docker"           # docker (default) or subprocess
  network: "bridge"            # Docker network for function containers
  default_timeout: 30          # Function timeout in seconds

logging:
  level: "info"                # error | warn | info | debug | trace
```

### Configuration resolution order

1. `--config <FILE>` flag (explicit path, skips all fallbacks)
2. `./cloudish.yaml` (current working directory)
3. `~/.cloudish/config.yaml`

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
├── iam/         # IAM data
├── sns/         # Topics and subscriptions
├── lambda/      # Functions and event source mappings
├── iot/         # Things, certificates, policies, and attachments
├── cloudwatch/  # Metrics (namespaces/metric_name.json) and alarms
└── cloudwatch_logs/  # Log groups, streams, and events
```

Delete the `data/` directory to reset all state.

## S3-specific notes

Both URL styles are supported:

- Path-style: `http://localhost:4566/bucket-name/object-key`
- Virtual-hosted-style: `http://bucket-name.localhost:4566/object-key`

Features: multipart upload, presigned GET/PUT URLs, object versioning, object metadata (Content-Type, ETag, user metadata).

### Example usage (AWS CLI)

First, configure a named profile so you don't have to repeat flags on every command:

```bash
aws configure --profile cloudish
# AWS Access Key ID:     test
# AWS Secret Access Key: test
# Default region name:   eu-west-1
# Default output format: json
```

Or set the profile's endpoint URL directly in `~/.aws/config`:

```ini
[profile cloudish]
aws_access_key_id = test
aws_secret_access_key = test
region = eu-west-1
endpoint_url = http://localhost:4566
```

Then use the `--profile cloudish` flag (or set `AWS_PROFILE=cloudish` in your shell):

```bash
export AWS_PROFILE=cloudish

# Create a bucket
aws s3 mb s3://my-bucket

# Upload a file
aws s3 cp ./hello.txt s3://my-bucket/hello.txt

# List objects in a bucket
aws s3 ls s3://my-bucket

# Download a file
aws s3 cp s3://my-bucket/hello.txt ./hello-downloaded.t
# Sync a local directory to a bucket
aws s3 sync ./my-dir s3://my-bucket/my-dir/

# Generate a presigned GET URL (valid for 1 hour)
aws s3 presign s3://my-bucket/hello.txt --expires-in 3600

# Delete an object
aws s3 rm s3://my-bucket/hello.txt

# Delete a bucket and all its contents
aws s3 rb s3://my-bucket --force
```

If you prefer environment variables over a named profile:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=eu-west-1

aws --endpoint-url http://localhost:4566 s3 ls
```

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

## SQS-specific notes

SQS uses the AWS JSON protocol (`application/x-amz-json-1.0`) with an `X-Amz-Target` header (e.g. `AmazonSQS.SendMessage`), unlike IAM which uses form-encoded/XML.

Queue URLs follow the pattern `http://localhost:4566/000000000000/{queue_name}`. Queue-specific operations (SendMessage, ReceiveMessage, etc.) are sent by the SDK directly to the queue URL path; service-level operations (CreateQueue, ListQueues, GetQueueUrl) go to `POST /`.

**Supported operations:** CreateQueue, DeleteQueue, GetQueueUrl, ListQueues, ListDeadLetterSourceQueues, GetQueueAttributes, SetQueueAttributes, SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage, DeleteMessageBatch, ChangeMessageVisibility, ChangeMessageVisibilityBatch, PurgeQueue.

**FIFO queues:** Create with a `.fifo` name suffix and set `FifoQueue=true`. Supports `MessageGroupId` (ordering) and `MessageDeduplicationId` (5-minute dedup window). Also supports `ContentBasedDeduplication`.

**Dead-letter queues:** Set `RedrivePolicy` with `deadLetterTargetArn` and `maxReceiveCount` on the source queue. Messages exceeding the receive count are automatically moved to the DLQ during `ReceiveMessage`.

**Long polling:** `ReceiveMessage` with `WaitTimeSeconds > 0` holds the connection open until a message arrives or the timeout expires (max 20 s, configurable via `sqs.max_wait_time`). Uses `tokio::sync::Notify` for efficient wakeup rather than busy-polling.

## IAM-specific notes

IAM uses a different wire format from the JSON-based services. Requests are `POST /` with `Content-Type: application/x-www-form-urlencoded` and an `Action=` parameter in the body (e.g. `Action=CreateUser&UserName=alice`). Responses are XML.

**Supported operations:**

| Category | Operations |
|----------|------------|
| Users | CreateUser, DeleteUser, GetUser, UpdateUser, ListUsers, TagUser, UntagUser, ListUserTags |
| Access keys | CreateAccessKey, DeleteAccessKey, ListAccessKeys, UpdateAccessKey |
| Inline policies | PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies, PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies |
| Managed policies | CreatePolicy, DeletePolicy, GetPolicy, ListPolicies, GetPolicyVersion |
| Attach/detach | AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies, AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies |
| Roles | CreateRole, DeleteRole, GetRole, ListRoles, TagRole, UntagRole, ListRoleTags |
| STS | GetCallerIdentity |

**STS:** `GetCallerIdentity` is handled by the IAM dispatcher (same form-encoded XML format). The SigV4 credential scope `service=sts` routes it automatically when using the AWS SDK with a custom endpoint.

## SES-specific notes

SES uses the AWS Query protocol: `POST /` with `Content-Type: application/x-www-form-urlencoded` and an `Action=` parameter in the body. The SigV4 credential scope `service=ses` routes it automatically when using the AWS SDK with a custom endpoint.

**Supported operations:** SendEmail, SendRawEmail, VerifyEmailIdentity, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics.

**Identity verification:** All identities (email addresses and domains) are immediately marked as verified — there is no real email challenge.

**Sent mail storage:** Every sent email is saved as a JSON file under `data/ses/sent/{message_id}.json` containing the source, destinations, subject, body, and timestamp. For `SendRawEmail`, the base64-encoded MIME payload is stored as-is.

**Quota:** `GetSendQuota` returns static values (50 000 messages/day, 14 messages/second).

## AppConfig-specific notes

AppConfig uses a REST/JSON API (no `X-Amz-Target` header). The management plane (`aws-sdk-appconfig`) and the data plane (`aws-sdk-appconfigdata`) both route to the same port.

**Management plane supported operations:**

| Category | Operations |
|----------|------------|
| Applications | CreateApplication, GetApplication, ListApplications, UpdateApplication, DeleteApplication |
| Environments | CreateEnvironment, GetEnvironment, ListEnvironments, UpdateEnvironment, DeleteEnvironment |
| Configuration Profiles | CreateConfigurationProfile, GetConfigurationProfile, ListConfigurationProfiles, UpdateConfigurationProfile, DeleteConfigurationProfile |
| Hosted Config Versions | CreateHostedConfigurationVersion, GetHostedConfigurationVersion, ListHostedConfigurationVersions, DeleteHostedConfigurationVersion |
| Deployment Strategies | CreateDeploymentStrategy, GetDeploymentStrategy, ListDeploymentStrategies |
| Deployments | StartDeployment, GetDeployment, ListDeployments, StopDeployment |

**Data plane supported operations:** StartConfigurationSession, GetLatestConfiguration.

**Built-in deployment strategies:** `AppConfig.AllAtOnce`, `AppConfig.Linear50PercentEvery30Seconds`, `AppConfig.Canary10Percent20Minutes`.

**Deployments:** Deployments complete immediately as `COMPLETE`. The deployed configuration version is tracked per-environment.

**Configuration sessions:** `StartConfigurationSession` returns an `InitialConfigurationToken`. `GetLatestConfiguration` (via `configuration_token` query parameter) returns the configuration content on the first call; subsequent calls with no new deployment return an empty body (304-equivalent).

## SNS-specific notes

SNS uses the AWS Query protocol: `POST /` with `Content-Type: application/x-www-form-urlencoded` and an `Action=` parameter in the body (e.g. `Action=CreateTopic&Name=my-topic`). The SigV4 credential scope `service=sns` routes it automatically when using the AWS SDK with a custom endpoint.

**Supported operations:**

| Operation | Notes |
|-----------|-------|
| CreateTopic | Idempotent — returns same ARN for duplicate names |
| DeleteTopic | Removes topic and all its subscriptions |
| ListTopics | Paginated (NextToken supported) |
| GetTopicAttributes | Returns TopicArn, DisplayName, SubscriptionsConfirmed, etc. |
| SetTopicAttributes | DisplayName, DeliveryPolicy |
| Subscribe | Protocols: `sqs`, `http`, `https`, `email`, `email-json`. Auto-confirmed. Attributes (RawMessageDelivery, FilterPolicy) accepted at subscribe time |
| Unsubscribe | |
| ConfirmSubscription | No-op (subscriptions are auto-confirmed) |
| ListSubscriptions | |
| ListSubscriptionsByTopic | |
| GetSubscriptionAttributes | |
| SetSubscriptionAttributes | RawMessageDelivery, FilterPolicy |
| Publish | Delivers to all confirmed subscriptions |
| PublishBatch | Up to 10 messages per batch |
| TagResource / UntagResource / ListTagsForResource | |
| CreatePlatformApplication / DeletePlatformApplication / ListPlatformApplications | Stub (returns empty list) |

**Delivery:**
- **SQS** — enqueued directly into the target queue (no HTTP round-trip). Wrapped in SNS JSON envelope unless `RawMessageDelivery=true`.
- **HTTP/HTTPS** — fire-and-forget POST to the endpoint URL. Failures are logged but not retried.
- **email / email-json** — logged only; no real email is sent.

## Lambda-specific notes

Lambda uses a REST/JSON API over paths rooted at `/2015-03-31/`. The SigV4 credential scope `service=lambda` identifies requests.

### Execution model

Functions are executed via Docker using the AWS Lambda Runtime Interface Emulator (RIE), which is built into all official `public.ecr.aws/lambda/*` base images.

On `Invoke`:
1. Cloudish looks up the function config and checks for a running container in its in-memory map.
2. If none, it runs `docker run --rm -d -p 0:8080 {image_uri}` and waits up to 10 s for the RIE to become ready.
3. The container is kept alive and reused for subsequent invocations of the same function.
4. The event JSON is POSTed to `http://127.0.0.1:{host_port}/2015-03-31/functions/function/invocations`.
5. The response body and any `x-amz-function-error` header are returned to the caller.

**Only image-based functions are supported** (`PackageType=Image` with an `ImageUri`). Zip-based functions return `501`.

### Example

```bash
# Create an image-based function
aws lambda create-function \
  --function-name my-fn \
  --package-type Image \
  --code ImageUri=public.ecr.aws/lambda/python:3.12 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --endpoint-url http://localhost:4566

# Invoke it
aws lambda invoke \
  --function-name my-fn \
  --payload '{"key":"value"}' \
  response.json \
  --endpoint-url http://localhost:4566

cat response.json
```

### Event source mappings

Creating an event source mapping spawns a background task that polls the source and invokes the function automatically:

- **SQS** — receives up to `BatchSize` messages (default 10), invokes the function with a `{"Records":[...]}` event. On success the batch is deleted; on error the messages remain visible for retry or DLQ processing.
- **DynamoDB Streams** — polls the stream storage directly with a shard iterator, delivers records as a `{"Records":[...]}` event, advances the iterator on each poll. Polling interval is 500 ms.

```bash
# Wire an SQS queue to a Lambda function
aws lambda create-event-source-mapping \
  --function-name my-fn \
  --event-source-arn arn:aws:sqs:eu-west-1:000000000000:my-queue \
  --batch-size 5 \
  --endpoint-url http://localhost:4566
```

ESM tasks are started at server boot for all persisted mappings with `State=Enabled`.

### Supported operations

| Method | Path | Operation | Notes |
|--------|------|-----------|-------|
| POST | `/2015-03-31/functions` | CreateFunction | |
| GET | `/2015-03-31/functions` | ListFunctions | |
| GET | `/2015-03-31/functions/{name}` | GetFunction | |
| DELETE | `/2015-03-31/functions/{name}` | DeleteFunction | Stops container |
| PUT | `/2015-03-31/functions/{name}/code` | UpdateFunctionCode | Stops & restarts container |
| GET | `/2015-03-31/functions/{name}/configuration` | GetFunctionConfiguration | |
| PUT | `/2015-03-31/functions/{name}/configuration` | UpdateFunctionConfiguration | |
| POST | `/2015-03-31/functions/{name}/invocations` | Invoke | Docker/RIE; Image only |
| GET/POST | `/2015-03-31/functions/{name}/aliases` | ListAliases / CreateAlias | |
| GET/PUT/DELETE | `/2015-03-31/functions/{name}/aliases/{alias}` | GetAlias / UpdateAlias / DeleteAlias | |
| GET/POST | `/2015-03-31/functions/{name}/policy` | GetPolicy / AddPermission | |
| DELETE | `/2015-03-31/functions/{name}/policy/{sid}` | RemovePermission | |
| GET/POST | `/2015-03-31/event-source-mappings` | ListEventSourceMappings / CreateEventSourceMapping | |
| GET/PUT/DELETE | `/2015-03-31/event-source-mappings/{uuid}` | GetEventSourceMapping / UpdateEventSourceMapping / DeleteEventSourceMapping | |
| GET | `/2015-03-31/layers` | ListLayers | Returns empty list |
| GET/POST | `/2015-03-31/layers/{name}/versions` | ListLayerVersions / PublishLayerVersion | Stub |
| GET/DELETE | `/2015-03-31/layers/{name}/versions/{version}` | GetLayerVersion / DeleteLayerVersion | Stub |

## IoT-specific notes

IoT uses a REST/JSON API. The SigV4 credential scope `service=iot` routes requests automatically when using the AWS SDK with a custom endpoint.

**Supported operations:**

| Category | Operations |
|----------|-----------|
| Things | CreateThing, DescribeThing, ListThings, DeleteThing, UpdateThing |
| Thing Types | CreateThingType, DescribeThingType, ListThingTypes, DeleteThingType |
| Certificates | CreateKeysAndCertificate, DescribeCertificate, ListCertificates, DeleteCertificate, UpdateCertificate |
| Policies | CreatePolicy, GetPolicy, ListPolicies, DeletePolicy, CreatePolicyVersion, GetPolicyVersion, ListPolicyVersions, DeletePolicyVersion, SetDefaultPolicyVersion |
| Attach/Detach | AttachPolicy, DetachPolicy, ListAttachedPolicies, AttachThingPrincipal, DetachThingPrincipal, ListThingPrincipals, ListPrincipalThings |
| Endpoint | GetEndpoint |

**Certificates:** `CreateKeysAndCertificate` generates a real ECDSA P-256 self-signed X.509 certificate, returning the certificate PEM, public key PEM, and private key PEM. The private key is only available at creation time. The certificate ID is the lowercase hex SHA-256 of the certificate PEM (64 characters), matching the AWS format.

**MQTT:** The data plane (MQTT broker) is **not implemented**. `GetEndpoint` returns `localhost:8883` as a placeholder. Device connectivity can be added later without changing the control plane.

**Example (AWS CLI):**
```bash
# Create a thing
aws iot create-thing --thing-name my-sensor --endpoint-url http://localhost:4566

# Create a certificate and keys
aws iot create-keys-and-certificate --set-as-active --endpoint-url http://localhost:4566

# Create a policy
aws iot create-policy \
  --policy-name my-policy \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}' \
  --endpoint-url http://localhost:4566

# Attach policy to certificate
aws iot attach-policy \
  --policy-name my-policy \
  --target arn:aws:iot:eu-west-1:000000000000:cert/<cert-id> \
  --endpoint-url http://localhost:4566
```

## CloudWatch-specific notes

CloudWatch is split into two services with different wire formats:

### CloudWatch (metrics and alarms)

Uses the **Smithy RPCv2-CBOR** protocol (used by `aws-sdk-cloudwatch` v1). Requests go to `/service/GraniteServiceVersion20100801/operation/{OperationName}` with CBOR-encoded bodies. SigV4 credential scope `service=monitoring`.

**Supported operations:**

| Category | Operations |
|----------|-----------|
| Metrics | PutMetricData, ListMetrics, GetMetricStatistics |
| Alarms | PutMetricAlarm, DescribeAlarms, SetAlarmState, DeleteAlarms |

Metric data points are stored per (Namespace, MetricName) under `data/cloudwatch/metrics/`. `GetMetricStatistics` returns a single aggregated datapoint over the requested time range. Alarms are created with initial state `INSUFFICIENT_DATA`.

### CloudWatch Logs

Uses the **JSON API** with `X-Amz-Target: Logs_20140328.{Operation}` header. SigV4 credential scope `service=logs`.

**Supported operations:** CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream, DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, FilterLogEvents.

`PutLogEvents` auto-creates the log group and stream if they don't exist. `FilterLogEvents` supports case-sensitive substring matching on the message field (full CloudWatch filter syntax not supported). Storage under `data/cloudwatch_logs/`.

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
cargo test --test sqs
cargo test --test ses
cargo test --test appconfig
cargo test --test sns
cargo test --test iot
cargo test --test cloudwatch
cargo test --test cloudwatch_logs

# Show log output
cargo test --test cognito -- --nocapture

# Run a specific test
cargo test --test dynamodb test_streams -- --nocapture
```

Test data is written to `data/test_{port}/` and wiped at the start of each run, so it never interferes with your local `data/` directory.
