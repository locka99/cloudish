This project shall emulate the APIs for following AWS services

- S3
- DynamoDB
- Cognito
- AppConfig
- RDS
- SES
- SQS
- IAM (basics)
- SNS
- Lambda
- CloudWatch

For each of these the APIs will be backed by persistent storage that will hold data under a subdirectory of `data/`
except for RDS which will map to a Postgres database of my choosing.

Code will be structured such that new services can be implemented. Services will be tested with unit tests and integration tests.

Services shall be implemented with tokio.

Services will test basic AWS credentials to apply IAM policy to the session.

## Architecture Decisions

1. **SigV4 strictness** — parse the `Authorization` header to extract the access key and credential scope, but do not verify the HMAC-SHA256 signature. Any well-formed signature is accepted.

2. **S3 routing** — support both path-based (`http://localhost:4566/bucket/key`) and host-based (`http://bucket.localhost:4566/key`). Object metadata (Content-Type, ETag, user metadata, etc.) is stored in a sibling `.metadata` JSON file alongside the object data.

3. **RDS** — proxy the Postgres wire protocol to a real Postgres instance. Proxy target and all other runtime settings are configured via `cloudish.yaml` in the working directory.

4. **Cognito JWT signing** — issue real RS256-signed JWTs. Generate an RSA keypair on first run and persist it under `data/cognito/`.

5. **Ports** — single port 4566 for all services. Callers configure each AWS SDK client with `endpoint_url = "http://localhost:4566"`. (Same model as LocalStack v2+.)

6. **Default credentials** — access key `test`, secret key `test`. Pre-seeded so callers work before IAM is configured.

7. **AWS account ID** — `000000000000` used in all ARNs.

8. **Default region** — `eu-west-1`.

9. **Config file** — `cloudish.yaml` loaded from the working directory first, then `~/.cloudish/config.yaml` as a fallback.

10. **S3 versioning** — simple suffix scheme: `{key}_0`, `{key}_1`, etc. Metadata file tracks version history.

11. **S3 presigned URLs** — supported for GET and PUT.

12. **S3 multipart upload** — supported.

13. **DynamoDB GSI/LSI** — not supported (deferred; too complex for now).

14. **DynamoDB Streams** — supported.

15. **DynamoDB TTL** — supported; items with an expired TTL attribute are automatically expired.

16. **SQS FIFO queues** — supported (`.fifo` queue names with deduplication).

17. **SQS dead-letter queues** — supported.

18. **SQS long polling** — supported; `ReceiveMessage` holds the connection open until a message arrives or the wait timeout expires.

19. **SES sent mail storage** — sent emails saved to `data/ses/sent/{message_id}.json` as JSON (source, destinations, subject, body, timestamp). `SendRawEmail` stores the base64-encoded MIME payload as-is.

20. **SES SMTP interface** — not supported; HTTP API only.

20a. **SES wire format** — Query protocol (`POST /` with URL-encoded body and `Action=` param). SigV4 credential scope `service=ses`. Identities are auto-verified (no email challenge). Supported: SendEmail, SendRawEmail, VerifyEmailIdentity, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics.

21. **Cognito auth flows** — `USER_PASSWORD_AUTH` and `REFRESH_TOKEN_AUTH` only. SRP not supported.

22. **Cognito MFA** — not supported.

23. **SNS wire format** — Query protocol (`POST /` with URL-encoded body and `Action=` parameter). Routed via SigV4 credential scope `service=sns`. Currently a stub; all operations return `NotImplemented`.

24. **Lambda wire format** — REST/JSON under `/2015-03-31/`. Routed by path. Currently a stub; all operations return `NotImplemented`.

25. **AppConfig wire format** — REST/JSON. Management plane uses `aws-sdk-appconfig` (SigV4 credential scope `service=appconfig`). Data plane uses `aws-sdk-appconfigdata` (scope `service=appconfigdata`). Routes use real AWS API paths (`/applications`, `/deploymentstrategies`, `/configurationsessions`, `/configuration`) — axum's literal-route priority ensures these beat S3's `/{bucket}` wildcard. Deployments complete immediately as `COMPLETE`. GetLatestConfiguration receives the token via `configuration_token` query parameter. Response headers for hosted config versions use `Version-Number`, `Application-Id`, `Configuration-Profile-Id` (SDK-expected casing).

## Testing Rules

- Every integration test **must** clean up all resources it creates (buckets, queues, tables, etc.), even if the test fails or panics.
- Test data is stored under `data/test_{port}/` (isolated from `data/`). This directory is wiped at the start of each test run.
- Use a drop-guard to ensure cleanup runs on panic. Never rely solely on an explicit `cleanup()` call at the end of a test body.
- Each test must use a unique resource name (e.g. UUID-based bucket/queue/table name) to avoid cross-test interference when tests run in parallel.
- **Drop guard cleanup must use raw TCP, not the AWS SDK client.** An SDK client created in one tokio runtime (e.g. `#[tokio::test]`) cannot be reused from a `Drop` impl that creates a new runtime — the connection pool background tasks aren't running in the new runtime, causing the call to hang indefinitely before reaching the server. Use `std::net::TcpStream` to send a raw HTTP request instead. `reqwest::blocking` has the same problem when called from within a tokio context.