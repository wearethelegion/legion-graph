# kgrag-auth

Centralized authentication and authorization service for the KGRAG stack.

## What it does

The kgrag-auth service manages all user authentication, session management, role-based access control (RBAC), and multi-tenant company/project hierarchies. It issues JWT access and refresh tokens validated by all other KGRAG services. It also provides email verification, TOTP 2FA, OAuth2 provider linking (Google/GitHub), API token generation, and user account deletion with cascade cleanup across Qdrant and Neo4j.

## Where it lives

- **Dockerfile**: `Dockerfile.auth`
- **Source folder**: `auth/`
- **Built image**: `kgrag-auth:latest`
- **Internal port**: 8001 (HTTP REST API)
- **Published port**: 8001 → 8001 (host access via `http://localhost:8001`)

## Inputs

### HTTP routes served

- `POST /register` — Register new user; sends email verification code (or auto-activates if `SKIP_EMAIL_VERIFICATION=true`)
- `POST /login` — Password authentication; returns JWT tokens OR 2FA challenge token if TOTP enabled
- `POST /login/2fa` — Complete 2FA login with TOTP code or backup code
- `POST /refresh` — Refresh access token using refresh token
- `POST /logout` — Revoke tokens (individual or all sessions)
- `POST /verify-email` — Verify email with 6-digit code
- `POST /resend-verification` — Resend verification code
- `GET /me` — Get current user profile
- `PATCH /me` — Update user profile (name, avatar, timezone, etc.)
- `GET /me/deletion/preview` — Preview cascade deletion impact
- `DELETE /me` — Delete user account (phased: session invalidation → Qdrant/Neo4j cleanup → Postgres cascade)
- `GET /me/deletion/status/{task_id}` — Poll deletion task progress
- `GET /2fa/setup` — Generate TOTP QR code and secret
- `POST /2fa/verify-setup` — Verify TOTP code and enable 2FA
- `POST /2fa/disable` — Disable 2FA with password + TOTP code
- `POST /2fa/regenerate-backup` — Regenerate backup codes
- `GET /oauth/{provider}/login` — Initiate OAuth flow (Google/GitHub)
- `GET /oauth/{provider}/callback` — OAuth callback handler
- `POST /oauth/{provider}/confirm-link` — Confirm OAuth link to existing account
- `POST /oauth/{provider}/link` — Link OAuth to authenticated account
- `GET /api-tokens` — List user's API tokens
- `POST /api-tokens` — Create new API token (format: `lgn_*`)
- `PATCH /api-tokens/{token_id}` — Update API token name/scopes
- `DELETE /api-tokens/{token_id}` — Revoke API token
- `POST /verify` — Verify JWT token validity (internal API used by other services)
- `GET /health` — Health check endpoint
- Company/project CRUD routes (not listed; see `main.py:1783-2335`)

### Environment variables consumed

- `POSTGRES_URL` — PostgreSQL connection string (required; points to `kgrag_auth` DB)
- `REDIS_URI` — Redis connection for JWT blacklist and revocation timestamps (required)
- `JWT_SECRET_KEY` — HMAC-SHA256 secret for signing JWTs (required; used by all KGRAG services)
- `ACCESS_TOKEN_EXPIRE_MINUTES` — Access token TTL in minutes (default: 10080 = 7 days)
- `REFRESH_TOKEN_EXPIRE_DAYS` — Refresh token TTL in days (default: 30)
- `TOTP_ENCRYPTION_KEY` — Fernet encryption key for storing TOTP secrets in DB (required)
- `INFOBIP_API_KEY` — Infobip API key for sending verification emails (optional; email disabled if not set)
- `INFOBIP_BASE_URL` — Infobip API base URL (optional)
- `VERIFICATION_FROM_EMAIL` — Sender address for verification emails (default: `noreply@kgrag.ai`)
- `SKIP_EMAIL_VERIFICATION` — When `true`, `/register` auto-activates accounts without email send (DEV ONLY; default: `false`)
- `TOKEN_BLACKLIST_FAIL_OPEN` — When `true`, allows tokens if Redis is down (testing only; default: `false`)
- `PYTHONUNBUFFERED=1` — Force unbuffered stdout for Docker logs

## Outputs

### Database tables written (Postgres `kgrag_auth` DB)

Tables are auto-created via `Base.metadata.create_all()` on startup (`database.py:465`).

**Core auth tables**:
- `users` (id, email, username, password_hash, is_active, is_superuser, email_verified, verification_code, totp_secret, totp_enabled, totp_backup_codes, oauth_provider, oauth_provider_id, first_name, last_name, display_name, avatar_url, bio, phone_number, timezone, locale, profile_completed_at, created_at, updated_at)
- `roles` (id, name, description, created_at)
- `permissions` (id, resource, action, description)
- `user_roles` (user_id, role_id) — many-to-many junction
- `role_permissions` (role_id, permission_id) — many-to-many junction
- `api_tokens` (id, user_id, name, token_prefix, token_hash, token_hint, scopes, created_at, expires_at, last_used_at, revoked_at, created_from_ip, last_used_ip)

**Multi-tenant hierarchy**:
- `companies` (id, name, created_at, updated_at, is_active, cognee_enabled)
- `company_users` (company_id, user_id, role, joined_at) — many-to-many junction
- `projects` (id, company_id, name, description, github_token, github_webhook_secret, webhook_url, created_at, updated_at)
- `repositories` (id, project_id, name, url, created_at, updated_at)
- `branches` (id, repository_id, name, commit_sha, created_at, updated_at)

**Deletion tracking**:
- `deletion_tasks` (id, user_id, user_email, status, current_phase, created_at, started_at, completed_at, progress, external_cleanup_state, error_message, error_phase, retry_count, last_retry_at, owned_company_ids, affected_member_ids, deletion_summary)

**Knowledge domain** (`knowledge_models.py`):
- `knowledge` (id, company_id, project_id, title, text_content, when_to_use, content_hash, metadata, created_by_user_id, created_at, updated_at)
- `knowledge_chunks` (id, knowledge_id, content, summary, position, level, parent_chunk_id, chunk_type, section_title, has_code, keywords, created_at)
- `expertise` (id, company_id, project_id, title, content, summary, when_to_use, is_company_level, content_hash, metadata, created_by_user_id, created_at, updated_at)
- `expertise_chunks` (id, expertise_id, content, summary, position, level, parent_chunk_id, chunk_path, chunk_type, section_title, has_code, keywords, created_at)
- `lessons_learned` (id, company_id, project_id, title, category, symptom, root_cause, solution, prevention, severity, tags, files_changed, content, content_hash, metadata, created_by_user_id, created_at, updated_at)

Default roles are seeded on first boot: `admin`, `user`, `readonly` with corresponding permissions for `memories` and `users` resources (`database.py:487-501`).

### Responses produced

- JWT tokens: `{"access_token": "...", "refresh_token": "...", "token_type": "bearer", "expires_in": 1800}`
- 2FA challenge: `{"requires_2fa": true, "challenge_token": "..."}`
- User profile: JSON with user metadata, roles, companies, email_verified, totp_enabled
- API token creation: `{"id": "...", "token": "lgn_...", "token_hint": "...AbCd", "scopes": [...], "warning": "Save this token now. You won't be able to see it again."}`
- Token verification (for internal API): `{"valid": true, "user_id": "...", "email": "...", "roles": [...], "companies": [...], "is_superuser": false}`

### Kafka topics produced

None. kgrag-auth does not publish to Kafka.

## Dependencies

- **postgres** — Stores all user, role, company, project, knowledge, and deletion tracking data in `kgrag_auth` DB
- **redis** — JWT blacklist (revoked tokens stored as `token:blacklist:{jti}` with TTL) and user-wide revocation timestamps (`token:revoked_at:{user_id}`) used for logout and session invalidation

Optional for user deletion:
- **qdrant** (via `qdrant-client`) — Deletes user-owned company/project data from Qdrant collections during account deletion (`main.py:1039-1121`)
- **neo4j** (via `neo4j` driver) — Deletes user-owned company/project nodes during account deletion (`main.py:1123-1173`)

## How to run and smoke-test in isolation

### Start the service

```bash
docker compose up -d postgres redis kgrag-auth
```

### Smoke test: register a user (with email verification bypass)

```bash
curl -X POST http://localhost:8001/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "password": "testpass123",
    "roles": ["user"]
  }'
```

**Expected response** (when `SKIP_EMAIL_VERIFICATION=true` in `.env`):
```json
{
  "user_id": "...",
  "id": "...",
  "email": "test@example.com",
  "username": "testuser",
  "is_active": true,
  "email_verified": true,
  "message": "User created and auto-verified (SKIP_EMAIL_VERIFICATION=true)"
}
```

**Expected response** (production mode, `SKIP_EMAIL_VERIFICATION=false`):
```json
{
  "user_id": "...",
  "message": "Verification code sent to email"
}
```

### Smoke test: login

```bash
curl -X POST http://localhost:8001/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "testpass123"
  }'
```

**Expected response**:
```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

### Smoke test: health check

```bash
curl http://localhost:8001/health
```

**Expected response**:
```json
{
  "status": "healthy",
  "services": {"database": true},
  "timestamp": "2026-05-13T12:34:56.789Z"
}
```

## Operational notes

### SKIP_EMAIL_VERIFICATION behaviour

When `SKIP_EMAIL_VERIFICATION=true` (`auth/main.py:1401-1407`):
- `/register` creates users with `is_active=True`, `email_verified=True` immediately
- No email is sent
- No verification code is generated or stored
- User can log in immediately after registration
- **DEV ONLY** — never enable in production

When `SKIP_EMAIL_VERIFICATION=false` (production default):
- `/register` creates users with `is_active=False`, `email_verified=False`
- 6-digit verification code is generated, bcrypt-hashed, and stored in `users.verification_code` with 15-minute TTL
- Email sent via Infobip API with plaintext code
- User must call `/verify-email` with code to activate account
- Maximum 5 verification attempts before code invalidation

### Auto-created tables

All tables are created via `Base.metadata.create_all(bind=engine)` on first startup (`database.py:465`). No Alembic migrations are used. Default roles (`admin`, `user`, `readonly`) and permissions (`memories:read/write/delete`, `users:read/write/delete`) are seeded if the `roles` table is empty (`database.py:471-501`).

### JWT token blacklist (Redis)

Revoked tokens are stored in Redis with key `token:blacklist:{jti}` and TTL matching token expiry (`token_blacklist.py:34-56`). All services must check `is_blacklisted(jti)` before accepting a token. If Redis is unavailable, tokens are rejected (fail-closed) unless `TOKEN_BLACKLIST_FAIL_OPEN=true` (testing only).

User-wide revocation (e.g., "log out all sessions") stores a timestamp in `token:revoked_at:{user_id}` with 7-day TTL. Any token with `iat < revocation_timestamp` is considered revoked (`token_blacklist.py:78-100`).

### TOTP secrets encryption

TOTP secrets are stored encrypted in `users.totp_secret` using Fernet symmetric encryption with key from `TOTP_ENCRYPTION_KEY` env var (`services/totp_service.py`). Secrets are decrypted on-the-fly during TOTP verification. Backup codes are stored as a JSON array in `users.totp_backup_codes` (PostgreSQL `ARRAY(String)` type).

### API token format

API tokens use prefix `lgn_` + base64-encoded 32 random bytes (~47 chars total). Only the SHA-256 hash is stored in `api_tokens.token_hash`. The plaintext token is returned ONCE at creation and never shown again (`api_token_utils.py:25-78`). Tokens are verified by hashing the provided token and comparing to the stored hash. `last_used_at` is updated asynchronously via an in-memory buffer flushed every 60 seconds to avoid blocking request handlers (`main.py:567-747`).

### User deletion cascade phases

Deletion is phased to prevent orphaned data (forensic map §7, `main.py:976-1376`):
1. **Phase 0**: Invalidate all user sessions in Redis (prevents actions during deletion)
2. **Phase 1**: Clean external storage — delete Qdrant points and Neo4j nodes for owned companies/projects
3. **Phase 2**: Postgres cascade delete — companies → projects → repositories → branches → user record

Each phase is idempotent and resumable from `deletion_tasks` table. If Phase 1 fails, Postgres remains intact (safe rollback). External cleanup uses `company_id` and `project_id` filters.

### Known quirks

- OAuth tokens (`oauth_access_token`, `oauth_refresh_token`) are stored encrypted but never refreshed automatically. The service does not implement OAuth token refresh flows.
- The `projects.github_token` column is written by `kgrag-rest-api` (via `api/repositories/project_repository.py`), not by kgrag-auth itself. kgrag-auth only provides the schema.
- API token `last_used_at` updates are buffered in-process and flushed every 60 seconds. If the container crashes, the last minute of usage data may be lost (acceptable tradeoff for performance).

## Code map

- `main.py` — FastAPI app; all HTTP routes for auth, profile, 2FA, OAuth, API tokens, companies, projects, user deletion
- `database.py` — SQLAlchemy models for users, roles, permissions, companies, projects, repositories, branches, api_tokens, deletion_tasks; database init and health check
- `knowledge_models.py` — SQLAlchemy models for knowledge, expertise, lessons_learned tables (used by kgrag-cognee gRPC service)
- `jwt_utils.py` — JWT creation and verification (access/refresh tokens, token payload builder, password hashing with bcrypt)
- `token_blacklist.py` — Redis-backed token blacklist and user-wide revocation (fail-closed by default)
- `api_token_utils.py` — API token generation, hashing, verification, scope validation (`lgn_*` prefix format)
- `services/email_service.py` — Email verification code generation and Infobip API integration
- `services/totp_service.py` — TOTP secret generation, QR code generation, TOTP/backup code verification, Fernet encryption/decryption
- `oauth/client.py` — OAuth2 client for Google/GitHub (state generation, token exchange, user info fetch)
- `oauth/providers.py` — OAuth provider configuration (client IDs, secrets, endpoints)
- `utils/logging_filter.py` — Loguru filter to redact sensitive fields (passwords, tokens, secrets) from logs
- `tests/` — Pytest unit tests for API token utils and endpoints (coverage: token creation, verification, revocation, scope validation)
