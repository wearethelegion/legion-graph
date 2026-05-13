# API Reference

Quick reference. For exact request/response schemas inspect the route handlers in `api/routes/`, `auth/main.py`, or run the services and visit the OpenAPI docs:

- `http://localhost:8000/docs` — REST API (FastAPI auto-generated)
- `http://localhost:8001/docs` — Auth service
- `grpcurl -plaintext localhost:50051 list` — gRPC services

---

## REST — Auth service (`:8001`)

Owns identity, companies, JWTs.

### Authentication
| Method | Path                        | Purpose                                       |
|--------|-----------------------------|-----------------------------------------------|
| POST   | `/register`                 | Create a new user                             |
| POST   | `/login`                    | Email + password → JWT                        |
| POST   | `/login/2fa`                | Complete 2FA challenge                        |
| POST   | `/refresh`                  | Exchange refresh token for new access token   |
| POST   | `/verify-email`             | Confirm email via emailed link                |
| POST   | `/resend-verification`      | Re-send verification email                    |
| POST   | `/verify`                   | Validate a JWT (used by other services)       |
| POST   | `/logout`                   | Revoke current tokens                         |

### 2FA
| Method | Path                | Purpose                                  |
|--------|---------------------|------------------------------------------|
| POST   | `/2fa/setup`        | Begin TOTP setup, return QR              |
| POST   | `/2fa/verify-setup` | Confirm TOTP code, enable 2FA            |
| POST   | `/2fa/disable`      | Disable 2FA                              |

### User management
| Method | Path                              | Purpose                       |
|--------|-----------------------------------|-------------------------------|
| GET    | `/users/me`                       | Current user info             |
| GET    | `/users`                          | List users (admin)            |
| GET    | `/users/me/profile`               | Full profile                  |
| PATCH  | `/users/me`                       | Update profile                |
| GET    | `/users/me/delete-preview`        | Preview deletion impact       |
| DELETE | `/users/me`                       | Delete account                |
| GET    | `/users/me/deletion-status/{id}`  | Check deletion task           |

### API tokens (machine credentials)
| Method | Path                                  | Purpose                          |
|--------|---------------------------------------|----------------------------------|
| POST   | `/users/me/api-tokens`                | Create a scoped API token        |
| GET    | `/users/me/api-tokens`                | List tokens                      |
| GET    | `/users/me/api-tokens/{token_id}`     | Token details                    |
| PATCH  | `/users/me/api-tokens/{token_id}`     | Update name / scopes             |
| DELETE | `/users/me/api-tokens/{token_id}`     | Revoke                           |

### Roles & permissions
| Method | Path           | Purpose                |
|--------|----------------|------------------------|
| POST   | `/roles`       | Create role            |
| GET    | `/roles`       | List roles             |
| GET    | `/permissions` | List permissions       |

### Companies (yes — companies live in the auth service)
| Method | Path                        | Purpose                  |
|--------|-----------------------------|--------------------------|
| POST   | `/companies`                | Create company           |
| GET    | `/companies`                | List my companies        |
| GET    | `/companies/{company_id}`   | Company details          |
| PATCH  | `/companies/{company_id}`   | Update                   |
| DELETE | `/companies/{company_id}`   | Delete                   |

### Health
| Method | Path     | Purpose         |
|--------|----------|-----------------|
| GET    | `/health`| Liveness        |
| GET    | `/`      | Service info    |

---

## REST — Main API (`:8000`)

Owns projects, repositories, branches, documents, ingestion triggers, search.

All routes are under `/api/v1/...` or `/api/v2/...`. v2 is the current shape for company/project/repo/branch endpoints.

### Companies (v2 mirror — read-only window into the auth service's data)
| Method | Path                                      | Purpose                  |
|--------|-------------------------------------------|--------------------------|
| POST   | `/api/v2/companies`                       | Create                   |
| GET    | `/api/v2/companies`                       | List                     |
| GET    | `/api/v2/companies/{company_id}`          | Details                  |
| DELETE | `/api/v2/companies/{company_id}`          | Delete                   |

### Projects
| Method | Path                                                | Purpose                         |
|--------|-----------------------------------------------------|---------------------------------|
| POST   | `/api/v2/projects`                                  | Create (under a company)        |
| GET    | `/api/v2/projects`                                  | List visible projects           |
| GET    | `/api/v2/projects/projects/{project_id}`            | Project details                 |
| PUT    | `/api/v2/projects/projects/{project_id}`            | Update                          |
| DELETE | `/api/v2/projects/projects/{project_id}`            | Delete                          |

### Repositories
| Method | Path                                                          | Purpose             |
|--------|---------------------------------------------------------------|---------------------|
| POST   | `/api/v2/projects/{project_id}/repositories`                  | Register repo       |
| GET    | `/api/v2/projects/{project_id}/repositories`                  | List                |
| GET    | `/api/v2/projects/{project_id}/repositories/{repository_id}`  | Details             |

### Branches
| Method | Path                                                              | Purpose       |
|--------|-------------------------------------------------------------------|---------------|
| POST   | `/api/v2/repositories/{repository_id}/branches`                   | Register      |
| GET    | `/api/v2/repositories/{repository_id}/branches`                   | List          |
| GET    | `/api/v2/repositories/{repository_id}/branches/{branch_id}`       | Details       |

### Ingestion
| Method | Path                                                  | Purpose                                 |
|--------|-------------------------------------------------------|-----------------------------------------|
| POST   | `/api/v1/code_ingestion`                              | Trigger ingestion (publishes to Kafka)  |
| GET    | `/api/v1/ingestions/{ingestion_id}`                   | Status of one ingestion                 |
| GET    | `/api/v1/ingestions/{ingestion_id}/progress`          | Detailed progress                       |
| GET    | `/api/v1/projects/{project_id}/ingestions`            | List ingestions for a project           |

### Documents
| Method | Path                                       | Purpose                              |
|--------|--------------------------------------------|--------------------------------------|
| POST   | `/api/v1/documents/upload`                 | Upload files (multipart)             |
| GET    | `/api/v1/documents`                        | List (filter by `project_id`)        |
| GET    | `/api/v1/documents/{document_id}`          | Document details                     |
| GET    | `/api/v1/documents/{document_id}/status`   | Processing status                    |

### Brain content (legacy knowledge store)
| Method | Path                              | Purpose                            |
|--------|-----------------------------------|------------------------------------|
| POST   | `/api/v1/brain`                   | Create knowledge content           |
| GET    | `/api/v1/brain`                   | List                               |
| GET    | `/api/v1/brain/{content_id}`      | Get                                |
| PUT    | `/api/v1/brain/{content_id}`      | Update                             |
| DELETE | `/api/v1/brain/{content_id}`      | Delete                             |
| POST   | `/api/v1/brain/search`            | Search knowledge                   |

### Code search (REST mirror of the gRPC service)
| Method | Path                              | Purpose                          |
|--------|-----------------------------------|----------------------------------|
| POST   | `/api/v1/code-search/search`      | Semantic search over code        |

### Webhooks
| Method | Path                                                          | Purpose                                  |
|--------|---------------------------------------------------------------|------------------------------------------|
| POST   | `/api/v1/webhooks/github/{company_name}/{project_name}`       | GitHub push webhook → triggers ingestion |

### Other surfaces
- `/api/v1/agents`, `/api/v1/agent-workflows` — agent registry/workflow surface.
- `/api/v1/features`, `/api/v1/company-config`, `/api/v1/company-roles` — company configuration.
- `/api/v1/instructions` — system/project instructions.
- `/api/v1/stats` — usage statistics.
- `/api/v1/cli/version` — CLI version metadata.
- `/api/v1/registration-requests` — invitation flow.

### Health
| Method | Path     | Purpose       |
|--------|----------|---------------|
| GET    | `/health`| Liveness      |

---

## gRPC — port `:50051`

All proto packages are under `kgrag.*`. The full `.proto` definitions live in `grpc_server/protos/`.

### `kgrag.auth.AuthService`
| RPC               | Purpose                          |
|-------------------|----------------------------------|
| `Authenticate`    | Validate credentials → JWT       |
| `GetProjects`     | List projects visible to user    |

### `kgrag.code.CodeService`
| RPC                       | Purpose                                                  |
|---------------------------|----------------------------------------------------------|
| `CreateCode`              | Write code into the graph (internal use)                 |
| `FindSimilarCode`         | Vector similarity over code snippets                     |
| `AnalyzeImpact`           | Upstream callers + downstream callees of an entity       |
| `TraceExecutionFlow`      | DFS from an entry point                                  |

### `kgrag.code_search.CodeSearchService`
| RPC                       | Purpose                                                                  |
|---------------------------|--------------------------------------------------------------------------|
| `GetDomains`              | List code domains/projects visible to the caller                         |
| `SearchEntities`          | Vector search over code entities (functions, classes, methods)           |
| `SearchSummaries`         | Vector search over LLM-generated chunk summaries                         |
| `GetCodeForEntity`        | Fetch source code for an entity by ID                                    |
| `TraverseGraph`           | Walk the code call graph from an anchor entity                           |
| `GetEntityGraph`          | Local graph (callers + callees) around an entity                         |
| `FullSearch`              | **Hybrid retrieval** — vector + graph + RRF fusion. Use this first.      |
| `Health`                  | Liveness                                                                 |

### `kgrag.document_search.DocumentSearchService`
| RPC                          | Purpose                                                |
|------------------------------|--------------------------------------------------------|
| `GetCollections`             | List document collections in scope                     |
| `SearchDocuments`            | Vector search over document chunks                     |
| `GetDocumentChunk`           | Fetch a specific chunk by ID                           |
| `SearchDocumentSummaries`    | Vector search over chunk-level summaries               |
| `TraverseDocumentGraph`      | Walk the document graph (sections / entities)          |
| `FullDocumentSearch`         | Hybrid retrieval (vector + graph + RRF). Use first.    |
| `Health`                     | Liveness                                               |

### `kgrag.ingestion.IngestionService`
| RPC                       | Purpose                                                       |
|---------------------------|---------------------------------------------------------------|
| `GetIngestionStatus`      | Get state of a specific ingestion                             |
| `ListIngestions`          | List ingestions (paginated, filterable)                       |

> Note: `IngestionService` is **read-only**. To start an ingestion use the REST endpoint `POST /api/v1/code_ingestion` or `POST /api/v1/documents/upload`.

### Standard gRPC services
- `grpc.health.v1.Health` — health check protocol.
- `grpc.reflection.v1alpha.ServerReflection` — enables `grpcurl list`.

---

## Authentication

For both REST and gRPC, pass a JWT in the standard header:

**REST:**
```
Authorization: Bearer <jwt>
```

**gRPC (with `grpcurl`):**
```
-H "authorization: Bearer <jwt>"
```

**gRPC (programmatic):** add `("authorization", f"Bearer {jwt}")` to call metadata.

The `AuthenticationInterceptor` in `kgrag-search` validates the token's signature using `JWT_SECRET_KEY` and rejects expired or malformed tokens with `UNAUTHENTICATED`.

API tokens issued via `POST /users/me/api-tokens` are JWTs with scoped permissions — use them the same way.

---

## Idempotency

The gRPC server includes an `IdempotencyInterceptor` that deduplicates mutation calls. Pass a unique key in metadata:

```
-H "idempotency-key: <uuid>"
```

Cache TTL is `GRPC_IDEMPOTENCY_CACHE_TTL=3600` seconds by default. Backed by Redis.
