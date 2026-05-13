# Docker Secrets Setup

This directory is for Docker Secrets configuration (simple production deployment).

## Quick Start

1. Copy example files (remove `.example` suffix):
   ```bash
   cp gemini_api_key.example gemini_api_key
   cp neo4j_password.example neo4j_password
   cp qdrant_api_key.example qdrant_api_key
   ```

2. Edit files with your actual secrets:
   ```bash
   echo "your-actual-gemini-key" > gemini_api_key
   echo "your-actual-neo4j-password" > neo4j_password
   echo "your-actual-qdrant-key" > qdrant_api_key
   ```

3. Update `docker-compose.yml` to use secrets (see below)

## Docker Compose Integration

Add to your `docker-compose.yml`:

```yaml
services:
  your-service:
    secrets:
      - gemini_api_key
      - neo4j_password
      - qdrant_api_key

secrets:
  gemini_api_key:
    file: ./secrets/gemini_api_key
  neo4j_password:
    file: ./secrets/neo4j_password
  qdrant_api_key:
    file: ./secrets/qdrant_api_key
```

Secrets will be mounted at `/run/secrets/<secret_name>` inside containers.

## Security Notes

- All actual secret files are gitignored
- Never commit actual secrets to version control
- File permissions should be 600 (owner read/write only)
- Use Docker Swarm secrets for production orchestration

## Migration from .env

Your existing `.env` workflow continues to work. Docker Secrets are optional:

- **Development**: Use `.env` files (simple, fast)
- **Simple Production**: Use Docker Secrets (file-based)
- **Enterprise Production**: Use Vault integration

Kgrag automatically tries backends in order: Vault → Docker Secrets → Environment
