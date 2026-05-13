# ============================================================
# kgrag Makefile — Docker build and compose orchestration
#
# Targets:
#   make base              Build kgrag-base:latest
#   make build             Build base + all 4 child images
#   make up                build + docker compose up -d
#   make down              docker compose down
#   make logs              tail all container logs
#   make rebuild SVC=<name>  Rebuild a single service image (e.g. SVC=kgrag-search)
#   make ps                show container status
#   make clean             down -v + remove all kgrag images
#
# Build ordering note:
#   `make build` MUST run `docker build -f Dockerfile.base` first because
#   docker compose cannot express cross-image build dependencies for the
#   "FROM kgrag-base:latest" child images. Running `docker compose build`
#   directly will fail if kgrag-base:latest hasn't been built yet.
# ============================================================

COMPOSE_FILE := docker/docker-compose.yml
ENV_FILE     := .env
COMPOSE      := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

# Service image name for `make rebuild SVC=...`
SVC ?=

.PHONY: base build cognee up down logs rebuild ps clean help

## Build the shared base image (must be built before child images)
base:
	@echo "==> Building kgrag-base:latest..."
	docker build -f Dockerfile.base -t kgrag-base:latest .
	@echo "==> kgrag-base:latest built successfully"

## Build kgrag-cognee:latest only (requires kgrag-base:latest)
cognee:
	@echo "==> Building kgrag-cognee:latest..."
	docker build -f Dockerfile.cognee -t kgrag-cognee:latest .
	@echo "==> kgrag-cognee:latest built successfully"

## Build base + all child images (ingestion, search, auth, rest-api, cognee)
build: base
	@echo "==> Building kgrag-ingestion:latest..."
	docker build -f Dockerfile.ingestion -t kgrag-ingestion:latest .
	@echo "==> Building kgrag-search:latest..."
	docker build -f Dockerfile.search -t kgrag-search:latest .
	@echo "==> Building kgrag-auth:latest..."
	docker build -f Dockerfile.auth -t kgrag-auth:latest .
	@echo "==> Building kgrag-rest-api:latest..."
	docker build -f Dockerfile.rest-api -t kgrag-rest-api:latest .
	@echo "==> Building kgrag-cognee:latest..."
	docker build -f Dockerfile.cognee -t kgrag-cognee:latest .
	@echo "==> All kgrag images built:"
	@docker images | grep kgrag

## Build all images then start all services in detached mode
up: build
	@echo "==> Starting kgrag stack..."
	$(COMPOSE) up -d
	@echo "==> Stack started. Use 'make ps' to check status."

## Stop and remove all containers (preserve volumes)
down:
	$(COMPOSE) down

## Tail logs from all running containers
logs:
	$(COMPOSE) logs -f

## Show container status
ps:
	$(COMPOSE) ps

## Rebuild a single service image and restart its container
## Usage: make rebuild SVC=kgrag-search
rebuild:
ifndef SVC
	$(error SVC is required. Usage: make rebuild SVC=kgrag-search)
endif
	@echo "==> Rebuilding $(SVC)..."
	@case "$(SVC)" in \
		kgrag-ingestion) docker build -f Dockerfile.ingestion -t kgrag-ingestion:latest . ;; \
		kgrag-search)    docker build -f Dockerfile.search    -t kgrag-search:latest    . ;; \
		kgrag-auth)      docker build -f Dockerfile.auth      -t kgrag-auth:latest      . ;; \
		kgrag-rest-api)  docker build -f Dockerfile.rest-api  -t kgrag-rest-api:latest  . ;; \
		kgrag-cognee)    docker build -f Dockerfile.cognee    -t kgrag-cognee:latest    . ;; \
		kgrag-base)      docker build -f Dockerfile.base      -t kgrag-base:latest      . ;; \
		*) echo "Unknown service: $(SVC). Known: kgrag-base kgrag-ingestion kgrag-search kgrag-auth kgrag-rest-api kgrag-cognee"; exit 1 ;; \
	esac
	@echo "==> Restarting affected compose services..."
	$(COMPOSE) up -d --no-deps --force-recreate $$($(COMPOSE) ps --services | grep -v "postgres\|qdrant\|neo4j\|redis\|redpanda") 2>/dev/null || true

## Validate compose config without starting services
config:
	$(COMPOSE) config

## Stop, remove containers+volumes, and remove all kgrag images
clean: down
	$(COMPOSE) down -v 2>/dev/null || true
	docker rmi kgrag-base:latest kgrag-ingestion:latest kgrag-search:latest kgrag-auth:latest kgrag-rest-api:latest kgrag-cognee:latest 2>/dev/null || true
	@echo "==> Cleaned up all kgrag containers, volumes, and images"

## Print available targets
help:
	@grep -E '^##' Makefile | sed 's/^## //'

.PHONY: sbom
sbom:  ## Regenerate sbom.json from running container images
	python3 scripts/generate_sbom.py
