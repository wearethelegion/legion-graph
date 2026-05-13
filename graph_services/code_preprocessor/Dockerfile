FROM python:3.13-slim

WORKDIR /app

# System dependencies: git for cloning, build-essential for tree-sitter native extensions
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements first for better caching
COPY code_inteligence_preprocessor/requirements.txt .

# Install cognee-community-vector-adapter-qdrant FIRST with --no-deps
# to prevent it from pulling a stale cognee version that conflicts with
# the pinned version in requirements.txt (same pattern as Dockerfile.cognee).
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --ignore-requires-python --no-deps cognee-community-vector-adapter-qdrant \
    && pip install --no-cache-dir -r requirements.txt

WORKDIR /app

# Copy application code
COPY code_inteligence_preprocessor/ ./code_inteligence_preprocessor/
COPY document_preprocessor/ ./document_preprocessor/
COPY config/ ./config/
COPY shared/ ./shared/

# Copy cognee_service modules needed for cognee.add() integration:
#   cognee_patches.py — MUST be imported before any cognee import
#   multi_tenancy.py  — ensure_neo4j_database + set_company_context
#   gemini_fallback_router.py — Gemini API primary with Vertex AI failover
COPY cognee_service/__init__.py ./cognee_service/__init__.py
COPY cognee_service/cognee_patches.py ./cognee_service/cognee_patches.py
COPY cognee_service/multi_tenancy.py ./cognee_service/multi_tenancy.py
COPY cognee_service/gemini_fallback_router.py ./cognee_service/gemini_fallback_router.py

# Set Python path
ENV PYTHONPATH=/app

# Create user and cognee data directory with proper permissions
RUN useradd -m -u 1000 appuser \
    && mkdir -p /data/cognee/data \
    && chown -R appuser:appuser /app /data/cognee
USER appuser

# Run the application
CMD ["python", "-m", "code_inteligence_preprocessor.main"]
