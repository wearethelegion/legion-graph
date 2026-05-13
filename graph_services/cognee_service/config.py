"""
Cognee service — configuration loader.
Must be called at startup BEFORE any cognee.add()/cognify()/search() calls.
"""

import os


def configure_cognee() -> None:
    # Qdrant adapter is already registered by cognee_patches (imported at server startup).
    # Just import cognee — no separate registration step needed here.
    import cognee

    # ── LLM ──────────────────────────────────────────────────────────────────
    llm_provider = os.environ.get("LLM_PROVIDER", "gemini")
    cognee.config.llm_provider = llm_provider
    cognee.config.llm_model = os.environ.get("LLM_MODEL", "gemini/gemini-3.1-flash-lite-preview")
    cognee.config.llm_api_key = os.environ["LLM_API_KEY"]
    cognee.config.llm_api_version = os.environ.get("LLM_API_VERSION", "v1beta")
    # Only set custom endpoint for OpenAI-compatible providers (OpenRouter, OpenAI, custom)
    # Gemini provider uses LiteLLM native routing — no custom endpoint needed
    if llm_provider in ("openai", "openrouter", "custom"):
        cognee.config.llm_endpoint = os.environ.get("LLM_ENDPOINT", "https://openrouter.ai/api/v1")
    cognee.config.llm_instructor_mode = os.environ.get("LLM_INSTRUCTOR_MODE", "tool_call")

    # ── Embeddings ────────────────────────────────────────────────────────────
    cognee.config.embedding_provider = os.environ.get("EMBEDDING_PROVIDER", "vertex_ai")
    # cognee.config.embedding_model = os.environ.get("EMBEDDING_MODEL", "gemini-embedding-2-preview")
    cognee.config.embedding_model = os.environ.get("EMBEDDING_MODEL", "gemini-embedding-001")

    # Vertex AI service account auth — env vars read by LiteLLM directly:
    #   GOOGLE_APPLICATION_CREDENTIALS, VERTEXAI_PROJECT, VERTEXAI_LOCATION

    # ── Vector store (Qdrant) — programmatic config overrides env vars ────────
    cognee.config.set_vector_db_config(
        {
            "vector_db_provider": "qdrant",
            "vector_db_url": os.environ.get("VECTOR_DB_URL", "http://qdrant:6333"),
            "vector_db_key": os.environ.get("QDRANT_API_KEY", ""),
        }
    )

    # ── Graph database (Neo4j) ────────────────────────────────────────────────
    cognee.config.graph_database_provider = "neo4j"
    cognee.config.graph_database_url = os.environ["NEO4J_URI"]
    cognee.config.graph_database_username = os.environ.get("NEO4J_USERNAME", "neo4j")
    cognee.config.graph_database_password = os.environ["NEO4J_PASSWORD"]
    cognee.config.graph_database_name = os.environ.get("COGNEE_NEO4J_DATABASE", "cognee-global")

    # ── Relational database (PostgreSQL) ──────────────────────────────────────
    cognee.config.db_provider = os.environ.get("DB_PROVIDER", "postgres")
    cognee.config.db_host = os.environ.get("DB_HOST", "postgres")
    cognee.config.db_port = int(os.environ.get("DB_PORT", "5432"))
    cognee.config.db_name = os.environ.get("DB_NAME", "cognee")
    cognee.config.db_username = os.environ.get("DB_USERNAME", "kgrag")
    cognee.config.db_password = os.environ.get("DB_PASSWORD", "")

    # ── Telemetry ─────────────────────────────────────────────────────────────
    try:
        cognee.config.telemetry_disabled = True
    except AttributeError:
        pass

    # ── Storage directories ───────────────────────────────────────────────────
    data_root = os.environ.get("DATA_ROOT_DIRECTORY", "/data/cognee/data")
    try:
        cognee.config.data_root_directory = data_root
    except AttributeError:
        pass
    try:
        cognee.config.system_root_directory = os.environ.get("SYSTEM_ROOT_DIRECTORY", data_root)
    except AttributeError:
        pass
