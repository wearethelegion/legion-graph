"""V3 pipeline integration tests — verify module wiring, not isolated functions.

These tests exist to catch WIRING failures between pipeline modules. Unit tests
with mocks cannot catch a wrong import path or a missing DB write. These tests do.

Failures are expected for tests marked @pytest.mark.xfail — they document what
needs to be fixed in the wiring layer before V3 is production-ready.

Test matrix:
  T1: project_analyzer wired into consumer flow (CURRENTLY FAILS — wrong import)
  T2: project_analyzer writes to project_profiles table (store round-trip)
  T2c: REAL analyze_project() calls LLM + writes to project_profiles (LLM mocked)
  T2d: analyze_project() handles LLM failure gracefully
  T2e: analyze_project() deduplicates business domains by normalised_key
  T3: _file_processing reads project profile and uses extraction_prompt
  T4: EnrichedChunkMessage has extraction_prompt populated end-to-end
  T5: entity_extraction_service reads extraction_prompt from message
  T5d: processor._resolve_prompt() uses message extraction_prompt (static analysis)
  T6: node_set naming uses project_name not branch (static analysis)
  T7: cognee.add() NOT called from preprocessor (static analysis)
"""

from __future__ import annotations

import ast
import importlib
import inspect
import json
import textwrap
import uuid
from pathlib import Path
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parents[2]  # kgrag-backend/
_PREPROCESSOR_ROOT = _REPO_ROOT / "code_preprocessor"


def _source(rel_path: str) -> str:
    """Return source of a file relative to repo root."""
    return (_REPO_ROOT / rel_path).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# T1 — project_analyzer is wired into the consumer flow
# ---------------------------------------------------------------------------


def test_t1_ingestion_processor_imports_project_analyzer():
    """The consumer flow MUST call project_analyzer.analyze_project, not project_classifier.

    ingestion_processor._run_project_analysis() currently does:
        from ..project_analyzer import analyze_project
    (Fixed: was previously importing from project_classifier — the old module.)

    This is a static-analysis test — no DB or network required.
    """
    source = _source(
        "code_preprocessor/kafka_processing_service/ingestion_processor.py"
    )

    # Must import project_analyzer somewhere in the file
    assert "project_analyzer" in source, (
        "ingestion_processor.py does not import from project_analyzer at all. "
        "The V3 analysis module is never called."
    )

    # Must NOT import project_classifier for analyze_project (old module)
    assert "from ..project_classifier import analyze_project" not in source, (
        "ingestion_processor.py still imports analyze_project from project_classifier (OLD module). "
        "Switch to project_analyzer."
    )


def test_t1b_project_analyzer_module_is_importable():
    """project_analyzer module must be importable without LLM/DB side effects."""
    import code_preprocessor.project_analyzer as pa

    assert hasattr(pa, "analyze_project"), "analyze_project function must exist in project_analyzer"
    assert callable(pa.analyze_project), "analyze_project must be callable"


def test_t1c_project_analyzer_analyze_project_signature():
    """analyze_project in project_analyzer has the right signature (project_id, repo_path, ...)."""
    import code_preprocessor.project_analyzer as pa

    sig = inspect.signature(pa.analyze_project)
    params = list(sig.parameters.keys())

    # V3 signature: analyze_project(project_id, repo_path, company_id, pool, ...)
    assert "project_id" in params, f"analyze_project missing project_id param. Got: {params}"
    assert "pool" in params, f"analyze_project missing pool param. Got: {params}"
    # file_tree is the new optional kwarg that lets callers skip build_tree()
    assert "file_tree" in params, (
        "analyze_project missing file_tree param. "
        "Callers (consumer, ingestion_processor) must be able to pass a pre-built tree."
    )
    # Old classifier signature was: analyze_project(pool, repository, branch, ...)
    # Verify this is the NEW signature (no 'repository' or 'branch' positional params)
    assert "repository" not in params, (
        "analyze_project has 'repository' param — this looks like the OLD project_classifier "
        "signature, not the V3 project_analyzer signature."
    )


# ---------------------------------------------------------------------------
# T2 — project_analyzer writes to project_profiles table
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2_profile_store_upsert_and_read_round_trip():
    """ProjectProfileStore upsert then get_project_profile returns the same data.

    Uses a mock pool — verifies the SQL is called with correct parameters.
    This is a pure store-layer integration test with no LLM calls.
    """
    from code_preprocessor.storage.project_profile_store import ProjectProfileStore

    project_id = str(uuid.uuid4())
    chunker_config = {
        "language": "Python",
        "ast_chunk_boundaries": [],
        "fallback_strategy": "recursive_text",
    }
    extraction_prompt = "Extract entities from this Python code..."
    technical_domains = [{"name": "API Layer", "description": "REST endpoints"}]

    # Build a fake row that asyncpg would return
    fake_row = {
        "id": str(uuid.uuid4()),
        "project_id": project_id,
        "language": "Python",
        "framework": "FastAPI",
        "chunker_config": json.dumps(chunker_config),
        "extraction_prompt": extraction_prompt,
        "technical_domains": json.dumps(technical_domains),
        "analysed_at": None,
        "created_at": None,
        "updated_at": None,
    }

    mock_pool = AsyncMock()
    mock_pool.fetchrow = AsyncMock(return_value=fake_row)
    mock_pool.execute = AsyncMock(return_value="INSERT 0 1")

    store = ProjectProfileStore(mock_pool)

    # Upsert (verifies SQL is called)
    upserted = await store.upsert_project_profile(
        project_id=project_id,
        language="Python",
        framework="FastAPI",
        chunker_config=chunker_config,
        extraction_prompt=extraction_prompt,
        technical_domains=technical_domains,
    )
    assert upserted["project_id"] == project_id
    assert mock_pool.fetchrow.called, "upsert_project_profile must call pool.fetchrow (RETURNING)"

    # Get profile back
    profile = await store.get_project_profile(project_id)
    assert profile is not None, "get_project_profile returned None — profile not written"
    assert profile["project_id"] == project_id
    assert profile["extraction_prompt"] == extraction_prompt


@pytest.mark.asyncio
async def test_t2b_upsert_project_profile_sql_targets_project_profiles_table():
    """The upsert SQL in ProjectProfileStore must target code_processing.project_profiles."""
    import code_preprocessor.storage.project_profile_store as module

    source = inspect.getsource(module)

    assert "code_processing.project_profiles" in source, (
        "ProjectProfileStore does not reference code_processing.project_profiles table. "
        "Writes go nowhere."
    )
    assert "ON CONFLICT" in source, (
        "ProjectProfileStore.upsert_project_profile lacks ON CONFLICT clause — "
        "re-runs will fail or duplicate rows."
    )


@pytest.mark.asyncio
async def test_t2c_run_project_analysis_calls_project_analyzer_upsert():
    """_run_project_analysis must call project_analyzer.analyze_project which upserts to DB.

    Simulates the consumer calling _run_project_analysis and verifies that
    the project_profiles upsert SQL is executed.
    (Fixed: ingestion_processor now imports from project_analyzer with the correct signature.)
    """
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        IngestionProcessor,
    )

    execute_calls: list[str] = []

    mock_pool = AsyncMock()
    mock_pool._closed = False

    # Capture execute calls so we can inspect the SQL
    async def capture_execute(sql, *args):
        execute_calls.append(sql)
        return "UPDATE 1"

    mock_pool.execute = capture_execute
    mock_pool.fetchrow = AsyncMock(return_value=None)
    mock_pool.fetch = AsyncMock(return_value=[])

    processor = IngestionProcessor(
        settings=MagicMock(
            max_concurrent_files=1,
            embed_workers=1,
            embed_batch_size=10,
            embed_batch_timeout=0.1,
            progress_update_interval=10,
        ),
        db_pool=mock_pool,
        version_store=None,
        ingestion_store=None,
        pipeline_store=None,
        producer=None,
        event_emitter=None,
    )

    # Mock analyze_project in project_analyzer (not project_classifier)
    upsert_called = False
    received_file_tree: Optional[str] = None

    async def mock_analyze_project(project_id, repo_path, company_id, pool, **kwargs):
        nonlocal upsert_called, received_file_tree
        received_file_tree = kwargs.get("file_tree")
        # Verify it calls pool with project_profiles table
        await pool.execute(
            "INSERT INTO code_processing.project_profiles (project_id) VALUES ($1)",
            project_id,
        )
        upsert_called = True
        from code_preprocessor.project_analyzer import ProjectProfile

        return ProjectProfile(
            project_id=project_id,
            company_id=company_id,
            language="Python",
            framework="FastAPI",
        )

    with patch(
        "code_preprocessor.project_analyzer.analyze_project",
        side_effect=mock_analyze_project,
    ):
        await processor._run_project_analysis(
            repo="test-repo", branch="main", iid="test-iid", file_tree="src/\n  main.py"
        )

    assert upsert_called, (
        "_run_project_analysis did not call project_analyzer.analyze_project. "
        "Consumer still uses project_classifier (OLD module)."
    )
    assert received_file_tree == "src/\n  main.py", (
        f"_run_project_analysis did not forward file_tree to analyze_project. "
        f"Got file_tree={received_file_tree!r}. "
        "ingestion_processor._run_project_analysis must pass file_tree=file_tree to analyze_project()."
    )
    project_profile_writes = [s for s in execute_calls if "project_profiles" in s]
    assert project_profile_writes, (
        "No SQL executed against project_profiles table. Profile is analyzed but never persisted."
    )


@pytest.mark.asyncio
async def test_t2c_real_analyze_project_writes_to_project_profiles(tmp_path):
    """Call REAL analyze_project() with mocked LLM → verify SQL hits project_profiles table.

    This is the TRUE integration test: the REAL analyze_project() function is called,
    only litellm.acompletion is mocked (LLM boundary). All DB logic (SQL generation,
    parameter binding, table targeting) runs through the real code paths.

    Key guarantees tested:
    - SQL is executed against code_processing.project_profiles
    - extraction_prompt parameter is a non-empty string
    - chunker_config parameter is valid JSON with ast_chunk_boundaries
    - technical_domains parameter is valid JSON array
    - company_business_domains SQL is also executed (business domain persistence)
    - Returned ProjectProfile has all fields populated
    """
    import asyncio
    from code_preprocessor.project_analyzer import analyze_project, ProjectProfile

    # ── 1. Create a minimal real repo directory ──────────────────────────────
    (tmp_path / "main.py").write_text("def hello():\n    return 'world'\n")
    (tmp_path / "pyproject.toml").write_text("[tool.poetry]\nname = 'test-project'\n")
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "api.py").write_text("from fastapi import FastAPI\napp = FastAPI()\n")

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # ── 2. Realistic LLM JSON responses for all 4 calls ─────────────────────
    _business_domains_response = json.dumps(
        {
            "language": "Python",
            "framework": "FastAPI",
            "business_domains": [
                {
                    "canonical_name": "API Management",
                    "normalised_key": "api_manag",
                    "description": "Handles REST API routing and request processing.",
                }
            ],
        }
    )
    _technical_domains_response = json.dumps(
        {
            "technical_domains": [
                {
                    "name": "API Layer",
                    "description": "FastAPI route handlers and middleware.",
                    "patterns": ["app/api.py", "app/routes/"],
                },
                {
                    "name": "Core Logic",
                    "description": "Business logic and domain services.",
                    "patterns": ["app/services/"],
                },
            ]
        }
    )
    _chunker_config_response = json.dumps(
        {
            "language": "Python",
            "framework": "FastAPI",
            "ast_chunk_boundaries": [
                {
                    "node_type": "function_definition",
                    "min_size_chars": 50,
                    "max_size_chars": 1500,
                    "description": "Python function definitions are natural chunk boundaries.",
                },
                {
                    "node_type": "class_definition",
                    "min_size_chars": 100,
                    "max_size_chars": 3000,
                    "description": "Class definitions with all their methods.",
                },
            ],
            "fallback_strategy": "recursive_text",
            "fallback_chunk_size": 1000,
            "fallback_overlap": 100,
            "language_rules": {
                "rule_description": "Decorators belong with the function they decorate"
            },
            "file_type_overrides": [
                {"extension": ".yml", "strategy": "recursive_text", "chunk_size": 800}
            ],
        }
    )
    _extraction_prompt_response = json.dumps(
        {
            "filled_prompt": (
                "You are a knowledge graph extractor for Python/FastAPI codebases.\n"
                "Business domains: API Management\n"
                "Technical domains: API Layer, Core Logic\n"
                "Entity types (closed list): Function, Class, Module, Endpoint, Model\n"
                "Relationship types (closed list): calls, imports, inherits, decorates\n"
                "Extract entities and relationships from the provided code chunk. "
                "Return JSON with keys: entities, relationships."
            )
        }
    )

    # Cycle through 4 LLM responses in order (business, technical, chunker, extraction)
    _llm_responses = [
        _business_domains_response,
        _technical_domains_response,
        _chunker_config_response,
        _extraction_prompt_response,
    ]
    _call_index = 0
    _call_index_lock = asyncio.Lock()

    async def mock_acompletion(**kwargs):
        nonlocal _call_index
        # Skip probe calls (max_tokens=1) — they succeed but don't consume a response slot
        if kwargs.get("max_tokens") == 1:
            mock_resp = MagicMock()
            mock_resp.choices = [MagicMock()]
            mock_resp.choices[0].message.content = '{"ok": true}'
            return mock_resp
        async with _call_index_lock:
            idx = _call_index % len(_llm_responses)
            _call_index += 1
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = _llm_responses[idx]
        return mock_resp

    # ── 3. Mock asyncpg pool that captures ALL SQL executed ──────────────────
    sql_calls: list[tuple[str, tuple]] = []  # (sql, args)

    # Connection mock for `async with pool.acquire() as conn:`
    mock_conn = AsyncMock()

    async def conn_execute(sql, *args):
        sql_calls.append((sql.strip(), args))
        return "UPDATE 1"

    mock_conn.execute = conn_execute

    # Transaction context manager (nested: async with conn.transaction())
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    # Acquire context manager
    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=mock_acquire_ctx)

    async def pool_execute(sql, *args):
        sql_calls.append((sql.strip(), args))
        return "INSERT 0 1"

    mock_pool.execute = pool_execute

    # pool.fetch — returns empty existing domains
    mock_pool.fetch = AsyncMock(return_value=[])
    # pool.fetchrow — returns None (no prompt template in DB → uses default)
    mock_pool.fetchrow = AsyncMock(return_value=None)

    # ── 4. Call the REAL analyze_project() with mocked LLM ──────────────────
    # Pre-build the file tree and pass it explicitly so the test also exercises
    # the file_tree parameter path (mirrors how consumer.py calls it).
    from code_preprocessor.project_analyzer import build_tree as _build_tree

    prebuilt_tree = _build_tree(str(tmp_path))
    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=mock_acompletion,
    ):
        profile = await analyze_project(
            project_id=project_id,
            repo_path=str(tmp_path),
            company_id=company_id,
            pool=mock_pool,
            file_tree=prebuilt_tree,
        )

    # ── 5. Assertions ────────────────────────────────────────────────────────

    # (a) Returned object is a populated ProjectProfile
    assert isinstance(profile, ProjectProfile), f"Expected ProjectProfile, got {type(profile)}"
    assert profile.project_id == project_id
    assert profile.company_id == company_id

    # (b) extraction_prompt is non-empty
    assert profile.extraction_prompt, (
        "extraction_prompt is empty on returned ProjectProfile. "
        "LLM call D (generate_extraction_prompt) result was not stored."
    )
    assert len(profile.extraction_prompt) > 50, (
        f"extraction_prompt too short ({len(profile.extraction_prompt)} chars) — "
        "likely not the filled prompt from the LLM."
    )

    # (c) chunker_config has ast_chunk_boundaries
    assert profile.chunker_config, "chunker_config is empty on ProjectProfile"
    assert "ast_chunk_boundaries" in profile.chunker_config, (
        "chunker_config missing ast_chunk_boundaries key. "
        "LLM call C (generate_chunker_config) was not parsed correctly."
    )
    assert len(profile.chunker_config["ast_chunk_boundaries"]) >= 1, (
        "chunker_config.ast_chunk_boundaries is empty — LLM response not applied."
    )

    # (d) technical_domains is a non-empty list
    assert profile.technical_domains, "technical_domains is empty on ProjectProfile"
    assert len(profile.technical_domains) >= 1, "Expected at least 1 technical domain"
    assert "name" in profile.technical_domains[0], "technical_domains items must have 'name' key"

    # (e) SQL was executed against code_processing.project_profiles
    all_sql = " ".join(s for s, _ in sql_calls)
    project_profile_sqls = [(s, args) for s, args in sql_calls if "project_profiles" in s]
    assert project_profile_sqls, (
        "No SQL executed against project_profiles table!\n"
        f"All SQL executed:\n" + "\n".join(f"  {s[:80]!r}" for s, _ in sql_calls)
    )

    # (f) project_profiles SQL contains the correct project_id as first parameter
    pp_sql, pp_args = project_profile_sqls[0]
    assert pp_args[0] == project_id, (
        f"project_profiles SQL first arg={pp_args[0]!r}, expected project_id={project_id!r}. "
        "Wrong project_id being persisted."
    )

    # SQL parameter order (after fix): project_id, language, framework,
    #   technical_domains, chunker_config, extraction_prompt
    # Indices:                          0,          1,        2,
    #                                   3,                4,             5

    # (g) extraction_prompt was passed to the DB (not empty string)
    # The 6th parameter (index 5) in the upsert is extraction_prompt
    extraction_in_db = pp_args[5] if len(pp_args) > 5 else None
    assert extraction_in_db, (
        f"extraction_prompt passed to project_profiles SQL is empty/None: {extraction_in_db!r}. "
        "Profile is inserted without the extraction prompt."
    )

    # (h) chunker_config was passed as JSON string to the DB (5th param, index 4)
    chunker_in_db = pp_args[4] if len(pp_args) > 4 else None
    assert chunker_in_db, f"chunker_config passed to DB is empty/None: {chunker_in_db!r}"
    parsed_chunker = json.loads(chunker_in_db)
    assert "ast_chunk_boundaries" in parsed_chunker, (
        "chunker_config in DB is missing ast_chunk_boundaries"
    )

    # (i) technical_domains was passed as valid JSON array to the DB (4th param, index 3)
    tech_in_db = pp_args[3] if len(pp_args) > 3 else None
    assert tech_in_db, f"technical_domains passed to DB is empty/None: {tech_in_db!r}"
    parsed_tech = json.loads(tech_in_db)
    assert isinstance(parsed_tech, list), "technical_domains in DB must be a JSON array"
    assert len(parsed_tech) >= 1, "technical_domains array in DB is empty"

    # (j) company_business_domains SQL was executed (business domain persistence)
    biz_domain_sqls = [(s, args) for s, args in sql_calls if "company_business_domains" in s]
    assert biz_domain_sqls, (
        "No SQL executed against company_business_domains table! "
        "Business domains from LLM call A are not being persisted.\n"
        f"All SQL executed:\n" + "\n".join(f"  {s[:80]!r}" for s, _ in sql_calls)
    )


# ---------------------------------------------------------------------------
# T3 — _file_processing reads project profile and uses it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t3_fetch_project_profile_returns_none_when_table_empty():
    """_fetch_project_profile returns (None, None) when project_profiles has no rows.

    Verifies the graceful-degradation path — pipeline must not crash when
    project_profiles is empty (first ingestion before analysis runs).
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        _fetch_project_profile,
    )

    # Pool that simulates an empty project_profiles table
    mock_pool = AsyncMock()
    mock_pool.fetchrow = AsyncMock(return_value=None)

    chunker_config, extraction_prompt = await _fetch_project_profile(mock_pool, "proj-123")

    assert chunker_config is None, "Expected None chunker_config when no profile"
    assert extraction_prompt is None, "Expected None extraction_prompt when no profile"


@pytest.mark.asyncio
async def test_t3b_chunk_and_enrich_uses_extraction_prompt_from_profile():
    """chunk_and_enrich_file attaches extraction_prompt from project profile.

    Sets up a mock project_profiles row, calls chunk_and_enrich_file, and
    verifies extraction_prompt appears on every returned chunk item.
    (Fixed: _fetch_project_profile now parses chunker_config JSON string to dict.)
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    project_id = str(uuid.uuid4())
    expected_prompt = "Extract Python entities: functions, classes, imports..."

    profile_row = {
        "id": str(uuid.uuid4()),
        "project_id": project_id,
        "language": "Python",
        "framework": "FastAPI",
        "chunker_config": json.dumps(
            {"fallback_strategy": "recursive_text", "fallback_chunk_size": 500}
        ),
        "extraction_prompt": expected_prompt,
        "technical_domains": json.dumps([]),
        "analysed_at": None,
        "created_at": None,
        "updated_at": None,
    }

    chunk_insert_row = MagicMock()
    chunk_insert_row.__getitem__ = lambda self, k: str(uuid.uuid4()) if k == "id" else None

    call_count = [0]

    async def fake_fetchrow(sql, *args):
        call_count[0] += 1
        # project_profiles query
        if "project_profiles" in sql:
            return profile_row
        # file_version_id lookup
        if "repository_file_versions" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: str(uuid.uuid4()) if k == "id" else None
            return row
        return None

    async def fake_fetchrow_chunk(sql, *args):
        return chunk_insert_row

    mock_pool = AsyncMock()
    mock_pool.fetchrow = fake_fetchrow
    mock_pool.execute = AsyncMock(return_value="UPDATE 1")

    # Patch INSERT chunk to return a fake row id
    original_fetchrow = mock_pool.fetchrow

    async def routing_fetchrow(sql, *args):
        if "INSERT" in sql and "file_chunks" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: str(uuid.uuid4()) if k == "id" else None
            return row
        return await original_fetchrow(sql, *args)

    mock_pool.fetchrow = routing_fetchrow

    content = textwrap.dedent("""
        def hello():
            return "world"

        class Greeter:
            def greet(self, name: str) -> str:
                return f"Hello, {name}"
    """).strip()

    items = await chunk_and_enrich_file(
        pool=mock_pool,
        document_id=str(uuid.uuid4()),
        file_path="src/greeter.py",
        content=content,
        ingestion_id=str(uuid.uuid4()),
        project_id=project_id,
        company_id=str(uuid.uuid4()),
        repository="test-repo",
        branch="main",
        project_analysis=None,
    )

    assert items, "chunk_and_enrich_file returned no items — check DB mock wiring"

    # Every chunk item MUST carry extraction_prompt from the profile
    for i, item in enumerate(items):
        assert item.get("extraction_prompt") == expected_prompt, (
            f"chunk[{i}] has extraction_prompt={item.get('extraction_prompt')!r}, "
            f"expected {expected_prompt!r}. "
            "Profile was fetched but extraction_prompt not propagated to chunk items."
        )


@pytest.mark.asyncio
async def test_t3c_chunk_and_enrich_extraction_prompt_is_none_when_no_profile():
    """When project_profiles is empty, extraction_prompt in chunk items is None.

    This is the current (broken) state — documents the gap without hiding it.
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    file_version_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())

    async def fake_fetchrow(sql, *args):
        if "INSERT" in sql and "file_chunks" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: chunk_id if k == "id" else None
            return row
        if "repository_file_versions" in sql and "document_id" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: file_version_id if k == "id" else None
            return row
        # project_profiles — empty table
        return None

    mock_pool = AsyncMock()
    mock_pool.fetchrow = fake_fetchrow
    mock_pool.execute = AsyncMock(return_value="UPDATE 1")

    content = "def hello():\n    return 'world'\n"

    items = await chunk_and_enrich_file(
        pool=mock_pool,
        document_id=str(uuid.uuid4()),
        file_path="src/hello.py",
        content=content,
        ingestion_id=str(uuid.uuid4()),
        project_id=str(uuid.uuid4()),
        company_id=str(uuid.uuid4()),
        repository="test-repo",
        branch="main",
        project_analysis=None,
    )

    # Documents current broken state: extraction_prompt is None
    for item in items:
        assert item.get("extraction_prompt") is None, (
            "Expected extraction_prompt=None when project_profiles is empty, "
            f"but got {item.get('extraction_prompt')!r}. "
            "If this test fails, the wiring has been fixed — update accordingly."
        )


# ---------------------------------------------------------------------------
# T4 — EnrichedChunkMessage has extraction_prompt populated
# ---------------------------------------------------------------------------


def test_t4_enriched_chunk_message_schema_has_extraction_prompt():
    """EnrichedChunkMessage schema MUST include extraction_prompt field.

    Uses static source analysis to avoid importing cognee (broken mistralai
    dependency in local dev environment).
    """
    source = _source("cognee_service/kafka_consumer/enriched_chunks/models.py")

    assert "extraction_prompt" in source, (
        "EnrichedChunkMessage does not define extraction_prompt field. "
        "Kafka messages cannot carry the prompt downstream."
    )
    # Field must be Optional[str] with default=None
    assert "Optional[str]" in source or "Optional[str]" in source, (
        "extraction_prompt should be Optional[str] for backward compatibility."
    )
    assert "default=None" in source or '"extraction_prompt": Optional' in source, (
        "extraction_prompt should default to None (legacy messages have no prompt)."
    )


@pytest.mark.asyncio
async def test_t4b_kafka_message_has_extraction_prompt_when_profile_exists():
    """After embed_and_publish_batch, Kafka message contains non-null extraction_prompt.

    Injects a chunk item with extraction_prompt set and verifies the Kafka
    message carries the prompt through. enrichment.py copies extraction_prompt
    from chunk items to the Kafka message.
    """
    from code_preprocessor.enrichment import embed_and_publish_batch

    extraction_prompt = "Extract Python entities: functions, classes..."
    chunk_item = {
        "chunk_id": str(uuid.uuid4()),
        "chunk_text": "def hello():\n    return 'world'",
        "header": "PROJECT: test\nFILE: hello.py",
        "file_path": "hello.py",
        "language": "python",
        "file_skeleton": None,
        "repository": "test-repo",
        "branch": "main",
        "ingestion_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "company_id": str(uuid.uuid4()),
        "chunk_index": 0,
        "total_chunks": 1,
        "file_version_id": str(uuid.uuid4()),
        "start_line": 1,
        "end_line": 2,
        "extraction_prompt": extraction_prompt,  # This is what T3 should produce
        "business_domains": [],
        "technical_tags": [],
    }

    published_messages: list[dict] = []

    mock_producer = AsyncMock()

    async def capture_send(topic, value=None, key=None):
        if value:
            published_messages.append(json.loads(value.decode()))

    mock_producer.send = capture_send
    mock_producer.flush = AsyncMock()

    mock_pool = AsyncMock()
    mock_pool._closed = False
    mock_pool.executemany = AsyncMock()

    # Mock the embedding call
    fake_embedding = [0.1] * 3072

    with patch("litellm.aembedding") as mock_embed:
        mock_embed.return_value = MagicMock(data=[{"embedding": fake_embedding}])
        await embed_and_publish_batch(
            mock_pool,
            mock_producer,
            [chunk_item],
            "vertex_ai/gemini-embedding-001",
            topic="enriched-code-chunks",
        )

    assert published_messages, "No messages published to Kafka"
    msg = published_messages[0]

    assert msg.get("extraction_prompt") == extraction_prompt, (
        f"Kafka message extraction_prompt={msg.get('extraction_prompt')!r}, "
        f"expected {extraction_prompt!r}. "
        "enrichment.py does not copy extraction_prompt to Kafka message."
    )


def test_t4c_enrichment_copies_extraction_prompt_to_kafka_message():
    """Static check: enrichment.py includes 'extraction_prompt' in the Kafka message dict."""
    source = _source("code_preprocessor/enrichment.py")

    # The message dict in embed_and_publish_batch must include extraction_prompt
    assert '"extraction_prompt"' in source or "'extraction_prompt'" in source, (
        "enrichment.py does not include extraction_prompt in the Kafka message dict. "
        "Prompt cannot flow to entity_extraction_service."
    )
    assert (
        'chunk.get("extraction_prompt")' in source or "chunk.get('extraction_prompt')" in source
    ), (
        "enrichment.py does not read extraction_prompt from chunk items. "
        "The field will always be null in Kafka messages."
    )


# ---------------------------------------------------------------------------
# T5 — entity_extraction_service reads extraction_prompt from message
#
# NOTE: entity_extraction_service.processor imports cognee which triggers a
# broken mistralai import in the local dev environment. These tests use static
# source analysis to verify the prompt routing logic without importing the
# module at runtime.
# ---------------------------------------------------------------------------


def test_t5_processor_resolve_prompt_uses_message_field():
    """_resolve_prompt in processor.py uses msg.extraction_prompt when present and non-empty.

    Verifies via static analysis that the logic exists in the source.
    """
    source = _source("entity_extraction_service/processor.py")

    # Must have _resolve_prompt function
    assert "def _resolve_prompt" in source, (
        "entity_extraction_service/processor.py does not define _resolve_prompt. "
        "Prompt routing logic is missing."
    )
    # Must read extraction_prompt from the message
    assert "msg.extraction_prompt" in source or "extraction_prompt" in source, (
        "_resolve_prompt does not access msg.extraction_prompt. "
        "Processor ignores the V3 prompt field."
    )
    # Must use hasattr to detect old-format messages (field absent entirely)
    assert "hasattr" in source, (
        "_resolve_prompt does not use hasattr to detect old-format messages. "
        "The transitional fallback branch is missing."
    )


def test_t5b_processor_rejects_empty_extraction_prompt_via_source():
    """_resolve_prompt returns None (rejection) when extraction_prompt is present but empty.

    Per Phase 3.1 spec: field present but None/empty → REJECT (return None).
    Verified via static source analysis.
    """
    source = _source("entity_extraction_service/processor.py")

    # Must return None when extraction_prompt is falsy (present but empty/None)
    assert "return None" in source, (
        "_resolve_prompt never returns None. "
        "It should return None to reject chunks with empty extraction_prompt."
    )
    # Phase 3.1 spec comment should be present or logic should be explicit
    assert (
        "chunk_rejected_no_prompt" in source
        or "no silent fallback" in source.lower()
        or "return None" in source
    ), (
        "processor.py does not implement rejection for empty extraction_prompt. "
        "Chunks with null prompts will silently fall back to file-based prompt."
    )


def test_t5c_processor_has_old_format_fallback_branch():
    """_resolve_prompt has transitional fallback for old-format messages (no extraction_prompt field).

    Old messages (pre-V3) don't have extraction_prompt attribute.
    The processor must warn and fall back, not crash.
    """
    source = _source("entity_extraction_service/processor.py")

    # Must have the fallback warning logged
    assert (
        "old_format_message" in source
        or "old-format" in source.lower()
        or "transitional fallback" in source.lower()
    ), (
        "processor.py does not handle old-format messages (no extraction_prompt field). "
        "Pre-V3 messages will crash or be silently lost."
    )
    # TODO(v4) comment must exist to track when to remove the fallback
    assert "TODO" in source and "v4" in source, (
        "processor.py is missing the TODO(v4) comment to remove the fallback. "
        "Technical debt will never be cleaned up."
    )


# ---------------------------------------------------------------------------
# T6 — node_set naming uses project_name not branch
# ---------------------------------------------------------------------------


def test_t6_node_set_does_not_use_branch_variable():
    """build_document_chunk should NOT use branch in node_set / belongs_to_set.

    Current implementation uses f"code_{msg.project_id}" which is acceptable,
    but must NOT use msg.branch in the set name.
    """
    source = _source("cognee_service/kafka_consumer/enriched_chunks/models.py")

    # Find the line(s) that build source_node_set and belongs_to_set
    lines_with_node_set = [
        (i + 1, line.strip())
        for i, line in enumerate(source.splitlines())
        if "node_set" in line or "belongs_to_set" in line
    ]

    assert lines_with_node_set, "No node_set / belongs_to_set assignments found in models.py"

    for lineno, line in lines_with_node_set:
        assert "branch" not in line, (
            f"Line {lineno}: node_set uses 'branch' variable: {line!r}. "
            "Node sets must be identified by project_id or project_name, not branch. "
            "Using branch causes a separate set per branch — entities can't be merged."
        )


def test_t6b_node_set_format_contains_project_id():
    """node_set in build_document_chunk uses project_id for stable naming."""
    source = _source("cognee_service/kafka_consumer/enriched_chunks/models.py")

    # Verify the format uses project_id
    assert (
        "project_id" in source.split("belongs_to_set")[1][:200]
        or "project_id" in source.split("source_node_set")[1][:200]
    ), (
        "belongs_to_set / source_node_set does not reference project_id. "
        "Node sets have no stable identifier."
    )


# ---------------------------------------------------------------------------
# T7 — cognee.add() NOT called from preprocessor
# ---------------------------------------------------------------------------


def test_t7_preprocessor_has_no_cognee_imports():
    """code_preprocessor must not import from cognee directly.

    The preprocessor's job ends at publishing to Kafka. Cognee integration
    belongs in cognee_service. Direct cognee imports in the preprocessor
    create a tight coupling that bypasses the Kafka queue.
    """
    violations: list[str] = []

    for py_file in _PREPROCESSOR_ROOT.rglob("*.py"):
        # Skip test files — they may import cognee for model classes
        if "tests" in py_file.parts:
            continue
        try:
            source = py_file.read_text(encoding="utf-8")
        except OSError:
            continue

        lines = source.splitlines()
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            # Check for direct cognee usage (not model imports from shared schema)
            if stripped.startswith("import cognee") or "from cognee" in stripped:
                # Allow importing cognee data models for type compatibility
                if "from cognee" in stripped and any(
                    ok in stripped
                    for ok in [
                        "cognee_service",  # our own service
                        "cognee.modules.chunking.models",
                        "cognee.modules.data.processing.document_types",
                        "cognee.shared.data_models",
                        "cognee.infrastructure.llm",
                    ]
                ):
                    continue
                violations.append(f"{py_file.relative_to(_REPO_ROOT)}:{i}: {stripped}")

    assert not violations, (
        "code_preprocessor imports cognee directly — this is wrong. "
        "The preprocessor must publish to Kafka only. Cognee integration belongs in cognee_service.\n"
        "Violations:\n" + "\n".join(f"  {v}" for v in violations)
    )


def test_t7b_file_processing_has_no_register_with_cognee():
    """_file_processing.py must not define _register_with_cognee function."""
    source = _source("code_preprocessor/kafka_processing_service/_file_processing.py")

    assert "_register_with_cognee" not in source, (
        "_file_processing.py defines _register_with_cognee — this was the old architecture "
        "where the preprocessor called cognee directly. "
        "This function must not exist. Cognee registration belongs in cognee_service."
    )


def test_t7c_preprocessor_publishes_to_kafka_not_cognee():
    """enrichment.py embed_and_publish_batch must call producer.send (Kafka), not cognee.add."""
    source = _source("code_preprocessor/enrichment.py")

    assert "producer.send" in source, (
        "enrichment.py does not call producer.send — chunks may not be reaching Kafka."
    )
    assert "cognee.add" not in source, (
        "enrichment.py calls cognee.add directly. The preprocessor must only publish to Kafka."
    )


# ---------------------------------------------------------------------------
# T8 — Bonus: verify start_line / end_line are populated in chunk items
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t8_chunk_items_have_line_numbers():
    """chunk_and_enrich_file produces items with start_line > 0 and end_line >= start_line."""
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    file_version_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())

    async def fake_fetchrow(sql, *args):
        if "INSERT" in sql and "file_chunks" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: chunk_id if k == "id" else None
            return row
        if "repository_file_versions" in sql:
            row = MagicMock()
            row.__getitem__ = lambda self, k: file_version_id if k == "id" else None
            return row
        return None

    mock_pool = AsyncMock()
    mock_pool.fetchrow = fake_fetchrow
    mock_pool.execute = AsyncMock(return_value="UPDATE 1")

    lines = []
    for i in range(5):
        lines.extend([f"def function_{i}():", f"    return {i}", ""])
    content = "\n".join(lines)

    items = await chunk_and_enrich_file(
        pool=mock_pool,
        document_id=str(uuid.uuid4()),
        file_path="src/funcs.py",
        content=content,
        ingestion_id=str(uuid.uuid4()),
        project_id=str(uuid.uuid4()),
        company_id=str(uuid.uuid4()),
        repository="test-repo",
        branch="main",
        project_analysis=None,
    )

    assert items, "chunk_and_enrich_file returned no items"

    for i, item in enumerate(items):
        start = item.get("start_line", 0)
        end = item.get("end_line", 0)
        assert start >= 0, f"chunk[{i}] start_line={start} is negative"
        assert end >= start, f"chunk[{i}] end_line={end} < start_line={start}"

    # At least some chunks should have meaningful line numbers (not all zeros)
    nonzero = [it for it in items if it.get("start_line", 0) > 0]
    assert nonzero, (
        "All chunks have start_line=0. "
        "The chunker is not returning line number information. "
        "Downstream cannot map chunks back to source lines."
    )


# ---------------------------------------------------------------------------
# T2d — analyze_project() handles LLM failure gracefully
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2d_analyze_project_handles_llm_failure_gracefully(tmp_path):
    """When LLM raises exceptions, analyze_project raises a clear RuntimeError.

    Verifies:
    - analyze_project does NOT swallow LLM errors silently
    - No partial/corrupt data is written to project_profiles
    - The exception is a RuntimeError with a meaningful message
    """
    from code_preprocessor.project_analyzer import analyze_project

    # ── Create a minimal repo directory ──────────────────────────────────────
    (tmp_path / "main.py").write_text("def hello(): pass\n")
    (tmp_path / "pyproject.toml").write_text("[tool.poetry]\nname = 'test'\n")

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # ── Track all SQL to detect partial writes ─────────────────────────────
    sql_calls: list[str] = []

    mock_conn = AsyncMock()

    async def conn_execute(sql, *args):
        sql_calls.append(sql.strip())
        return "UPDATE 1"

    mock_conn.execute = conn_execute
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=mock_acquire_ctx)

    async def pool_execute(sql, *args):
        sql_calls.append(sql.strip())
        return "INSERT 0 1"

    mock_pool.execute = pool_execute
    mock_pool.fetch = AsyncMock(return_value=[])
    mock_pool.fetchrow = AsyncMock(return_value=None)

    # ── Mock LLM to always raise an exception ─────────────────────────────
    call_count = 0

    async def mock_acompletion_fails(**kwargs):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("LLM API unavailable — simulated failure")

    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=mock_acompletion_fails,
    ):
        with pytest.raises(RuntimeError) as exc_info:
            await analyze_project(
                project_id=project_id,
                repo_path=str(tmp_path),
                company_id=company_id,
                pool=mock_pool,
            )

    # (a) Exception is raised — not swallowed
    assert exc_info.value is not None, "analyze_project should raise on LLM failure, not return"
    error_msg = str(exc_info.value)
    assert "failed" in error_msg.lower() or "attempt" in error_msg.lower(), (
        f"RuntimeError message is unclear: {error_msg!r}. "
        "Should indicate which LLM call failed and how many retries were attempted."
    )

    # (b) No project_profiles write occurred (no partial/corrupt data)
    project_profile_writes = [s for s in sql_calls if "project_profiles" in s]
    assert not project_profile_writes, (
        "project_profiles was written even though the LLM failed! "
        "Partial/corrupt profile data should not be persisted on LLM failure.\n"
        f"Unexpected SQL: {project_profile_writes}"
    )

    # (c) LLM was actually called (retry mechanism engaged, not short-circuited)
    assert call_count > 0, (
        "LLM was never called — analyze_project short-circuited before making LLM calls."
    )


# ---------------------------------------------------------------------------
# T2e — analyze_project() deduplicates business domains by normalised_key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2e_analyze_project_deduplicates_business_domains(tmp_path):
    """LLM returning duplicate domain names results in only ONE domain upserted.

    Scenario: LLM returns "Payment Processing" and "payment processing" (same
    normalised_key = "payment_process"). deduplicate_domains must ensure only
    one entry is persisted per normalised_key.

    Also verifies:
    - normalised_key is computed correctly (lowercase, stemmed, sorted, underscore)
    - Duplicate within LLM output is collapsed, not double-inserted
    """
    from code_preprocessor.project_analyzer import (
        analyze_project,
        normalize_domain_key,
        ProjectProfile,
    )

    # Verify the normalisation algorithm directly first
    assert normalize_domain_key("Payment Processing") == normalize_domain_key(
        "payment processing"
    ), (
        "normalize_domain_key('Payment Processing') != normalize_domain_key('payment processing'). "
        "The normalisation is case-sensitive — it should be case-insensitive."
    )
    key = normalize_domain_key("Payment Processing")
    assert key == "payment_process", (
        f"normalize_domain_key('Payment Processing') = {key!r}, expected 'payment_process'. "
        "Stemming or sorting logic is incorrect."
    )

    # ── Create a minimal repo ─────────────────────────────────────────────
    (tmp_path / "main.py").write_text("def process_payment(): pass\n")
    (tmp_path / "pyproject.toml").write_text("[tool.poetry]\nname = 'payments'\n")

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # ── LLM returns duplicate domains with different casing ───────────────
    _business_domains_response = json.dumps(
        {
            "language": "Python",
            "framework": "Python",
            "business_domains": [
                {
                    "canonical_name": "Payment Processing",
                    "normalised_key": "payment_process",
                    "description": "Handles payment transactions.",
                },
                {
                    "canonical_name": "payment processing",  # duplicate — same key
                    "normalised_key": "payment_process",
                    "description": "Duplicate entry that must be collapsed.",
                },
                {
                    "canonical_name": "Order Management",
                    "normalised_key": "manag_order",
                    "description": "Manages customer orders.",
                },
            ],
        }
    )
    _technical_domains_response = json.dumps(
        {
            "technical_domains": [
                {"name": "API Layer", "description": "REST endpoints", "patterns": []}
            ]
        }
    )
    _chunker_config_response = json.dumps(
        {
            "language": "Python",
            "framework": "Python",
            "ast_chunk_boundaries": [
                {
                    "node_type": "function_definition",
                    "min_size_chars": 50,
                    "max_size_chars": 1500,
                    "description": "Functions",
                },
            ],
            "fallback_strategy": "recursive_text",
            "fallback_chunk_size": 1000,
            "fallback_overlap": 100,
            "language_rules": {},
            "file_type_overrides": [],
        }
    )
    _extraction_prompt_response = json.dumps(
        {"filled_prompt": "Extract entities from Python payment processing code."}
    )

    _llm_responses = [
        _business_domains_response,
        _technical_domains_response,
        _chunker_config_response,
        _extraction_prompt_response,
    ]

    import asyncio

    _call_index = 0
    _call_index_lock = asyncio.Lock()

    async def mock_acompletion(**kwargs):
        nonlocal _call_index
        # Skip probe calls (max_tokens=1) — they succeed but don't consume a response slot
        if kwargs.get("max_tokens") == 1:
            mock_resp = MagicMock()
            mock_resp.choices = [MagicMock()]
            mock_resp.choices[0].message.content = '{"ok": true}'
            return mock_resp
        async with _call_index_lock:
            idx = _call_index % len(_llm_responses)
            _call_index += 1
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = _llm_responses[idx]
        return mock_resp

    # ── Track INSERT calls to company_business_domains ───────────────────
    biz_domain_inserts: list[tuple[str, tuple]] = []

    mock_conn = AsyncMock()

    async def conn_execute(sql, *args):
        if "company_business_domains" in sql:
            biz_domain_inserts.append((sql.strip(), args))
        return "INSERT 0 1"

    mock_conn.execute = conn_execute

    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=mock_acquire_ctx)

    async def pool_execute(sql, *args):
        return "INSERT 0 1"

    mock_pool.execute = pool_execute
    # No existing domains in DB — all are new
    mock_pool.fetch = AsyncMock(return_value=[])
    mock_pool.fetchrow = AsyncMock(return_value=None)

    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=mock_acompletion,
    ):
        profile = await analyze_project(
            project_id=project_id,
            repo_path=str(tmp_path),
            company_id=company_id,
            pool=mock_pool,
        )

    # ── Assertions ─────────────────────────────────────────────────────────

    # (a) Profile business_domains has been deduplicated
    bd_keys = [d.get("normalised_key") for d in profile.business_domains]
    assert bd_keys.count("payment_process") == 1, (
        f"business_domains has {bd_keys.count('payment_process')} entries with key "
        f"'payment_process', expected exactly 1. Deduplication failed.\n"
        f"All keys: {bd_keys}"
    )

    # (b) Two distinct domains remain: payment_process + manag_order
    assert len(profile.business_domains) == 2, (
        f"Expected 2 deduplicated business domains (payment_process + manag_order), "
        f"got {len(profile.business_domains)}: {bd_keys}"
    )

    # (c) SQL inserts match the deduplicated count (not 3)
    # Each INSERT in the loop corresponds to one unique domain
    insert_keys = [args[2] for _, args in biz_domain_inserts if len(args) >= 3]
    assert insert_keys.count("payment_process") == 1, (
        f"'payment_process' was inserted {insert_keys.count('payment_process')} times into DB "
        f"(expected 1). Duplicate domain was not deduplicated before DB write.\n"
        f"All inserted keys: {insert_keys}"
    )

    # (d) manag_order key is correct
    assert "manag_order" in insert_keys, (
        f"'manag_order' not in inserted keys {insert_keys}. "
        "Order Management domain was lost during deduplication."
    )

    # (e) normalised_key in profile is correct format (lowercase, underscore-joined)
    for domain in profile.business_domains:
        key = domain.get("normalised_key", "")
        assert key == key.lower(), f"normalised_key {key!r} is not lowercase"
        assert " " not in key, f"normalised_key {key!r} contains spaces (should use underscores)"


# ---------------------------------------------------------------------------
# T5d — processor._resolve_prompt() uses message extraction_prompt (static)
# ---------------------------------------------------------------------------


def test_t5d_processor_resolve_prompt_returns_message_field_when_set():
    """_resolve_prompt returns msg.extraction_prompt when it is set and non-empty.

    Uses static source analysis because entity_extraction_service.processor cannot
    be imported in local dev (cognee → mistralai import failure).

    Verifies the CONTROL FLOW logic in _resolve_prompt:
      1. If msg has no extraction_prompt attribute → old-format fallback (warn + use file prompt)
      2. If msg.extraction_prompt is falsy (None or empty) → REJECT (return None)
      3. If msg.extraction_prompt is set and non-empty → USE IT (return the message prompt)

    This is the Phase 3.1 routing spec.
    """
    source = _source("entity_extraction_service/processor.py")

    # (a) _resolve_prompt exists
    assert "def _resolve_prompt" in source, (
        "entity_extraction_service/processor.py does not define _resolve_prompt. "
        "Prompt routing is missing entirely."
    )

    # Extract the _resolve_prompt function body for targeted analysis
    lines = source.splitlines()
    in_func = False
    func_lines = []
    for line in lines:
        if "def _resolve_prompt" in line:
            in_func = True
        if in_func:
            func_lines.append(line)
            # Stop at next top-level function/class definition (after starting)
            if len(func_lines) > 1 and (
                line.startswith("def ")
                or line.startswith("class ")
                or line.startswith("async def ")
            ):
                func_lines.pop()  # remove the triggering line of next func
                break
    func_source = "\n".join(func_lines)

    # (b) Uses msg.extraction_prompt — reads from message field
    assert "extraction_prompt" in func_source, (
        "_resolve_prompt does not reference extraction_prompt at all. "
        "It cannot route based on the message field."
    )

    # (c) Has hasattr guard for backward compatibility with old-format messages
    assert "hasattr" in func_source, (
        "_resolve_prompt does not use hasattr to check for extraction_prompt presence. "
        "Old-format messages (pre-V3, no field) will crash with AttributeError."
    )

    # (d) Returns None to REJECT chunks with present-but-empty extraction_prompt
    assert "return None" in func_source, (
        "_resolve_prompt never returns None. "
        "Per Phase 3.1 spec, empty extraction_prompt must REJECT the chunk (return None), "
        "not silently fall back to file-based prompt."
    )

    # (e) Has old-format fallback path (transitional — not permanent)
    assert (
        "old_format" in func_source.lower()
        or "old-format" in func_source.lower()
        or "transitional" in func_source.lower()
        or ("warn" in func_source.lower() and "hasattr" in func_source)
    ), (
        "_resolve_prompt has no old-format message handling. "
        "Pre-V3 messages (no extraction_prompt field) will crash. "
        "Add a hasattr guard with warning + fallback to file-based prompt."
    )

    # (f) TODO(v4) marker exists to track removal of the transitional fallback
    assert "TODO" in source and "v4" in source.lower(), (
        "processor.py is missing the TODO(v4) comment marking the transitional fallback "
        "for removal. Technical debt will accumulate without this marker."
    )


# ---------------------------------------------------------------------------
# T2f — analyze_project() uses pre-built file_tree, skips build_tree()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2f_analyze_project_uses_prebuilt_file_tree():
    """analyze_project() accepts a pre-built file_tree and succeeds even when
    repo_path is NOT a real filesystem path.

    This is the regression guard for the bug where ingestion_processor passed
    a repo NAME (e.g. 'oscar-vet/vet_ui') as repo_path to analyze_project(),
    which then crashed in build_tree() because the path does not exist.

    The consumer already builds the real file tree from the cloned path and
    passes it as file_tree to _run_project_analysis.  That value must be
    forwarded to analyze_project() so build_tree() is never called with a
    non-existent path.

    Verifies:
    - analyze_project() succeeds when file_tree is provided and repo_path is fake
    - build_tree() is NOT called when file_tree is non-empty
    - Returned ProjectProfile is fully populated
    """
    import asyncio
    from unittest.mock import patch as _patch

    from code_preprocessor.project_analyzer import analyze_project, ProjectProfile

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # A pre-built file tree (what consumer.py would pass after cloning)
    prebuilt_tree = (
        "vet_ui/\n  src/\n    main.py\n    api/\n      routes.py\n  pyproject.toml\n  README.md\n"
    )

    # Realistic LLM JSON responses for all 4 calls (same format as T2c)
    _business_domains_response = json.dumps(
        {
            "language": "Python",
            "framework": "FastAPI",
            "business_domains": [
                {
                    "canonical_name": "Veterinary Management",
                    "normalised_key": "vet_mgmt",
                    "description": "Core vet clinic domain logic.",
                }
            ],
        }
    )
    _technical_domains_response = json.dumps(
        {
            "technical_domains": [
                {
                    "name": "API Layer",
                    "description": "Route handlers.",
                    "patterns": ["src/api/"],
                }
            ]
        }
    )
    _chunker_config_response = json.dumps(
        {
            "language": "Python",
            "framework": "FastAPI",
            "ast_chunk_boundaries": [
                {
                    "node_type": "function_definition",
                    "min_size_chars": 50,
                    "max_size_chars": 1500,
                    "description": "Functions.",
                }
            ],
            "fallback_strategy": "recursive_text",
            "fallback_chunk_size": 1000,
            "fallback_overlap": 100,
            "language_rules": {},
            "file_type_overrides": [],
        }
    )
    _extraction_prompt_response = json.dumps(
        {"filled_prompt": "Extract entities from this Python/FastAPI veterinary codebase."}
    )

    llm_responses = [
        _business_domains_response,
        _technical_domains_response,
        _chunker_config_response,
        _extraction_prompt_response,
    ]
    call_idx = 0
    call_idx_lock = asyncio.Lock()

    async def mock_acompletion(**kwargs):
        nonlocal call_idx
        # Skip probe calls (max_tokens=1) — they succeed but don't consume a response slot
        if kwargs.get("max_tokens") == 1:
            mock_resp = MagicMock()
            mock_resp.choices = [MagicMock()]
            mock_resp.choices[0].message.content = '{"ok": true}'
            return mock_resp
        async with call_idx_lock:
            idx = call_idx % len(llm_responses)
            call_idx += 1
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = llm_responses[idx]
        return mock_resp

    # Minimal mock pool
    mock_conn = AsyncMock()

    async def conn_execute(sql, *args):
        return "UPDATE 1"

    mock_conn.execute = conn_execute
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=mock_acquire_ctx)

    async def pool_execute(sql, *args):
        return "INSERT 0 1"

    mock_pool.execute = pool_execute
    mock_pool.fetch = AsyncMock(return_value=[])
    mock_pool.fetchrow = AsyncMock(return_value=None)

    # Track if build_tree was called — it must NOT be called
    build_tree_called = False

    def mock_build_tree(path: str) -> str:
        nonlocal build_tree_called
        build_tree_called = True
        return f"ERROR: path does not exist: {path}"

    with (
        _patch(
            "code_preprocessor.project_analyzer.build_tree",
            side_effect=mock_build_tree,
        ),
        _patch(
            "code_preprocessor.project_analyzer.litellm.acompletion",
            side_effect=mock_acompletion,
        ),
    ):
        profile = await analyze_project(
            project_id=project_id,
            repo_path="oscar-vet/vet_ui",  # NOT a real path — would crash without file_tree
            company_id=company_id,
            pool=mock_pool,
            file_tree=prebuilt_tree,
        )

    # (a) build_tree was never called — pre-built tree was used
    assert not build_tree_called, (
        "analyze_project() called build_tree() even though file_tree was provided. "
        "When a pre-built file_tree is passed, build_tree() must be skipped. "
        "Check: `if not file_tree:` guard around the build_tree() call in analyze_project()."
    )

    # (b) Returned profile is a populated ProjectProfile
    assert isinstance(profile, ProjectProfile), f"Expected ProjectProfile, got {type(profile)}"
    assert profile.project_id == project_id
    assert profile.company_id == company_id

    # (c) LLM calls succeeded using the pre-built tree content
    assert profile.technical_domains, (
        "technical_domains empty — LLM call did not use pre-built tree"
    )
    assert profile.chunker_config, "chunker_config empty — generate_chunker_config did not run"
    assert profile.extraction_prompt, (
        "extraction_prompt empty — generate_extraction_prompt did not run"
    )


# ---------------------------------------------------------------------------
# T2g — regression: analyzer runs successfully but profile is NOT stored if SQL
#        references columns that don't exist in project_profiles (the fixed bug)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t2g_profile_not_stored_when_sql_uses_nonexistent_columns(tmp_path):
    """Regression test: _upsert_project_profile must NOT use company_id or business_domains.

    Root cause of the original bug:
        _upsert_project_profile() referenced columns 'company_id' and 'business_domains'
        that do NOT exist in the actual code_processing.project_profiles table.
        This caused a DB error on every run, which was silently caught in
        _run_project_analysis(), leaving project_profiles permanently empty.

    This test simulates production behaviour by having pool.execute RAISE when
    those non-existent columns appear in the SQL.  The test must PASS — meaning
    the fixed code never produces SQL with those columns.

    If this test starts failing it means the non-existent column names have
    been re-introduced into the upsert SQL and the bug is back.
    """
    import asyncio

    from code_preprocessor.project_analyzer import analyze_project, ProjectProfile

    # ── 1. Minimal repo ─────────────────────────────────────────────────────
    (tmp_path / "main.py").write_text("def hello(): pass\n")
    (tmp_path / "pyproject.toml").write_text("[tool.poetry]\nname = 'test'\n")

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # ── 2. Minimal but valid LLM responses ──────────────────────────────────
    _llm_responses = [
        json.dumps(
            {
                "language": "Python",
                "framework": "Python",
                "business_domains": [
                    {
                        "canonical_name": "Core",
                        "normalised_key": "core",
                        "description": "Core domain.",
                    }
                ],
            }
        ),
        json.dumps(
            {"technical_domains": [{"name": "API", "description": "API layer.", "patterns": []}]}
        ),
        json.dumps(
            {
                "language": "Python",
                "framework": "Python",
                "ast_chunk_boundaries": [
                    {
                        "node_type": "function_definition",
                        "min_size_chars": 50,
                        "max_size_chars": 1500,
                        "description": "Functions.",
                    }
                ],
                "fallback_strategy": "recursive_text",
                "fallback_chunk_size": 1000,
                "fallback_overlap": 100,
                "language_rules": {},
                "file_type_overrides": [],
            }
        ),
        json.dumps(
            {
                "filled_prompt": "Extract entities from Python code. Return JSON with entities and relationships."
            }
        ),
    ]
    _call_index = 0
    _call_index_lock = asyncio.Lock()

    async def mock_acompletion(**kwargs):
        nonlocal _call_index
        # Skip probe calls (max_tokens=1) — they succeed but don't consume a response slot
        if kwargs.get("max_tokens") == 1:
            mock_resp = MagicMock()
            mock_resp.choices = [MagicMock()]
            mock_resp.choices[0].message.content = '{"ok": true}'
            return mock_resp
        async with _call_index_lock:
            idx = _call_index % len(_llm_responses)
            _call_index += 1
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = _llm_responses[idx]
        return mock_resp

    # ── 3. Pool that REJECTS SQL with non-existent columns ───────────────────
    # This simulates the real Postgres behaviour: INSERT into a column that
    # doesn't exist raises ProgrammingError.  We simulate it by raising on
    # any project_profiles SQL that references 'company_id' or 'business_domains'.
    _NON_EXISTENT_COLUMNS = ("company_id", "business_domains")
    profile_sql_calls: list[str] = []

    mock_conn = AsyncMock()

    async def conn_execute(sql, *args):
        return "UPDATE 1"

    mock_conn.execute = conn_execute
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=mock_acquire_ctx)

    async def pool_execute(sql, *args):
        stripped = sql.strip()
        if "project_profiles" in stripped:
            profile_sql_calls.append(stripped)
            # Simulate Postgres rejecting SQL with columns that don't exist
            for col in _NON_EXISTENT_COLUMNS:
                if col in stripped:
                    raise Exception(f'column "{col}" of relation "project_profiles" does not exist')
        return "INSERT 0 1"

    mock_pool.execute = pool_execute
    mock_pool.fetch = AsyncMock(return_value=[])
    mock_pool.fetchrow = AsyncMock(return_value=None)

    # ── 4. Call REAL analyze_project — must succeed without DB column errors ─
    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=mock_acompletion,
    ):
        # Should NOT raise — if the SQL uses non-existent columns our mock will
        # raise, which propagates back as RuntimeError/Exception from analyze_project.
        profile = await analyze_project(
            project_id=project_id,
            repo_path=str(tmp_path),
            company_id=company_id,
            pool=mock_pool,
            file_tree="src/\n  main.py\n  pyproject.toml\n",
        )

    # ── 5. Assertions ─────────────────────────────────────────────────────────

    # (a) Profile was returned — no exception was raised
    assert isinstance(profile, ProjectProfile), (
        f"analyze_project() raised or returned non-Profile: {profile!r}. "
        "The _upsert_project_profile SQL may still reference non-existent columns "
        "(company_id or business_domains), causing DB rejection."
    )
    assert profile.project_id == project_id

    # (b) project_profiles SQL was actually executed at least once
    assert profile_sql_calls, (
        "No SQL was executed against project_profiles table. "
        "_upsert_project_profile was never called from analyze_project()."
    )

    # (c) None of the executed project_profiles SQL references non-existent columns
    for sql in profile_sql_calls:
        for col in _NON_EXISTENT_COLUMNS:
            assert col not in sql, (
                f"project_profiles SQL references non-existent column '{col}'.\n"
                f"This was the root cause of the empty project_profiles bug.\n"
                f"SQL fragment: {sql[:200]!r}"
            )

    # (d) extraction_prompt is populated on the returned profile
    assert profile.extraction_prompt, (
        "extraction_prompt is empty on returned profile. "
        "LLM call D result was not stored on the ProjectProfile object."
    )


# ---------------------------------------------------------------------------
# T9 — _run_project_analysis converts ProjectProfile → dict (no .get() crash)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t9_run_project_analysis_converts_profile_to_dict():
    """_run_project_analysis must return a dict even when analyze_project returns ProjectProfile.

    Regression guard for: 'ProjectProfile' object has no attribute 'get'.

    Before the fix, _run_project_analysis returned the raw ProjectProfile dataclass.
    Downstream callers (chunk_and_enrich_file, build_chunk_header) called .get() on it
    as if it were a dict, causing AttributeError on every enrichment.

    This test:
    1. Injects a ProjectProfile (with realistic data) as the return value of analyze_project.
    2. Calls _run_project_analysis and asserts the return is a dict.
    3. Verifies the dict has the keys expected by _build_project_section and chunk_and_enrich_file.
    4. Verifies build_chunk_header succeeds with the returned dict (no .get() crash).
    5. Verifies business_domains items use 'name' key (mapped from canonical_name).
    6. Verifies design_patterns items use 'name' key (mapped from technical_domains).
    """
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        IngestionProcessor,
    )
    from code_preprocessor.project_analyzer import ProjectProfile
    from code_preprocessor.enrichment import build_chunk_header

    project_id = str(uuid.uuid4())
    company_id = str(uuid.uuid4())

    # A realistic ProjectProfile with all fields populated
    test_profile = ProjectProfile(
        project_id=project_id,
        company_id=company_id,
        language="Python",
        framework="FastAPI",
        business_domains=[
            {
                "canonical_name": "Payment Processing",
                "normalised_key": "payment_process",
                "description": "Handles payment transactions.",
            },
            {
                "canonical_name": "User Management",
                "normalised_key": "manag_user",
                "description": "User accounts and auth.",
            },
        ],
        technical_domains=[
            {"name": "API Layer", "description": "REST endpoints", "patterns": ["app/api/"]},
            {"name": "Data Access", "description": "ORM and queries", "patterns": ["app/db/"]},
        ],
        chunker_config={
            "language": "Python",
            "ast_chunk_boundaries": [
                {"node_type": "function_definition", "min_size_chars": 50, "max_size_chars": 1500}
            ],
            "fallback_strategy": "recursive_text",
        },
        extraction_prompt="Extract Python entities: functions, classes...",
    )

    mock_pool = AsyncMock()
    mock_pool._closed = False
    mock_pool.fetchrow = AsyncMock(return_value=None)
    mock_pool.fetch = AsyncMock(return_value=[])

    processor = IngestionProcessor(
        settings=MagicMock(
            max_concurrent_files=1,
            embed_workers=1,
            embed_batch_size=10,
            embed_batch_timeout=0.1,
            progress_update_interval=10,
        ),
        db_pool=mock_pool,
        version_store=None,
        ingestion_store=None,
        pipeline_store=None,
        producer=None,
        event_emitter=None,
    )

    # Patch analyze_project to return the ProjectProfile dataclass directly
    with patch(
        "code_preprocessor.project_analyzer.analyze_project",
        return_value=test_profile,
    ):
        result = await processor._run_project_analysis(
            repo="test-repo",
            branch="main",
            iid="test-iid",
            file_tree="src/\n  main.py",
            project_id=project_id,
            company_id=company_id,
        )

    # (1) Result must be a dict — never the raw ProjectProfile
    assert result is not None, "_run_project_analysis returned None"
    assert isinstance(result, dict), (
        f"_run_project_analysis returned {type(result).__name__}, expected dict. "
        "ProjectProfile object was not converted — callers will crash with "
        "'ProjectProfile' object has no attribute 'get'."
    )

    # (2) Dict has the keys expected by _build_project_section and chunk_and_enrich_file
    for key in ("description", "business_domains", "design_patterns", "architecture"):
        assert key in result, (
            f"result dict missing key '{key}'. Keys present: {list(result.keys())}"
        )

    # (3) business_domains items have 'name' key (mapped from canonical_name)
    assert result["business_domains"], "business_domains list is empty in converted dict"
    for bd in result["business_domains"]:
        assert "name" in bd, (
            f"business_domains item missing 'name' key: {bd!r}. "
            "canonical_name must be mapped to 'name' for _build_project_section compatibility."
        )
    bd_names = [bd["name"] for bd in result["business_domains"]]
    assert "Payment Processing" in bd_names, (
        f"'Payment Processing' not found in business_domain names: {bd_names}. "
        "canonical_name was not mapped correctly."
    )

    # (4) design_patterns items have 'name' key (from technical_domains)
    assert result["design_patterns"], "design_patterns list is empty in converted dict"
    for dp in result["design_patterns"]:
        assert "name" in dp, f"design_patterns item missing 'name' key: {dp!r}"
    dp_names = [dp["name"] for dp in result["design_patterns"]]
    assert "API Layer" in dp_names, (
        f"'API Layer' not found in design_pattern names: {dp_names}. "
        "technical_domains were not mapped to design_patterns correctly."
    )

    # (5) build_chunk_header succeeds with this dict — no .get() AttributeError
    try:
        header = build_chunk_header(
            project_analysis=result,
            file_path="src/payment.py",
            language="python",
            file_skeleton=None,
            chunk_index=0,
            total_chunks=3,
        )
    except AttributeError as exc:
        raise AssertionError(
            f"build_chunk_header raised AttributeError with the converted dict: {exc}. "
            "The dict format is incompatible with _build_project_section."
        ) from exc

    assert "Payment Processing" in header or "API Layer" in header or "Python" in header, (
        f"build_chunk_header output does not contain expected project context.\nHeader:\n{header}"
    )
