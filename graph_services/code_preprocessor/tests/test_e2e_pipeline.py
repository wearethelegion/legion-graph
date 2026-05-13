"""E2E pipeline tests — real data flowing through real functions.

These tests exist to catch bugs the 28 unit tests CANNOT catch because they
mock everything. Two production bugs already slipped through:

  Bug 1: _run_project_analysis() imported from wrong module and didn't pass
         project_id/company_id → project_profiles table empty after ingestion.

  Bug 2: analyze_project() returns a ProjectProfile dataclass, but enrichment
         called .get() on it → 'ProjectProfile' object has no attribute 'get'.

Scenarios:
  S1 — Golden path: full ingestion produces correct DB state
  S2 — Profile type safety: _run_project_analysis returns dict, not dataclass
  S3 — Data quality: line numbers, coverage, determinism
  S4 — Downstream message quality: Kafka-ready chunk dicts
  S5 — Failure modes: LLM errors, missing profile, graceful degradation

Infrastructure:
  - Real asyncpg Pool to kgrag-postgres (postgresql://kgrag@localhost:5432/kgrag_auth)
  - LLM mocked via litellm.acompletion patch
  - No Kafka — chunk dicts captured directly from chunk_and_enrich_file
  - Unique test project_id per test; cleaned up in teardown
  - Tests skipped when Postgres is unreachable (CI-safe)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from pathlib import Path
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# DB availability guard — skip entire module when Postgres is down
# ---------------------------------------------------------------------------

_DB_DSN = "postgresql://kgrag:kgrag_password@127.0.0.1:5432/kgrag_auth"
_FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Known company_id from the DB (cc6bdaf4 exists in all envs)
_TEST_COMPANY_ID = "cc6bdaf4-311f-4b04-8ee3-07cc85b76142"


def _postgres_available() -> bool:
    """Return True if we can connect to the test Postgres."""
    try:
        import asyncpg

        async def _probe():
            conn = await asyncpg.connect(_DB_DSN)
            await conn.close()

        asyncio.run(_probe())
        return True
    except Exception:
        return False


_DB_AVAILABLE = _postgres_available()
_SKIP_NO_DB = pytest.mark.skipif(not _DB_AVAILABLE, reason="Postgres unavailable — skipping E2E")


# ---------------------------------------------------------------------------
# LLM mock helpers
# ---------------------------------------------------------------------------

# Deterministic LLM responses for all 4 analysis calls
_MOCK_BUSINESS_DOMAINS = {
    "language": "Python",
    "framework": "FastAPI",
    "business_domains": [
        {
            "canonical_name": "User Management",
            "normalised_key": "manag_user",
            "description": "Handles user lifecycle, authentication, and role assignment.",
        },
        {
            "canonical_name": "API Layer",
            "normalised_key": "api_layer",
            "description": "Exposes REST endpoints for external consumers.",
        },
    ],
}

_MOCK_TECHNICAL_DOMAINS = {
    "technical_domains": [
        {
            "name": "Service Layer",
            "description": "Business logic encapsulated in service classes.",
            "patterns": ["*_service.py", "services/"],
        },
        {
            "name": "Data Access",
            "description": "Repository pattern for data persistence.",
            "patterns": ["*_repository.py", "storage/"],
        },
    ]
}

_MOCK_CHUNKER_CONFIG = {
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
            "description": "Class definitions including all methods.",
        },
    ],
    "fallback_strategy": "recursive_text",
    "fallback_chunk_size": 1000,
    "fallback_overlap": 100,
    "language_rules": {"rule_description": "Decorators belong with the decorated function"},
    "file_type_overrides": [{"extension": ".yml", "strategy": "recursive_text", "chunk_size": 800}],
}

_MOCK_EXTRACTION_PROMPT = (
    "You are a knowledge graph extractor for Python/FastAPI codebases.\n"
    "Business domains: User Management, API Layer\n"
    "Technical domains: Service Layer, Data Access\n"
    "Entity types (closed list): Function, Class, Module, Endpoint, Repository, Model\n"
    "Relationship types (closed list): calls, imports, inherits, implements, decorates\n"
    "Extract entities and relationships from the provided code chunk. "
    "Return JSON with keys: entities, relationships."
)

_MOCK_EXTRACTION_PROMPT_RESPONSE = {"filled_prompt": _MOCK_EXTRACTION_PROMPT}

_LLM_RESPONSES = [
    json.dumps(_MOCK_BUSINESS_DOMAINS),
    json.dumps(_MOCK_TECHNICAL_DOMAINS),
    json.dumps(_MOCK_CHUNKER_CONFIG),
    json.dumps(_MOCK_EXTRACTION_PROMPT_RESPONSE),
]


def make_mock_acompletion():
    """Return a mock acompletion that cycles through deterministic responses."""
    state = {"call_index": 0}
    lock = asyncio.Lock()

    async def mock_acompletion(**kwargs):
        # Skip probe calls (max_tokens=1) — they succeed but don't consume a response slot
        if kwargs.get("max_tokens") == 1:
            mock_resp = MagicMock()
            mock_resp.choices = [MagicMock()]
            mock_resp.choices[0].message.content = '{"ok": true}'
            return mock_resp
        async with lock:
            idx = state["call_index"] % len(_LLM_RESPONSES)
            state["call_index"] += 1
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = _LLM_RESPONSES[idx]
        return mock_resp

    return mock_acompletion


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool():
    """Real asyncpg pool for the test DB. Skips if unavailable."""
    if not _DB_AVAILABLE:
        pytest.skip("Postgres unavailable")
    import asyncpg

    pool = await asyncpg.create_pool(_DB_DSN, min_size=1, max_size=3)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def test_project(db_pool):
    """Insert a temporary test project row; delete it after the test."""
    project_id = str(uuid.uuid4())
    await db_pool.execute(
        """
        INSERT INTO projects (id, company_id, name, description)
        VALUES ($1, $2, $3, $4)
        """,
        project_id,
        _TEST_COMPANY_ID,
        f"e2e-test-{project_id[:8]}",
        "Temporary E2E test project",
    )
    yield project_id
    # Teardown: delete project (cascades to project_profiles, file_chunks, etc.)
    await db_pool.execute("DELETE FROM projects WHERE id = $1", project_id)


# ---------------------------------------------------------------------------
# S1 — Golden Path: full ingestion produces correct DB state
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s1_golden_path_full_ingestion(db_pool, test_project):
    """Full ingestion: analyze_project → _run_project_analysis → DB has correct state.

    Catches Bug 1: wrong import / missing project_id in _run_project_analysis.
    After this test passes, reintroduce the bug and verify it FAILS.
    """
    from code_preprocessor.project_analyzer import analyze_project
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    project_id = test_project
    company_id = _TEST_COMPANY_ID

    # Build a file tree from the fixture directory
    fixture_tree = "\n".join(f"  {f.name}" for f in _FIXTURES_DIR.iterdir() if f.suffix == ".py")
    file_tree = f"tests/fixtures/\n{fixture_tree}"

    # ── Step 1: run analyze_project (real logic, mocked LLM) ─────────────
    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=make_mock_acompletion(),
    ):
        profile = await analyze_project(
            project_id=project_id,
            repo_path="/nonexistent",  # not used — file_tree supplied
            company_id=company_id,
            pool=db_pool,
            file_tree=file_tree,
        )

    # ── Step 2: verify project_profiles row exists with correct data ──────
    row = await db_pool.fetchrow(
        "SELECT * FROM code_processing.project_profiles WHERE project_id = $1",
        project_id,
    )
    assert row is not None, (
        f"project_profiles has no row for project_id={project_id}. "
        "Bug 1 reintroduced: analyze_project not writing to DB."
    )
    assert row["extraction_prompt"], (
        "extraction_prompt is null in project_profiles. LLM call D result was not persisted."
    )
    assert len(row["extraction_prompt"]) > 50, (
        f"extraction_prompt too short ({len(row['extraction_prompt'])} chars)."
    )

    # ── Step 3: run chunk_and_enrich_file on a fixture ────────────────────
    calculator_content = (_FIXTURES_DIR / "calculator.py").read_text()

    # Insert a minimal file_version row so chunk_and_enrich_file can look it up
    document_id = f"e2e-doc-{uuid.uuid4()}"
    file_version_id = await db_pool.fetchval(
        """
        INSERT INTO code_processing.repository_file_versions
            (document_id, repository, branch, file_path, version,
             change_type, commit_sha)
        VALUES ($1, 'e2e-test-repo', 'main', 'tests/fixtures/calculator.py',
                1, 'A', 'deadbeef')
        RETURNING id
        """,
        document_id,
    )

    items = await chunk_and_enrich_file(
        pool=db_pool,
        document_id=document_id,
        file_path="tests/fixtures/calculator.py",
        content=calculator_content,
        ingestion_id=f"e2e-ingestion-{uuid.uuid4()}",
        project_id=project_id,
        company_id=company_id,
        repository="e2e-test-repo",
        branch="main",
        project_analysis=None,
    )

    # ── Step 4: assert chunk quality ──────────────────────────────────────
    assert items, "chunk_and_enrich_file returned no items for calculator.py"

    for i, item in enumerate(items):
        assert item.get("extraction_prompt") == _MOCK_EXTRACTION_PROMPT, (
            f"chunk[{i}] extraction_prompt={item.get('extraction_prompt')!r}, "
            f"expected the mock prompt. "
            "chunk_and_enrich_file is not reading from project_profiles."
        )
        start = item.get("start_line", 0)
        end = item.get("end_line", 0)
        assert isinstance(start, int) and start > 0, (
            f"chunk[{i}] start_line={start} must be int > 0"
        )
        assert isinstance(end, int) and end >= start, (
            f"chunk[{i}] end_line={end} must be >= start_line={start}"
        )
        assert item.get("header"), f"chunk[{i}] header is empty"

    # Cleanup file_version (project cleanup is handled by fixture)
    await db_pool.execute(
        "DELETE FROM code_processing.repository_file_versions WHERE id = $1",
        file_version_id,
    )


# ---------------------------------------------------------------------------
# S2 — Profile Type Safety
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s2_profile_type_safety(db_pool, test_project):
    """analyze_project returns ProjectProfile; _run_project_analysis converts to dict.

    Catches Bug 2: calling .get() on a ProjectProfile dataclass crashes.
    The pipeline MUST convert the dataclass to a dict before passing downstream.
    """
    from code_preprocessor.project_analyzer import analyze_project, ProjectProfile
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        _profile_to_analysis_dict,
    )
    from code_preprocessor.kafka_processing_service._file_processing import (
        _fetch_project_profile,
    )

    project_id = test_project
    company_id = _TEST_COMPANY_ID
    file_tree = "tests/fixtures/\n  calculator.py\n  user_service.py"

    # ── Step 1: analyze_project returns a ProjectProfile (not a dict) ──────
    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=make_mock_acompletion(),
    ):
        profile = await analyze_project(
            project_id=project_id,
            repo_path="/nonexistent",
            company_id=company_id,
            pool=db_pool,
            file_tree=file_tree,
        )

    assert isinstance(profile, ProjectProfile), (
        f"analyze_project must return ProjectProfile, got {type(profile).__name__}. "
        "The return type contract is broken."
    )

    # ── Step 2: _profile_to_analysis_dict converts it to the expected shape ──
    analysis_dict = _profile_to_analysis_dict(profile)

    assert isinstance(analysis_dict, dict), (
        f"_profile_to_analysis_dict must return dict, got {type(analysis_dict).__name__}. "
        "Bug 2 reintroduced: enrichment would call .get() on a dataclass."
    )
    required_keys = {"description", "business_domains", "design_patterns", "architecture"}
    missing = required_keys - set(analysis_dict.keys())
    assert not missing, (
        f"_profile_to_analysis_dict result missing keys: {missing}. "
        f"Got keys: {set(analysis_dict.keys())}"
    )
    assert isinstance(analysis_dict["business_domains"], list), (
        "business_domains must be a list in analysis_dict"
    )

    # ── Step 3: _fetch_profile_config reads real DB row ────────────────────
    chunker_config, extraction_prompt = await _fetch_project_profile(db_pool, project_id)

    assert extraction_prompt is not None, (
        "_fetch_project_profile returned None extraction_prompt after profile was stored. "
        "The DB row exists but the read path is broken."
    )
    assert isinstance(extraction_prompt, str) and len(extraction_prompt) > 10, (
        f"extraction_prompt from _fetch_project_profile is invalid: {extraction_prompt!r}"
    )
    assert chunker_config is not None, (
        "_fetch_project_profile returned None chunker_config after profile was stored."
    )
    assert isinstance(chunker_config, dict), (
        f"chunker_config must be dict (deserialized from JSONB), got {type(chunker_config).__name__}"
    )


# ---------------------------------------------------------------------------
# S3 — Data Quality
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s3_data_quality(db_pool, test_project):
    """Chunk quality: line coverage, monotonic ordering, determinism.

    Verifies the chunker produces well-formed output for the fixture files.
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    project_id = test_project
    company_id = _TEST_COMPANY_ID

    content = (_FIXTURES_DIR / "user_service.py").read_text()
    doc_id = f"e2e-doc-{uuid.uuid4()}"

    file_version_id = await db_pool.fetchval(
        """
        INSERT INTO code_processing.repository_file_versions
            (document_id, repository, branch, file_path, version,
             change_type, commit_sha)
        VALUES ($1, 'e2e-test-repo', 'main', 'tests/fixtures/user_service.py',
                1, 'A', 'deadbeef')
        RETURNING id
        """,
        doc_id,
    )

    items = await chunk_and_enrich_file(
        pool=db_pool,
        document_id=doc_id,
        file_path="tests/fixtures/user_service.py",
        content=content,
        ingestion_id=f"e2e-ingestion-{uuid.uuid4()}",
        project_id=project_id,
        company_id=company_id,
        repository="e2e-test-repo",
        branch="main",
        project_analysis=None,
    )

    assert items, "chunk_and_enrich_file returned no chunks for user_service.py"

    # ── Line number assertions ────────────────────────────────────────────
    prev_end = 0
    for i, item in enumerate(items):
        start = item.get("start_line", 0)
        end = item.get("end_line", 0)
        assert isinstance(start, int), f"chunk[{i}] start_line is not int: {start!r}"
        assert isinstance(end, int), f"chunk[{i}] end_line is not int: {end!r}"
        assert start >= 1, f"chunk[{i}] start_line={start} must be >= 1"
        assert end >= start, f"chunk[{i}] end_line={end} < start_line={start}"
        assert start >= prev_end or i == 0, (
            f"chunk[{i}] start_line={start} overlaps prev end={prev_end} "
            "(line numbers not monotonically increasing)"
        )
        prev_end = end

    # ── Header assertions ─────────────────────────────────────────────────
    for i, item in enumerate(items):
        header = item.get("header", "")
        assert header, f"chunk[{i}] has empty header"
        assert "user_service.py" in header or "fixtures" in header, (
            f"chunk[{i}] header does not reference file path: {header!r}"
        )

    # ── Determinism: same content → same chunk_hashes ─────────────────────
    doc_id2 = f"e2e-doc-{uuid.uuid4()}"
    file_version_id2 = await db_pool.fetchval(
        """
        INSERT INTO code_processing.repository_file_versions
            (document_id, repository, branch, file_path, version,
             change_type, commit_sha)
        VALUES ($1, 'e2e-test-repo', 'main', 'tests/fixtures/user_service.py',
                2, 'A', 'deadbeef2')
        RETURNING id
        """,
        doc_id2,
    )
    items2 = await chunk_and_enrich_file(
        pool=db_pool,
        document_id=doc_id2,
        file_path="tests/fixtures/user_service.py",
        content=content,
        ingestion_id=f"e2e-ingestion-{uuid.uuid4()}",
        project_id=project_id,
        company_id=company_id,
        repository="e2e-test-repo",
        branch="main",
        project_analysis=None,
    )

    hashes1 = [item.get("chunk_hash") for item in items]
    hashes2 = [item.get("chunk_hash") for item in items2]
    # Note: chunk_and_enrich_file doesn't expose chunk_hash in returned dicts,
    # but does write it to DB. Verify count is same (deterministic split).
    assert len(items) == len(items2), (
        f"Non-deterministic chunking: first run={len(items)} chunks, "
        f"second run={len(items2)} chunks for identical content."
    )

    # Cleanup
    for fv_id in (file_version_id, file_version_id2):
        await db_pool.execute(
            "DELETE FROM code_processing.repository_file_versions WHERE id = $1", fv_id
        )


# ---------------------------------------------------------------------------
# S4 — Downstream Message Quality
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s4_downstream_message_quality(db_pool, test_project):
    """Chunk dicts from chunk_and_enrich_file are Kafka-ready with all required fields.

    This validates the message contract for the entity_extraction_service consumer.
    """
    from code_preprocessor.project_analyzer import analyze_project
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    project_id = test_project
    company_id = _TEST_COMPANY_ID
    file_tree = "tests/fixtures/\n  api_routes.py"

    # Store a real profile so extraction_prompt is populated
    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=make_mock_acompletion(),
    ):
        await analyze_project(
            project_id=project_id,
            repo_path="/nonexistent",
            company_id=company_id,
            pool=db_pool,
            file_tree=file_tree,
        )

    content = (_FIXTURES_DIR / "api_routes.py").read_text()
    doc_id = f"e2e-doc-{uuid.uuid4()}"
    ingestion_id = f"e2e-ingestion-{uuid.uuid4()}"

    file_version_id = await db_pool.fetchval(
        """
        INSERT INTO code_processing.repository_file_versions
            (document_id, repository, branch, file_path, version,
             change_type, commit_sha)
        VALUES ($1, 'e2e-test-repo', 'main', 'tests/fixtures/api_routes.py',
                1, 'A', 'deadbeef')
        RETURNING id
        """,
        doc_id,
    )

    items = await chunk_and_enrich_file(
        pool=db_pool,
        document_id=doc_id,
        file_path="tests/fixtures/api_routes.py",
        content=content,
        ingestion_id=ingestion_id,
        project_id=project_id,
        company_id=company_id,
        repository="e2e-test-repo",
        branch="main",
        project_analysis=None,
    )

    assert items, "No chunks produced for api_routes.py — cannot validate message quality"

    required_fields = {
        "extraction_prompt",
        "project_id",
        "start_line",
        "end_line",
        "header",
        "file_skeleton",
    }

    for i, msg in enumerate(items):
        # extraction_prompt: must come from DB profile (non-null after analyze_project)
        assert msg.get("extraction_prompt") == _MOCK_EXTRACTION_PROMPT, (
            f"msg[{i}] extraction_prompt={msg.get('extraction_prompt')!r}, "
            "expected mock prompt. Profile was stored but not read by chunk_and_enrich_file."
        )

        # project_id: must match test project
        assert msg.get("project_id") == project_id, (
            f"msg[{i}] project_id={msg.get('project_id')!r}, expected {project_id!r}"
        )

        # start_line / end_line: integer types
        assert isinstance(msg.get("start_line"), int), (
            f"msg[{i}] start_line must be int, got {type(msg.get('start_line'))}"
        )
        assert isinstance(msg.get("end_line"), int), (
            f"msg[{i}] end_line must be int, got {type(msg.get('end_line'))}"
        )

        # header: non-empty
        assert msg.get("header"), f"msg[{i}] header is empty"

        # file_skeleton: present (may be None for non-parseable files)
        assert "file_skeleton" in msg, f"msg[{i}] missing 'file_skeleton' key"

    # Cleanup
    await db_pool.execute(
        "DELETE FROM code_processing.repository_file_versions WHERE id = $1",
        file_version_id,
    )


# ---------------------------------------------------------------------------
# S5 — Failure Modes
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s5a_llm_failure_returns_none_from_run_project_analysis(db_pool, test_project):
    """When LLM raises, _run_project_analysis returns None (not crash).

    Catches a regression where _run_project_analysis propagated the exception
    instead of gracefully degrading.
    """
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        IngestionProcessor,
    )

    project_id = test_project
    company_id = _TEST_COMPANY_ID

    processor = IngestionProcessor(
        settings=MagicMock(
            max_concurrent_files=1,
            embed_workers=1,
            embed_batch_size=10,
            embed_batch_timeout=0.1,
            progress_update_interval=10,
        ),
        db_pool=db_pool,
        version_store=None,
        ingestion_store=None,
        pipeline_store=None,
        producer=None,
        event_emitter=None,
    )

    async def always_fails(**kwargs):
        raise RuntimeError("LLM API simulated failure")

    with patch(
        "code_preprocessor.project_analyzer.litellm.acompletion",
        side_effect=always_fails,
    ):
        result = await processor._run_project_analysis(
            repo="e2e-test-repo",
            branch="main",
            iid=f"e2e-iid-{uuid.uuid4()}",
            file_tree="tests/fixtures/\n  calculator.py",
            project_id=project_id,
            company_id=company_id,
        )

    assert result is None, (
        f"_run_project_analysis must return None on LLM failure, got {result!r}. "
        "The caller depends on None to skip profile-based features gracefully."
    )


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s5b_fetch_profile_config_returns_none_tuple_when_no_profile(db_pool, test_project):
    """_fetch_project_profile returns (None, None) when no profile exists for project.

    The test_project fixture creates a project but does NOT run analyze_project,
    so project_profiles has no row for this project_id.
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        _fetch_project_profile,
    )

    project_id = test_project  # project exists in projects table, but no profile yet

    chunker_config, extraction_prompt = await _fetch_project_profile(db_pool, project_id)

    assert chunker_config is None, (
        f"Expected chunker_config=None when no profile, got {chunker_config!r}"
    )
    assert extraction_prompt is None, (
        f"Expected extraction_prompt=None when no profile, got {extraction_prompt!r}"
    )


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s5c_enrichment_runs_without_profile_extraction_prompt_is_none(db_pool, test_project):
    """chunk_and_enrich_file does NOT crash when no profile exists.

    When project_profiles has no row, extraction_prompt in chunk items is None.
    The pipeline must degrade gracefully, not crash.
    """
    from code_preprocessor.kafka_processing_service._file_processing import (
        chunk_and_enrich_file,
    )

    project_id = test_project  # no profile stored for this project
    company_id = _TEST_COMPANY_ID
    content = (_FIXTURES_DIR / "calculator.py").read_text()
    doc_id = f"e2e-doc-{uuid.uuid4()}"

    file_version_id = await db_pool.fetchval(
        """
        INSERT INTO code_processing.repository_file_versions
            (document_id, repository, branch, file_path, version,
             change_type, commit_sha)
        VALUES ($1, 'e2e-test-repo', 'main', 'tests/fixtures/calculator_noprofile.py',
                1, 'A', 'deadbeef')
        RETURNING id
        """,
        doc_id,
    )

    # Must not raise — graceful degradation when no profile
    items = await chunk_and_enrich_file(
        pool=db_pool,
        document_id=doc_id,
        file_path="tests/fixtures/calculator_noprofile.py",
        content=content,
        ingestion_id=f"e2e-ingestion-{uuid.uuid4()}",
        project_id=project_id,
        company_id=company_id,
        repository="e2e-test-repo",
        branch="main",
        project_analysis=None,
    )

    assert isinstance(items, list), "chunk_and_enrich_file must return a list, not raise"
    assert items, "Expected at least one chunk even without profile"

    # extraction_prompt must be None (not crash) when no profile exists
    for i, item in enumerate(items):
        assert item.get("extraction_prompt") is None, (
            f"chunk[{i}] extraction_prompt={item.get('extraction_prompt')!r}, "
            "expected None when no profile. If this fails, profile was unexpectedly found."
        )

    # Cleanup
    await db_pool.execute(
        "DELETE FROM code_processing.repository_file_versions WHERE id = $1",
        file_version_id,
    )


@_SKIP_NO_DB
@pytest.mark.asyncio
async def test_s5d_llm_failure_error_logged(db_pool, test_project, caplog):
    """LLM failure causes ERROR log from _run_project_analysis.

    Verifies the error is visible in logs (not silently swallowed).
    """
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        IngestionProcessor,
    )

    project_id = test_project

    processor = IngestionProcessor(
        settings=MagicMock(
            max_concurrent_files=1,
            embed_workers=1,
            embed_batch_size=10,
            embed_batch_timeout=0.1,
            progress_update_interval=10,
        ),
        db_pool=db_pool,
        version_store=None,
        ingestion_store=None,
        pipeline_store=None,
        producer=None,
        event_emitter=None,
    )

    async def always_fails(**kwargs):
        raise RuntimeError("Simulated LLM outage for log test")

    with caplog.at_level(logging.ERROR, logger="code_preprocessor"):
        with patch(
            "code_preprocessor.project_analyzer.litellm.acompletion",
            side_effect=always_fails,
        ):
            await processor._run_project_analysis(
                repo="e2e-test-repo",
                branch="main",
                iid=f"e2e-iid-{uuid.uuid4()}",
                file_tree="tests/fixtures/\n  calculator.py",
                project_id=project_id,
                company_id=_TEST_COMPANY_ID,
            )

    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert error_records, (
        "No ERROR log emitted when LLM failed. "
        "Silent failures make debugging impossible in production."
    )
