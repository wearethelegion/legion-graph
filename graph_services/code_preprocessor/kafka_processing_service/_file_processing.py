"""Per-file processing logic for the two-stage streaming pipeline.

Extracted from ingestion_processor.py to keep each module ≤ 600 LOC.
Contains: process_file (version → dedup → skeleton → chunk → header → push to embed queue)
           chunk_and_enrich_file (skeleton → chunk → Postgres insert → header build)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Optional

import asyncpg

from .event_emitter import EventEmitter
from .git_repository_manager import GitChange, GitDiffResult
from ..storage.repository_version_store import RepositoryVersionStore
from ..storage.project_profile_store import ProjectProfileStore
from ..enrichment import build_chunk_header, get_file_version_id
from ..skeleton_extractor import extract_skeleton
from ..chunker import chunk_file
from ..project_analyzer import _load_prompt_for_content_type
from shared.content_type_classifier import classify_content_type

logger = logging.getLogger(__name__)

# fmt: off
_SQL_LATEST_HASH = ("SELECT content_hash FROM code_processing.repository_file_versions"
    " WHERE repository=$1 AND branch=$2 AND file_path=$3 ORDER BY version DESC LIMIT 1")
_SQL_INSERT_SKIPPED = ("INSERT INTO code_processing.skipped_files (ingestion_id,company_id,"
    "project_id,repository,branch,file_path,service,skip_type,reason)"
    " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
_SQL_INSERT_ERROR = ("INSERT INTO code_processing.pipeline_errors (ingestion_id,company_id,"
    "project_id,repository,branch,file_path,service,stage,error_type,error_message)"
    " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
_SQL_INSERT_CHUNK = (
    "INSERT INTO code_processing.file_chunks"
    " (chunk_text, chunk_index, total_chunks, chunk_hash,"
    "  file_version_id, ingestion_id, project_id, company_id, status)"
    " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending') RETURNING id")
_SQL_LATEST_FILE_VERSION_ID = (
    "SELECT id FROM code_processing.repository_file_versions"
    " WHERE repository=$1 AND branch=$2 AND file_path=$3 AND NOT deleted"
    " ORDER BY version DESC LIMIT 1")
# fmt: on


async def _safe(coro, msg: str, *a) -> Any:
    try:
        return await coro
    except Exception as exc:
        logger.warning(msg, *a, exc)
        return None


async def _lookup_file_version_id(
    db_pool: Optional[asyncpg.Pool],
    repository: str,
    branch: str,
    file_path: str,
) -> str:
    """Return the latest non-deleted file_version_id for the given file path.

    Used before emitting delete messages so the downstream consumer knows
    which file_version_id's chunks to cascade-delete.
    Returns empty string if pool is unavailable or no version found.
    """
    if db_pool is None:
        logger.warning("Cannot look up file_version_id — db_pool is None for %s", file_path)
        return ""
    try:
        row = await db_pool.fetchrow(_SQL_LATEST_FILE_VERSION_ID, repository, branch, file_path)
        if row:
            return str(row["id"])
        logger.warning(
            "No non-deleted file version found for %s/%s:%s", repository, branch, file_path
        )
        return ""
    except Exception as exc:
        logger.warning("Failed to look up file_version_id for %s: %s", file_path, exc)
        return ""


async def process_file(
    diff: GitDiffResult,
    change: GitChange,
    fw: str,
    pid: str,
    cid: str,
    uid: str,
    *,
    ingestion_id: str,
    file_index: int,
    total_files: int,
    embed_queue: asyncio.Queue,
    project_analysis: Optional[dict],
    db_pool: Optional[asyncpg.Pool],
    version_store: Optional[RepositoryVersionStore],
    event_emitter: Optional[EventEmitter],
    stamp_fn,
) -> None:
    """Full per-file pipeline: version → skeleton → chunk → header → push to embed queue."""
    sha, repo, branch = diff.new_commit, diff.repository, diff.branch

    # Handle renames: emit delete for old path first
    if change.change_type.upper().startswith("R") and change.previous_path:
        if version_store is None:
            logger.error("Version store unavailable for rename of %s", change.previous_path)
            return
        # Look up file_version_id for the old path BEFORE recording delete
        old_fv_id = await _lookup_file_version_id(db_pool, repo, branch, change.previous_path)
        ddoc = await version_store.record_change(
            repository=repo,
            branch=branch,
            framework=fw,
            file_path=change.previous_path,
            change_type="D",
            commit_sha=sha,
            content_bytes=None,
            previous_path=None,
            force_full_refresh=diff.force_full_refresh,
        )
        stamp_fn(ddoc, diff, pid, cid, uid, ingestion_id, file_index, total_files)
        # Phase 4: data_enrichment emit removed (no consumer)
        if event_emitter:
            await event_emitter.emit_enriched_chunk_delete(
                repo,
                branch,
                change.previous_path,
                ingestion_id,
                pid,
                cid,
                file_version_id=old_fv_id,
            )

    # Handle modifies: emit delete for old version so stale chunks are purged
    if change.change_type.upper().startswith("M") and event_emitter:
        old_fv_id = await _lookup_file_version_id(db_pool, repo, branch, change.file_path)
        if old_fv_id:
            await event_emitter.emit_enriched_chunk_delete(
                repo,
                branch,
                change.file_path,
                ingestion_id,
                pid,
                cid,
                file_version_id=old_fv_id,
            )

    # Read content
    content: Optional[bytes] = None
    if not change.change_type.upper().startswith("D"):
        try:
            content = _read_file_bytes(diff.repo_path, change.file_path)
        except (FileNotFoundError, OSError) as exc:
            logger.warning("Cannot read %s: %s", change.file_path, exc)

    # Dedup check
    if content and db_pool:
        h = hashlib.sha256(content).hexdigest()
        existing = await db_pool.fetchval(_SQL_LATEST_HASH, repo, branch, change.file_path)
        if existing and existing == h:
            logger.info("Skipped (unchanged): %s", change.file_path)
            await _safe(
                db_pool.execute(
                    _SQL_INSERT_SKIPPED,
                    ingestion_id,
                    cid,
                    pid,
                    repo,
                    branch,
                    change.file_path,
                    "preprocessor",
                    "unchanged_content",
                    f"Content hash {h[:8]}... unchanged",
                ),
                "record skipped %s: %s",
                change.file_path,
            )
            return

    # Look up file_version_id before record_change for deletes (record_change
    # creates a new row with deleted=true, so we must capture the old id first).
    del_fv_id = ""
    if change.change_type.upper().startswith("D"):
        del_fv_id = await _lookup_file_version_id(db_pool, repo, branch, change.file_path)

    # Record version
    if version_store is None:
        raise RuntimeError("Version store not initialized")
    doc = await version_store.record_change(
        repository=repo,
        branch=branch,
        framework=fw,
        file_path=change.file_path,
        change_type=change.change_type,
        commit_sha=sha,
        content_bytes=content,
        previous_path=change.previous_path,
        force_full_refresh=diff.force_full_refresh,
    )

    # Handle deletes: emit delete event, skip chunking
    if change.change_type.upper().startswith("D"):
        if event_emitter:
            await event_emitter.emit_enriched_chunk_delete(
                repo,
                branch,
                change.file_path,
                ingestion_id,
                pid,
                cid,
                file_version_id=del_fv_id,
            )
        stamp_fn(doc, diff, pid, cid, uid, ingestion_id, file_index, total_files)
        # Phase 4: data_enrichment emit removed (no consumer)
        return

    # Chunk + enrich + push to embed queue (non-delete files with content)
    if db_pool and content:
        try:
            text = content.decode("utf-8", errors="replace")
            did = doc.get("_id")
            if did:
                chunk_items = await chunk_and_enrich_file(
                    db_pool,
                    did,
                    change.file_path,
                    text,
                    ingestion_id,
                    pid,
                    cid,
                    repo,
                    branch,
                    project_analysis,
                )
                for item in chunk_items:
                    await embed_queue.put(item)
        except Exception as exc:
            logger.warning("Enrichment failed for %s: %s", change.file_path, exc)
            await _safe(
                db_pool.execute(
                    _SQL_INSERT_ERROR,
                    ingestion_id,
                    cid,
                    pid,
                    repo,
                    branch,
                    change.file_path,
                    "preprocessor",
                    "enrichment",
                    type(exc).__name__,
                    str(exc),
                ),
                "record pipeline error %s: %s",
                change.file_path,
            )

    stamp_fn(doc, diff, pid, cid, uid, ingestion_id, file_index, total_files)
    # Phase 4: data_enrichment emit removed (no consumer)


async def _fetch_project_profile(
    pool: asyncpg.Pool, project_id: str
) -> tuple[Optional[dict], Optional[str]]:
    """Fetch (chunker_config, extraction_prompt) for a project from Postgres.

    Returns (None, None) when no profile exists yet (first ingestion before
    analysis runs).  All errors are logged as warnings — must not crash the
    pipeline.
    """
    try:
        store = ProjectProfileStore(pool)
        profile = await store.get_project_profile(project_id)
        if profile is None:
            logger.warning(
                "No project profile found for project_id=%s — "
                "chunker_config and extraction_prompt will be null (V3 features disabled)",
                project_id,
            )
            return None, None
        chunker_config = profile.get("chunker_config")
        # chunker_config is stored as JSONB but may come back as a string
        # from asyncpg when the column type is text or via mock rows in tests.
        if isinstance(chunker_config, str):
            try:
                chunker_config = json.loads(chunker_config)
            except (json.JSONDecodeError, ValueError):
                chunker_config = None
        extraction_prompt = profile.get("extraction_prompt")
        return chunker_config, extraction_prompt
    except Exception as exc:
        logger.warning(
            "Failed to fetch project profile for project_id=%s: %s — continuing without V3 config",
            project_id,
            exc,
        )
        return None, None


async def chunk_and_enrich_file(
    pool: asyncpg.Pool,
    document_id: str,
    file_path: str,
    content: str,
    ingestion_id: str,
    project_id: str,
    company_id: str,
    repository: str,
    branch: str,
    project_analysis: Optional[dict],
) -> list[dict]:
    """Skeleton → chunk → insert to Postgres → build headers → return embed items.

    V3 additions:
    - Fetches chunker_config and extraction_prompt from project_profiles table.
    - Passes chunker_config to chunk_file for AST-boundary-guided chunking.
    - Attaches extraction_prompt to every chunk item so embed workers can
      include it in the Kafka message.

    Phase 2 addition (ruby_spec routing):
    - Classifies each file's content_type via classify_content_type(file_path, language).
    - When content_type differs from the project-level default (e.g. ruby_spec vs ruby_rails),
      fetches the content-type-specific prompt from extraction_prompt_templates.
    - Falls back to the project-level extraction_prompt if no content-type-specific prompt found.
    """
    file_version_id = await get_file_version_id(pool, document_id)
    if not file_version_id:
        logger.warning("No file version found for document_id %s", document_id)
        return []

    skeleton_data = extract_skeleton(file_path, content)
    language = skeleton_data.get("language") if skeleton_data else None
    file_skeleton = json.dumps(skeleton_data.get("declarations", [])) if skeleton_data else None

    await pool.execute(
        "UPDATE code_processing.repository_file_versions"
        " SET language = $2, file_skeleton = $3::jsonb WHERE id = $1",
        file_version_id,
        language,
        file_skeleton,
    )

    # V3: fetch per-project chunker config + base extraction prompt
    chunker_config, extraction_prompt = await _fetch_project_profile(pool, project_id)

    # Phase 2: per-file content_type routing for extraction prompt selection.
    # classify_content_type maps (file_path, language) → content_type string.
    # Always try the content_type-specific prompt from extraction_prompt_templates
    # FIRST for any resolved content_type (ruby_rails, ruby_spec, typescript, etc).
    # Fall back to the project-level extraction_prompt only when no template exists.
    #
    # Why this order: project_profiles.extraction_prompt is populated by
    # analyze_project() and may hold a stale or wrong prompt for the file type
    # (e.g. a ruby_spec body stored against a Rails project). The DB template
    # keyed by content_type is authoritative; project-level is a last-resort
    # fallback for untyped / unknown-language files.
    file_content_type = classify_content_type(file_path or "", language or "")
    if file_content_type:
        ct_prompt = await _load_prompt_for_content_type(pool, file_content_type)
        if ct_prompt:
            logger.debug(
                "chunk_and_enrich_file: using content_type-specific prompt "
                "content_type=%s file_path=%s",
                file_content_type,
                file_path,
            )
            extraction_prompt = ct_prompt
        else:
            logger.debug(
                "chunk_and_enrich_file: no dedicated prompt for content_type=%s, "
                "falling back to project-level prompt for file_path=%s",
                file_content_type,
                file_path,
            )

    # chunk_file returns list of (chunk_text, start_line, end_line) tuples.
    # chunker_config drives AST-boundary chunking when available (V3); falls
    # back to size-based chunking when None (V2 backward compat).
    chunks = chunk_file(file_path, content, chunker_config=chunker_config)
    total_chunks = len(chunks)
    if not chunks:
        return []

    items: list[dict] = []
    for idx, (chunk_text, start_line, end_line) in enumerate(chunks):
        chunk_hash = hashlib.md5(chunk_text.encode("utf-8")).hexdigest()
        row = await pool.fetchrow(
            _SQL_INSERT_CHUNK,
            chunk_text,
            idx,
            total_chunks,
            chunk_hash,
            file_version_id,
            ingestion_id,
            project_id,
            company_id,
        )
        chunk_id = str(row["id"])

        header = build_chunk_header(
            project_analysis, file_path, language, file_skeleton, idx, total_chunks
        )
        await pool.execute(
            "UPDATE code_processing.file_chunks SET header = $2 WHERE id = $1",
            chunk_id,
            header,
        )

        items.append(
            {
                "chunk_id": chunk_id,
                "chunk_text": chunk_text,
                "header": header,
                "file_path": file_path,
                "language": language,
                "file_skeleton": file_skeleton,
                "repository": repository,
                "branch": branch,
                "ingestion_id": ingestion_id,
                "project_id": project_id,
                "company_id": company_id,
                "chunk_index": idx,
                "total_chunks": total_chunks,
                "file_version_id": file_version_id,
                "start_line": start_line,
                "end_line": end_line,
                # V3: propagated to Kafka message for downstream LLM extraction
                "extraction_prompt": extraction_prompt,
                # Phase 2: per-file content_type so ExtractedEntitiesEvent carries it
                "content_type": file_content_type,
                "business_domains": [
                    {"name": d["name"], "key_concepts": d.get("key_concepts", [])}
                    for d in (project_analysis or {}).get("business_domains", [])
                    if isinstance(d, dict) and "name" in d
                ],
                "technical_tags": [
                    p["name"]
                    for p in (project_analysis or {}).get("design_patterns", [])
                    if isinstance(p, dict) and "name" in p
                ],
            }
        )

    return items


def _read_file_bytes(repo_path: Path, rel: str) -> bytes:
    with (repo_path / rel).open("rb") as f:
        return f.read()
