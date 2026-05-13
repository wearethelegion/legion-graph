"""Two-stage streaming pipeline for per-file ingestion processing.

Architecture:
  Queue 1 (N file workers): file → dedup → skeleton → chunk → header → Postgres → push to Q2
  Queue 2 (M embed workers): micro-batch collect → embed → update Postgres → publish to Kafka

Both pools run concurrently — chunks flow to Kafka as soon as embedded.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
from aiokafka import AIOKafkaProducer

from .config import KafkaProcessingSettings
from .event_emitter import EventEmitter
from .file_filter import FileFilter, FilterReason, FilterStats, IngestionRules
from .git_repository_manager import GitChange, GitDiffResult
from ._file_processing import process_file
from ..storage.repository_version_store import RepositoryVersionStore
from ..storage.ingestion_store import IngestionStore
from ..storage.pipeline_store import PipelineStore
from ..enrichment import embed_and_publish_batch, store_project_tree

logger = logging.getLogger(__name__)
_SHUTDOWN_SENTINEL = object()
# fmt: off
_SQL_INSERT_STATS = ("INSERT INTO code_processing.cogni_ingestion_stats (ingestion_id,company_id,"
    "project_id,files_produced,files_skipped,files_consumed,files_processed,files_failed,"
    "first_consumed_at) VALUES ($1,$2,$3,0,0,0,0,0,NULL) ON CONFLICT (ingestion_id) DO NOTHING")
_SQL_INSERT_SKIPPED = ("INSERT INTO code_processing.skipped_files (ingestion_id,company_id,"
    "project_id,repository,branch,file_path,service,skip_type,reason)"
    " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
_SQL_DELETE_OLD_BATCHES = ("DELETE FROM code_processing.ingestion_batches"
    " WHERE repository=$1 AND branch=$2 AND ingestion_id!=$3")
_SQL_FILES_PARSED = ("SELECT COUNT(DISTINCT f.file_path) FROM code_processing.file_chunks c"
    " JOIN code_processing.repository_file_versions f ON c.file_version_id=f.id"
    " WHERE c.ingestion_id=$1")
_SQL_FILES_WITH_EMBED = ("SELECT COUNT(DISTINCT f.file_path) FROM code_processing.file_chunks c"
    " JOIN code_processing.repository_file_versions f ON c.file_version_id=f.id"
    " WHERE f.repository=$1 AND f.branch=$2 AND c.embedding IS NOT NULL")
# fmt: on
_SKIP_MAP = {
    FilterReason.SIZE: "filtered_size",
    FilterReason.EXTENSION: "filtered_extension",
    FilterReason.DIRECTORY: "filtered_directory",
}


async def _safe(coro, msg: str, *a) -> Any:
    try:
        return await coro
    except Exception as exc:
        logger.warning(msg, *a, exc)
        return None


def _profile_to_analysis_dict(profile: Any) -> dict:
    """Convert a ProjectProfile dataclass to the dict format expected by enrichment.

    ``chunk_and_enrich_file`` and ``build_chunk_header`` / ``_build_project_section``
    treat ``project_analysis`` as a plain dict with keys:
        - "description"       : str
        - "business_domains"  : list[{"name": str, "key_concepts": list}]
        - "design_patterns"   : list[{"name": str}]
        - "architecture"      : dict (optional "style" key)

    ProjectProfile stores:
        - business_domains : list[{"canonical_name": str, "normalised_key": str, …}]
        - technical_domains: list[{"name": str, "description": str, …}]

    This function maps between the two representations using proper attribute access
    — it does NOT call .dict() or .get() on the dataclass.
    """
    business_domains = [
        {
            "name": d.get("canonical_name") or d.get("name", ""),
            "key_concepts": d.get("key_concepts", []),
        }
        for d in (profile.business_domains or [])
        if isinstance(d, dict)
    ]
    design_patterns = [
        {"name": d.get("name", "")}
        for d in (profile.technical_domains or [])
        if isinstance(d, dict) and d.get("name")
    ]
    description = f"{profile.language}/{profile.framework} project"
    return {
        "description": description,
        "business_domains": business_domains,
        "design_patterns": design_patterns,
        "architecture": {},
    }


class IngestionProcessor:
    """Two-stage streaming pipeline: file workers → embed workers."""

    def __init__(
        self,
        settings: KafkaProcessingSettings,
        db_pool: Optional[asyncpg.Pool],
        version_store: Optional[RepositoryVersionStore],
        ingestion_store: Optional[IngestionStore],
        pipeline_store: Optional[PipelineStore],
        producer: Optional[AIOKafkaProducer],
        event_emitter: Optional[EventEmitter],
    ) -> None:
        self._settings = settings
        self._db_pool = db_pool
        self._version_store = version_store
        self._ingestion_store = ingestion_store
        self._pipeline_store = pipeline_store
        self._producer = producer
        self._event_emitter = event_emitter

    # ── Top-level orchestration ───────────────────────────────────────

    async def process_ingestion(
        self,
        diff: GitDiffResult,
        framework: str,
        project_id: str,
        company_id: str,
        user_id: str,
        ingestion_id: str,
        file_tree: str = "",
    ) -> None:
        """Filter → pre-worker analysis → two-stage worker pipeline → finalize."""
        if not diff.changes:
            return
        repo, branch = diff.repository, diff.branch
        await self._create_tracking(
            ingestion_id, project_id, company_id, user_id, repo, branch, diff, framework
        )
        changes, total_filtered, stats = await self._filter_files(
            diff, ingestion_id, project_id, company_id, repo, branch
        )
        total_files = len(changes)
        logger.info(
            "Starting ingestion %s for %s@%s (%d files, %d filtered: %d size, %d ext, %d dir)",
            ingestion_id,
            repo,
            branch,
            total_files,
            total_filtered,
            stats.files_filtered_size,
            stats.files_filtered_extension,
            stats.files_filtered_directory,
        )
        if self._pipeline_store:
            await _safe(
                self._pipeline_store.set_counter(ingestion_id, "files_filtered", total_files),
                "set files_filtered: %s",
            )
        await self._cleanup_old_metadata(ingestion_id, repo, branch)
        if self._ingestion_store and total_filtered > 0:
            try:
                await self._ingestion_store.update_progress(
                    ingestion_id, files_skipped_delta=total_filtered
                )
            except Exception:
                pass
        if not changes:
            await self._handle_all_filtered(ingestion_id, company_id, project_id)
            return

        # Pre-worker: store project tree + run project analysis
        if self._db_pool and file_tree:
            await _safe(
                store_project_tree(self._db_pool, ingestion_id, file_tree),
                "store project tree %s: %s",
                ingestion_id,
            )
        project_analysis = await self._run_project_analysis(
            repo, branch, ingestion_id, file_tree, project_id=project_id, company_id=company_id
        )

        # Two-stage streaming pipeline
        ok, failed, embedded = await self._run_two_stage_pipeline(
            changes,
            diff,
            framework,
            project_id,
            company_id,
            user_id,
            ingestion_id,
            total_files,
            project_analysis,
        )
        await self._finalize(
            diff,
            ingestion_id,
            project_id,
            company_id,
            repo,
            branch,
            total_files,
            ok,
            failed,
            embedded,
        )

    async def _run_project_analysis(
        self,
        repo: str,
        branch: str,
        iid: str,
        file_tree: str = "",
        project_id: str = "",
        company_id: str = "",
    ) -> Optional[dict]:
        if not self._db_pool:
            return None
        try:
            from ..project_analyzer import analyze_project, ProjectProfile

            analysis = await analyze_project(
                project_id=project_id,
                repo_path=repo,
                company_id=company_id,
                pool=self._db_pool,
                file_tree=file_tree or None,
            )
            if analysis is not None:
                # analyze_project returns a ProjectProfile dataclass; convert to the
                # dict format expected by chunk_and_enrich_file / build_chunk_header.
                if isinstance(analysis, ProjectProfile):
                    return _profile_to_analysis_dict(analysis)
                return analysis  # already a dict (should not occur, but safe)
        except Exception as exc:
            logger.error(
                "Project analysis failed for %s (%s: %s) — no profile will be stored",
                iid,
                type(exc).__name__,
                exc,
                exc_info=True,
            )
        try:
            row = await self._db_pool.fetchrow(
                "SELECT project_analysis FROM code_processing.ingestion_batches "
                "WHERE repository=$1 AND branch=$2 AND project_analysis IS NOT NULL "
                "ORDER BY created_at DESC LIMIT 1",
                repo,
                branch,
            )
            if row and row["project_analysis"]:
                pa = row["project_analysis"]
                return json.loads(pa) if isinstance(pa, str) else pa
        except Exception:
            pass
        return None

    # ── Two-stage pipeline ────────────────────────────────────────────

    async def _run_two_stage_pipeline(
        self,
        changes: list[GitChange],
        diff: GitDiffResult,
        fw: str,
        pid: str,
        cid: str,
        uid: str,
        iid: str,
        total: int,
        project_analysis: Optional[dict],
    ) -> tuple[int, int, int]:
        """Spawn file workers + embed workers, feed files, drain both queues."""
        n_file = self._settings.max_concurrent_files
        n_embed = self._settings.embed_workers
        file_queue: asyncio.Queue = asyncio.Queue(maxsize=n_file)
        embed_queue: asyncio.Queue = asyncio.Queue(maxsize=n_embed * 2)
        file_ctr = {"ok": 0, "fail": 0}
        embed_ctr = {"embedded": 0}
        embed_model = os.environ.get("EMBEDDING_MODEL", "vertex_ai/gemini-embedding-001")
        topic = os.environ.get("ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")

        file_workers = [
            asyncio.create_task(
                self._file_worker(
                    w,
                    file_queue,
                    embed_queue,
                    file_ctr,
                    diff,
                    fw,
                    pid,
                    cid,
                    uid,
                    iid,
                    total,
                    project_analysis,
                )
            )
            for w in range(n_file)
        ]
        embed_workers = [
            asyncio.create_task(self._embed_worker(w, embed_queue, embed_ctr, embed_model, topic))
            for w in range(n_embed)
        ]

        interval = self._settings.progress_update_interval
        for idx, ch in enumerate(changes, 1):
            await file_queue.put((ch, idx))
            if self._ingestion_store and idx % interval == 0:
                await self._ingestion_store.update_progress(iid, current_file=ch.file_path)

        for _ in range(n_file):
            await file_queue.put(_SHUTDOWN_SENTINEL)
        await asyncio.gather(*file_workers, return_exceptions=True)

        for _ in range(n_embed):
            await embed_queue.put(_SHUTDOWN_SENTINEL)
        await asyncio.gather(*embed_workers, return_exceptions=True)

        if self._ingestion_store and file_ctr["ok"] > 0:
            await self._ingestion_store.update_progress(iid, files_processed_delta=file_ctr["ok"])
        logger.info(
            "Pipeline complete for %s: %d ok, %d failed, %d embedded",
            iid,
            file_ctr["ok"],
            file_ctr["fail"],
            embed_ctr["embedded"],
        )
        return file_ctr["ok"], file_ctr["fail"], embed_ctr["embedded"]

    # ── File worker (Queue 1) ─────────────────────────────────────────

    async def _file_worker(
        self,
        wid: int,
        file_q: asyncio.Queue,
        embed_q: asyncio.Queue,
        ctr: dict,
        diff: GitDiffResult,
        fw: str,
        pid: str,
        cid: str,
        uid: str,
        iid: str,
        total: int,
        project_analysis: Optional[dict],
    ) -> None:
        try:
            while True:
                item = await file_q.get()
                if item is _SHUTDOWN_SENTINEL:
                    break
                ch, idx = item
                try:
                    await process_file(
                        diff,
                        ch,
                        fw,
                        pid,
                        cid,
                        uid,
                        ingestion_id=iid,
                        file_index=idx,
                        total_files=total,
                        embed_queue=embed_q,
                        project_analysis=project_analysis,
                        db_pool=self._db_pool,
                        version_store=self._version_store,
                        event_emitter=self._event_emitter,
                        stamp_fn=self._stamp,
                    )
                    ctr["ok"] += 1
                except Exception as exc:
                    logger.error("Failed to process %s: %s", ch.file_path, exc, exc_info=True)
                    ctr["fail"] += 1
                    if self._ingestion_store:
                        await self._ingestion_store.update_progress(
                            iid,
                            files_failed_delta=1,
                            failed_file={
                                "file_path": ch.file_path,
                                "error": str(exc),
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                finally:
                    file_q.task_done()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("file_worker.fatal wid=%d: %s", wid, exc, exc_info=True)

    # ── Embed worker (Queue 2) ────────────────────────────────────────

    async def _embed_worker(
        self,
        wid: int,
        embed_q: asyncio.Queue,
        ctr: dict,
        embed_model: str,
        topic: str,
    ) -> None:
        """Micro-batch embed worker: collect → embed → publish to Kafka."""
        batch_size = self._settings.embed_batch_size
        batch_timeout = self._settings.embed_batch_timeout
        buffer: list[dict] = []
        try:
            while True:
                if not buffer:
                    item = await embed_q.get()
                    if item is _SHUTDOWN_SENTINEL:
                        break
                    buffer.append(item)
                while len(buffer) < batch_size:
                    try:
                        item = await asyncio.wait_for(embed_q.get(), timeout=batch_timeout)
                        if item is _SHUTDOWN_SENTINEL:
                            if buffer:
                                ctr["embedded"] += await self._flush_embed(
                                    buffer, embed_model, topic, wid
                                )
                                buffer.clear()
                            return
                        buffer.append(item)
                    except asyncio.TimeoutError:
                        break
                if buffer:
                    ctr["embedded"] += await self._flush_embed(buffer, embed_model, topic, wid)
                    buffer.clear()
        except asyncio.CancelledError:
            if buffer and not self._db_pool._closed:
                ctr["embedded"] += await self._flush_embed(buffer, embed_model, topic, wid)
        except Exception as exc:
            logger.error("embed_worker.fatal wid=%d: %s", wid, exc, exc_info=True)

    async def _flush_embed(self, buf: list[dict], model: str, topic: str, wid: int) -> int:
        if not self._db_pool or not self._producer:
            return 0
        # Guard: skip if pool is already closed (shutdown race)
        if self._db_pool._closed:
            logger.warning("embed[%d] pool closed, dropping %d chunks", wid, len(buf))
            return 0
        try:
            t0 = time.time()
            n = await embed_and_publish_batch(
                self._db_pool,
                self._producer,
                buf,
                model,
                topic=topic,
                pipeline_store=self._pipeline_store,
            )
            logger.info("embed[%d] flushed %d/%d in %.3fs", wid, n, len(buf), time.time() - t0)
            return n
        except Exception as exc:
            logger.error("embed[%d] batch failed: %s", wid, exc, exc_info=True)
            return 0

    # ── Finalization ──────────────────────────────────────────────────

    async def _finalize(
        self,
        diff: GitDiffResult,
        iid: str,
        pid: str,
        cid: str,
        repo: str,
        branch: str,
        total: int,
        ok: int,
        failed: int,
        embedded: int,
    ) -> None:
        if self._pipeline_store and self._db_pool:
            try:
                parsed = await self._db_pool.fetchval(_SQL_FILES_PARSED, iid)
                await self._pipeline_store.set_counter(iid, "files_parsed", parsed or 0)
                await self._pipeline_store.set_counter(iid, "chunks_produced", embedded)
                await self._pipeline_store.set_counter(iid, "embeddings_computed", embedded)
            except Exception as exc:
                logger.warning("set pipeline counters: %s", exc)
        if self._pipeline_store:
            await _safe(self._pipeline_store.finalize_counters(iid), "finalize counters: %s")
        if self._event_emitter and embedded > 0:
            try:
                n_files = 0
                if self._db_pool:
                    n_files = await self._db_pool.fetchval(_SQL_FILES_WITH_EMBED, repo, branch) or 0
                await self._event_emitter.emit_ingestion_complete(
                    ingestion_id=iid,
                    company_id=cid,
                    project_id=pid,
                    total_files=n_files,
                    total_chunks=embedded,
                )
            except Exception as exc:
                logger.warning("emit ingestion_complete: %s", exc)
        if self._ingestion_store:
            await self._ingestion_store.mark_completed(iid)
        logger.info(
            "Completed ingestion %s for %s@%s (%d ok, %d failed, %d embedded)",
            iid,
            repo,
            branch,
            ok,
            failed,
            embedded,
        )

    # ── Tracking / filtering helpers ──────────────────────────────────

    async def _create_tracking(
        self,
        iid: str,
        pid: str,
        cid: str,
        uid: str,
        repo: str,
        branch: str,
        diff: GitDiffResult,
        fw: str,
    ) -> None:
        if self._ingestion_store:
            try:
                await self._ingestion_store.create_ingestion(
                    ingestion_id=iid,
                    project_id=pid,
                    company_id=cid,
                    repository=repo,
                    branch=branch,
                    total_files=len(diff.changes),
                    commit_sha=diff.new_commit,
                    framework=fw,
                    user_id=uid,
                )
                await self._ingestion_store.start_ingestion(iid)
            except Exception as exc:
                logger.warning("Failed to create ingestion tracking for %s: %s", iid, exc)
        if self._db_pool:
            await _safe(
                self._db_pool.execute(_SQL_INSERT_STATS, iid, cid, pid),
                "cogni_ingestion_stats for %s: %s",
                iid,
            )
        if self._pipeline_store:
            await _safe(
                self._pipeline_store.set_counter(iid, "files_discovered", len(diff.changes)),
                "set files_discovered: %s",
            )

    async def _filter_files(
        self,
        diff: GitDiffResult,
        iid: str,
        pid: str,
        cid: str,
        repo: str,
        branch: str,
    ) -> tuple[list[GitChange], int, FilterStats]:
        ff = FileFilter(IngestionRules.defaults())
        stats = FilterStats()
        accepted: list[GitChange] = []
        for ch in diff.changes:
            if ch.change_type.upper().startswith("D"):
                accepted.append(ch)
                continue
            sz = ff.get_file_size(diff.repo_path, ch.file_path)
            r = ff.check(ch.file_path, sz)
            if r.filtered:
                stats.increment(r.reason)
                await self._record_skipped(iid, cid, pid, repo, branch, ch.file_path, r)
            else:
                accepted.append(ch)
        return accepted, stats.total_filtered, stats

    async def _record_skipped(
        self,
        iid: str,
        cid: str,
        pid: str,
        repo: str,
        branch: str,
        fp: str,
        result: Any,
    ) -> None:
        if self._db_pool:
            await _safe(
                self._db_pool.execute(
                    _SQL_INSERT_SKIPPED,
                    iid,
                    cid,
                    pid,
                    repo,
                    branch,
                    fp,
                    "preprocessor",
                    _SKIP_MAP.get(result.reason, "unknown"),
                    result.detail,
                ),
                "record skipped %s: %s",
                fp,
            )
        if self._ingestion_store:
            await self._ingestion_store.update_progress(
                iid,
                files_filtered_size_delta=1 if result.reason == FilterReason.SIZE else 0,
                files_filtered_extension_delta=1 if result.reason == FilterReason.EXTENSION else 0,
                files_filtered_directory_delta=1 if result.reason == FilterReason.DIRECTORY else 0,
                path_skipped=fp,
            )

    async def _cleanup_old_metadata(self, iid: str, repo: str, branch: str) -> None:
        if not self._db_pool:
            return
        try:
            deleted = await self._db_pool.execute(_SQL_DELETE_OLD_BATCHES, repo, branch, iid)
            n = int(deleted.split()[-1]) if deleted else 0
            if n > 0:
                logger.info("Cleaned up %d old ingestion_batches for %s@%s", n, repo, branch)
        except Exception as exc:
            logger.warning("cleanup batches %s@%s: %s", repo, branch, exc)

    async def _handle_all_filtered(self, iid: str, cid: str, pid: str) -> None:
        logger.info("All files filtered for ingestion %s - marking completed", iid)
        if self._pipeline_store:
            try:
                for c in ("files_parsed", "chunks_produced", "embeddings_computed"):
                    await self._pipeline_store.set_counter(iid, c, 0)
                await self._pipeline_store.finalize_counters(iid)
            except Exception as exc:
                logger.warning("finalize empty counters: %s", exc)
        if self._event_emitter:
            await _safe(
                self._event_emitter.emit_ingestion_complete(
                    ingestion_id=iid,
                    company_id=cid,
                    project_id=pid,
                    total_files=0,
                    total_chunks=0,
                ),
                "emit ingestion_complete (empty): %s",
            )
        if self._ingestion_store:
            await self._ingestion_store.mark_completed(iid)

    @staticmethod
    def _stamp(
        doc: dict, diff: GitDiffResult, pid: str, cid: str, uid: str, iid: str, idx: int, total: int
    ) -> None:
        doc.update(
            force_full_refresh=diff.force_full_refresh,
            project_id=pid,
            company_id=cid,
            user_id=uid,
            ingestion_id=iid,
            file_index=idx,
            total_files=total,
        )
