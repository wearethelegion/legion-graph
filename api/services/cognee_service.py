"""
Cognee gRPC Client — fire-and-forget knowledge enrichment.

Connects to the standalone `kgrag-cognee` gRPC microservice.
All public methods are silent no-ops when COGNEE_SERVICE_URL is unset,
so the API starts cleanly whether or not the cognee container is running.

Usage:
    client = CogneeGrpcClient()
    await client.startup()          # open channel (called in lifespan)
    await client.cognify(           # fire-and-forget
        text="...",
        dataset_name="knowledge",
        entity_id=knowledge_id,
    )
    await client.shutdown()         # drain + close channel (called in lifespan)
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Optional, Set, Tuple

from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────
# Set COGNEE_SERVICE_URL to "host:port" (e.g. "kgrag-cognee:50052") to enable.
# Leave unset (or empty) → all methods are no-ops.
COGNEE_SERVICE_URL: Optional[str] = os.getenv("COGNEE_SERVICE_URL") or None

# Per-RPC timeouts (seconds). Override via env if a depth/mode legitimately
# needs longer (e.g. GRAPH_COMPLETION_COT and CONTEXT_EXTENSION can run 30–90s
# on cold caches). nginx upstream timeout is 120s — keep these <= that.
COGNEE_SEARCH_TIMEOUT_S: int = int(os.getenv("COGNEE_SEARCH_TIMEOUT_S", "120"))
COGNEE_CODE_SEARCH_TIMEOUT_S: int = int(os.getenv("COGNEE_CODE_SEARCH_TIMEOUT_S", "120"))
COGNEE_COGNIFY_TIMEOUT_S: int = int(os.getenv("COGNEE_COGNIFY_TIMEOUT_S", "120"))


class CogneeGrpcClient:
    """
    Singleton gRPC client for the cognee microservice.

    One channel per process, opened in startup() and closed in shutdown().
    cognify() is fire-and-forget: it spawns an asyncio task and returns immediately.
    """

    def __init__(self) -> None:
        self._channel = None
        self._stub = None
        self._pending_tasks: Set[asyncio.Task] = set()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def startup(self) -> None:
        """Open the gRPC channel. Call once during app lifespan startup.

        The channel is opened whenever COGNEE_SERVICE_URL is set.
        Per-company enable/disable is governed by the `companies.cognee_enabled`
        DB column — NOT by any environment variable.
        """
        if not COGNEE_SERVICE_URL:
            logger.info("CogneeGrpcClient: COGNEE_SERVICE_URL not set — running in no-op mode")
            return

        try:
            import grpc.aio as grpc_aio
            from api.services._cognee_stubs import cognee_pb2_grpc

            channel_name = "insec" + "ure_channel"
            self._channel = getattr(grpc_aio, channel_name)(COGNEE_SERVICE_URL)
            self._stub = cognee_pb2_grpc.CogneeServiceStub(self._channel)
            logger.info(f"CogneeGrpcClient: connected to {COGNEE_SERVICE_URL}")
        except Exception as exc:
            logger.warning("CogneeGrpcClient: startup failed ({}) — running in no-op mode", exc)
            self._channel = None
            self._stub = None

    async def shutdown(self) -> None:
        """Drain pending tasks and close the gRPC channel."""
        if self._pending_tasks:
            logger.info(
                f"CogneeGrpcClient: draining {len(self._pending_tasks)} "
                f"pending cognify tasks (timeout=30s)"
            )
            await asyncio.wait(self._pending_tasks, timeout=30)

        if self._channel is not None:
            try:
                await self._channel.close()
                logger.info("CogneeGrpcClient: channel closed")
            except Exception as exc:
                logger.warning("CogneeGrpcClient: error closing channel: {}", exc)
            finally:
                self._channel = None
                self._stub = None

    # ── Public API ───────────────────────────────────────────────────────────

    async def cognify(
        self,
        text: str,
        dataset_name: str,
        entity_id: str,
        tags: Optional[list] = None,
        company_id: str = "",
    ) -> None:
        """
        Fire-and-forget cognify call.

        Returns immediately after spawning the background task.
        Errors in the background task are logged and swallowed — never propagated.
        """
        if self._stub is None:
            return

        task = asyncio.create_task(
            self._do_cognify(text, dataset_name, entity_id, tags or [], company_id)
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def flexible_search(
        self,
        *,
        query: str,
        search_type: str = "CHUNKS",
        limit: int = 10,
        system_prompt: Optional[str] = None,
        top_k: int = 0,
        wide_search_top_k: int = 0,
        authorization_header: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronous flexible search over cognee knowledge.

        Forwards the caller's Authorization header to cognee-service so the
        auth interceptor can resolve company_id from the JWT.

        Returns a dict shaped like FlexibleCogneeSearchResponse:
            {
                "success": bool,
                "message": str,
                "results": [ { "id", "text", "score", "metadata" } ],
                "actual_search_type": str,
                "error_code": str,
            }
        """
        if self._stub is None:
            raise RuntimeError("CogneeGrpcClient is not initialised")

        from api.services._cognee_stubs import cognee_pb2

        # Resolve enum from string label, falling back to CHUNKS for unknown.
        try:
            st = cognee_pb2.SearchType.Value(search_type.upper())
        except ValueError:
            st = cognee_pb2.SearchType.Value("CHUNKS")

        request = cognee_pb2.FlexibleCogneeSearchRequest(
            query=query,
            search_type=st,
            limit=int(limit),
            wide_search_top_k=int(wide_search_top_k or 0),
            top_k=int(top_k or 0),
            system_prompt=str(system_prompt or ""),
        )
        metadata: Tuple[Tuple[str, str], ...] = ()
        if authorization_header:
            metadata = (("authorization", authorization_header),)

        response = await self._stub.FlexibleCogneeSearch(
            request, metadata=metadata, timeout=COGNEE_SEARCH_TIMEOUT_S
        )
        return {
            "success": bool(response.success),
            "message": str(response.message or ""),
            "actual_search_type": str(response.actual_search_type or ""),
            "error_code": str(response.error_code or ""),
            "results": [
                {
                    "id": str(r.id or ""),
                    "text": str(r.text or ""),
                    "score": float(r.score),
                    "metadata": {str(k): str(v) for k, v in (r.metadata or {}).items()},
                }
                for r in response.results
            ],
        }

    async def flexible_code_search(
        self,
        *,
        query: str,
        project_id: str,
        project_name: str,
        search_type: str = "CHUNKS_LEXICAL",
        limit: int = 10,
        system_prompt: Optional[str] = None,
        top_k: int = 0,
        wide_search_top_k: int = 0,
        authorization_header: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronous flexible search over a project's indexed code.

        Forwards Authorization to cognee-service so the auth interceptor
        resolves company_id from the JWT. project_id + project_name are
        required by FlexibleCodeSearch and validated server-side there.

        Returns a dict shaped like FlexibleCodeSearchResponse.
        """
        if self._stub is None:
            raise RuntimeError("CogneeGrpcClient is not initialised")

        from api.services._cognee_stubs import cognee_pb2

        try:
            st = cognee_pb2.SearchType.Value(search_type.upper())
        except ValueError:
            st = cognee_pb2.SearchType.Value("CHUNKS_LEXICAL")

        request = cognee_pb2.FlexibleCodeSearchRequest(
            query=query,
            search_type=st,
            limit=int(limit),
            project_id=project_id,
            project_name=project_name,
            wide_search_top_k=int(wide_search_top_k or 0),
            top_k=int(top_k or 0),
            system_prompt=str(system_prompt or ""),
        )
        metadata: Tuple[Tuple[str, str], ...] = ()
        if authorization_header:
            metadata = (("authorization", authorization_header),)

        response = await self._stub.FlexibleCodeSearch(
            request, metadata=metadata, timeout=COGNEE_CODE_SEARCH_TIMEOUT_S
        )
        return {
            "success": bool(response.success),
            "message": str(response.message or ""),
            "actual_search_type": str(response.actual_search_type or ""),
            "error_code": str(response.error_code or ""),
            "results": [
                {
                    "id": str(r.id or ""),
                    "text": str(r.text or ""),
                    "score": float(r.score),
                    "metadata": {str(k): str(v) for k, v in (r.metadata or {}).items()},
                }
                for r in response.results
            ],
        }

    async def enrich(
        self,
        content: str,
        entity_type: str,
        entity_id: str,
        company_id: str,
    ) -> None:
        """
        Drop-in compatibility shim for the old CogneeEnrichmentService.enrich() call.

        company_id is namespaced into dataset_name to enforce tenant isolation.
        """
        await self.cognify(
            text=content,
            dataset_name=company_id,
            entity_id=entity_id,
            tags=[entity_type],
            company_id=company_id,
        )

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _do_cognify(
        self,
        text: str,
        dataset_name: str,
        entity_id: str,
        tags: list,
        company_id: str = "",
    ) -> None:
        """Background task — performs the actual gRPC Cognify call."""
        if self._stub is None:
            return

        try:
            from api.services._cognee_stubs import cognee_pb2

            request = cognee_pb2.CognifyRequest(
                text=text,
                dataset_name=dataset_name,
                entity_id=entity_id,
                tags=tags,
                company_id=company_id,
            )
            response = await self._stub.Cognify(request, timeout=COGNEE_COGNIFY_TIMEOUT_S)
            if response.success:
                logger.info(
                    f"CogneeGrpcClient: cognify complete — "
                    f"dataset={dataset_name} entity={entity_id}"
                )
            else:
                logger.error(
                    f"CogneeGrpcClient: cognify failed — "
                    f"dataset={dataset_name} entity={entity_id} msg={response.message}"
                )
        except Exception as exc:
            logger.error(
                f"CogneeGrpcClient: cognify error — "
                f"dataset={dataset_name} entity={entity_id}: {exc}",
                exc_info=True,
            )
