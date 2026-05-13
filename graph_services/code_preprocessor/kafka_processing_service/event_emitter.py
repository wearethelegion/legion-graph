"""Kafka event emission for the code intelligence preprocessor.

Encapsulates outbound Kafka event publishing: enriched chunk deletes and
ingestion completion signals.

Phase 4 PRUNE: DataEnrichmentEvent / emit_data_enrichment_event removed —
no consumer exists for the data_enrichment topic. The v2 pipeline uses
enriched-code-chunks topic exclusively.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

from aiokafka import AIOKafkaProducer

from .config import KafkaProcessingSettings

import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class EventEmitter:
    """Publishes ingestion events to Kafka topics.

    Owns enriched chunk delete messages and the ingestion_complete lifecycle signal.
    """

    def __init__(
        self,
        producer: AIOKafkaProducer,
        settings: KafkaProcessingSettings,
    ) -> None:
        self._producer = producer
        self._settings = settings

    # ── Public API ────────────────────────────────────────────────────

    async def emit_enriched_chunk_delete(
        self,
        repository: str,
        branch: str,
        file_path: str,
        ingestion_id: str,
        project_id: str,
        company_id: str,
        file_version_id: str = "",
    ) -> None:
        """Emit a delete message to enriched-code-chunks topic.

        Triggers cleanup of all chunks and entities for the deleted file
        in Qdrant, Neo4j, and Postgres.
        """
        output_topic = os.environ.get("ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")

        delete_message = {
            "action": "delete",
            "company_id": company_id,
            "project_id": project_id,
            "repository": repository,
            "branch": branch,
            "file_path": file_path,
            "ingestion_id": ingestion_id,
            "file_version_id": file_version_id,
        }

        try:
            key_bytes = file_path.encode("utf-8")
            value_bytes = json.dumps(delete_message).encode("utf-8")
            await self._producer.send(output_topic, value=value_bytes, key=key_bytes)
            logger.info("Emitted enriched chunk delete for %s to %s", file_path, output_topic)
        except Exception as exc:
            logger.warning("Failed to emit enriched chunk delete for %s: %s", file_path, exc)

    async def emit_ingestion_complete(
        self,
        ingestion_id: str,
        company_id: str,
        project_id: str,
        total_files: int,
        total_chunks: int,
    ) -> None:
        """Emit ingestion_complete event to pipeline-events topic."""
        from ..enrichment import emit_ingestion_complete

        await emit_ingestion_complete(
            self._producer,
            ingestion_id=ingestion_id,
            company_id=company_id,
            project_id=project_id,
            total_files=total_files,
            total_chunks=total_chunks,
        )
