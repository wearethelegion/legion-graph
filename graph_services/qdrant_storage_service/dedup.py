"""Pipeline counter validation for Qdrant Storage Service (Service 5).

All entity/edge data flows via Kafka — no Postgres staging tables.
This module retains only counter-consistency validation against pipeline_counters.

The entity_id helper is kept for deterministic UUID5 generation
used elsewhere in the pipeline.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple
from uuid import UUID, uuid5, NAMESPACE_OID

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


def entity_id(name: str, node_set: str) -> UUID:
    """Generate deterministic UUID5 from entity name + node_set.

    Uses Cognee's generate_node_id normalisation: .lower().replace(" ", "_").replace("'", "")
    Must stay in sync with entity_extraction_service and Cognee.
    """
    return uuid5(
        NAMESPACE_OID,
        f"{name}|{node_set}".lower().replace(" ", "_").replace("'", ""),
    )


def entity_name_to_uuid(name: str, node_set: str) -> UUID:
    """Backward-compatible alias for entity_id."""
    return entity_id(name, node_set)


class EntityDeduplicator:
    """Pipeline counter validator.

    The asyncpg pool is injected — this class never creates or closes it.

    NOTE: Entity/edge dedup methods were removed — all entity and edge data
    now flows through Kafka, not Postgres staging tables.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def validate_counter_consistency(
        self,
        ingestion_id: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Validate counter consistency across all Phase A services.

        Checks that chunks_received counts match across services.
        Returns (is_valid, counter_summary).
        """
        rows = await self._pool.fetch(
            """
            SELECT service_name, counter_name, counter_value, status
              FROM pipeline_counters
             WHERE ingestion_id = $1
             ORDER BY service_name, counter_name
            """,
            ingestion_id,
        )

        counters: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            svc = row["service_name"]
            counters.setdefault(svc, {})[row["counter_name"]] = {
                "value": row["counter_value"],
                "status": row["status"],
            }

        # Preprocessor total_chunks should match extraction/summarization received
        preprocessor_total = (
            counters.get("preprocessor", {}).get("chunks_produced", {}).get("value", 0)
        )
        extraction_received = (
            counters.get("entity_extraction", {}).get("chunks_received", {}).get("value", 0)
        )
        summarization_received = (
            counters.get("summarization", {}).get("chunks_received", {}).get("value", 0)
        )

        summary = {
            "preprocessor_chunks": preprocessor_total,
            "extraction_received": extraction_received,
            "summarization_received": summarization_received,
            "all_services": counters,
        }

        is_valid = True
        if preprocessor_total > 0:
            if extraction_received != preprocessor_total:
                is_valid = False
            if summarization_received != preprocessor_total:
                is_valid = False

        return is_valid, summary
