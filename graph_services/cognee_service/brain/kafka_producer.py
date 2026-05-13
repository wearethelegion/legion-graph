"""
Brain v2 — Kafka Producer for brain_events topic

Thin AIOKafkaProducer wrapper.  Every Brain CRUD mutation publishes
a BrainEvent so the Cognee enrichment pipeline can process it async.

Fire-and-forget: log warning on failure, never block the RPC response.

Uses BrainEvent schema from shared/kafka_schemas.py.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS - comma-separated broker list (default: redpanda:9092)
"""

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog

logger = structlog.get_logger(__name__)

# ── Import BrainEvent from shared schemas ────────────────────────────────────

try:
    from shared.kafka_schemas import BrainEvent, BRAIN_EVENTS_TOPIC
except ImportError:
    # Fallback: define locally if shared package not available in this container
    logger.warning(
        "brain.kafka_schemas_import_fallback",
        detail="shared.kafka_schemas not available, using local BrainEvent",
    )

    from pydantic import BaseModel, Field
    import json

    BRAIN_EVENTS_TOPIC = "brain_events"

    class BrainEvent(BaseModel):  # type: ignore[no-redef]
        event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
        event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        schema_version: str = "1.0.0"
        entity_type: str = ""
        entity_id: str = ""
        company_id: str = ""
        project_id: Optional[str] = None
        text_content: str = ""
        title: str = ""
        metadata: Dict[str, Any] = Field(default_factory=dict)
        action: str = ""

        def to_json_bytes(self) -> bytes:
            return json.dumps(self.model_dump(), default=str).encode("utf-8")


# ── Producer singleton ───────────────────────────────────────────────────────

_producer = None


async def get_producer():
    """Return the initialised AIOKafkaProducer, creating it lazily."""
    global _producer
    if _producer is None:
        await _init_producer()
    return _producer


async def _init_producer():
    """Create and start the AIOKafkaProducer."""
    global _producer

    try:
        from aiokafka import AIOKafkaProducer
    except ImportError:
        logger.error("brain.aiokafka_not_installed", detail="pip install aiokafka")
        return

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

    _producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
        acks="all",
        request_timeout_ms=5000,
        retry_backoff_ms=200,
    )

    try:
        await _producer.start()
        logger.info("brain.kafka_producer_started", bootstrap=bootstrap)
    except Exception as exc:
        logger.error("brain.kafka_producer_start_failed", error=str(exc))
        _producer = None


async def publish_brain_event(
    *,
    entity_type: str,
    entity_id: str,
    company_id: str,
    action: str,
    title: str = "",
    text_content: str = "",
    project_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Publish a BrainEvent to the brain_events topic.

    Fire-and-forget: logs warning on failure, never raises.
    """
    try:
        producer = await get_producer()
        if producer is None:
            logger.warning(
                "brain.kafka_producer_unavailable", entity_type=entity_type, entity_id=entity_id
            )
            return

        event = BrainEvent(
            event_id=str(uuid.uuid4()),
            entity_type=entity_type,
            entity_id=entity_id,
            company_id=company_id,
            project_id=None,
            text_content=text_content,
            title=title,
            metadata=metadata or {},
            action=action,
        )

        await producer.send(
            BRAIN_EVENTS_TOPIC,
            value=event.to_json_bytes(),
            key=entity_id.encode("utf-8"),
        )

        logger.debug(
            "brain.event_published",
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
        )
    except Exception as exc:
        # Fire-and-forget: never block the RPC
        logger.warning(
            "brain.event_publish_failed",
            entity_type=entity_type,
            entity_id=entity_id,
            error=str(exc),
        )


async def shutdown_producer() -> None:
    """Gracefully stop the Kafka producer."""
    global _producer
    if _producer is not None:
        try:
            await _producer.stop()
            logger.info("brain.kafka_producer_stopped")
        except Exception as exc:
            logger.warning("brain.kafka_producer_stop_failed", error=str(exc))
        finally:
            _producer = None
