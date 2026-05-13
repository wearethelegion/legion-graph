"""Kafka producer service for ingestion requests and brain events."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer

from ..core.config import get_settings

logger = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """Raised when publishing to Kafka fails."""


# Import schema from shared module with backwards compatibility
import sys
from pathlib import Path
from dataclasses import dataclass, asdict

sys.path.append(str(Path(__file__).parent.parent.parent))

# Try to import the new Pydantic schema
try:
    from shared.kafka_schemas import RepositoryIngestionRequest as _PydanticIngestionRequest
    from shared.kafka_schemas import BrainEvent, BRAIN_EVENTS_TOPIC

    PYDANTIC_AVAILABLE = True
except ImportError as _import_err:
    PYDANTIC_AVAILABLE = False
    _PydanticIngestionRequest = None
    BrainEvent = None
    BRAIN_EVENTS_TOPIC = "brain_events"
    logger.warning("shared.kafka_schemas import failed: %s", _import_err)


@dataclass
class RepositoryIngestionMessage:
    """Backwards compatible wrapper that uses new Pydantic schema internally"""

    repository: str
    branch: str
    framework: str
    requested_at: str
    project_id: str  # REQUIRED
    company_id: str  # REQUIRED
    user_id: str = ""
    force_full_refresh: bool = False
    workspace: str = "default"  # Add workspace for new schema

    def to_json(self) -> bytes:
        """Legacy method that internally uses new Pydantic schema if available"""
        if PYDANTIC_AVAILABLE and _PydanticIngestionRequest:
            # Build the new Pydantic model with all metadata
            pydantic_msg = _PydanticIngestionRequest(
                event_id=str(uuid.uuid4()),
                repository=self.repository,
                branch=self.branch,
                framework=self.framework,
                requested_at=self.requested_at,  # Include for compatibility
                force_full_refresh=self.force_full_refresh,
                workspace=self.workspace,
                project_id=self.project_id,
                company_id=self.company_id,
                user_id=self.user_id,
                # These will get default values from schema
                priority=5,
                metadata={},
            )
            # Use the Pydantic model's serialization
            return pydantic_msg.to_json_bytes()
        else:
            # Fallback to simple JSON serialization
            return json.dumps(asdict(self)).encode("utf-8")

    def to_json_bytes(self) -> bytes:
        """Alias for new method name"""
        return self.to_json()


# For new code that expects Pydantic schema
if PYDANTIC_AVAILABLE:
    RepositoryIngestionRequest = _PydanticIngestionRequest
else:
    RepositoryIngestionRequest = RepositoryIngestionMessage


class KafkaProducerService:
    """Thin wrapper around AIOKafkaProducer with lazy initialization."""

    def __init__(self, bootstrap_servers: str, topic: str, client_id: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._client_id = client_id
        self._producer: Optional[AIOKafkaProducer] = None
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        """Start the underlying Kafka producer if it is not running."""
        if self._producer is not None:
            return

        async with self._lock:
            if self._producer is not None:
                return

            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                value_serializer=lambda value: value,
            )
            await producer.start()
            self._producer = producer

    async def stop(self) -> None:
        """Stop the underlying Kafka producer if it is running."""
        if self._producer is None:
            return

        async with self._lock:
            if self._producer is None:
                return

            await self._producer.stop()
            self._producer = None

    async def publish_repository(
        self,
        *,
        repository: str,
        branch: str,
        framework: str,
        project_id: str,
        company_id: str,
        user_id: str = "",
        force_full_refresh: bool = False,
        workspace: str = "default",
    ) -> None:
        """Publish repository details to the configured Kafka topic."""
        if not repository:
            raise KafkaPublishError("Repository name must be provided")

        if not project_id:
            raise KafkaPublishError("Project ID must be provided")

        if not company_id:
            raise KafkaPublishError("Company ID must be provided")

        if self._producer is None:
            raise KafkaPublishError("Kafka producer is not running")

        message = RepositoryIngestionMessage(
            repository=repository,
            branch=branch,
            framework=framework,
            requested_at=datetime.now(timezone.utc).isoformat(),
            project_id=project_id,
            company_id=company_id,
            user_id=user_id,
            force_full_refresh=force_full_refresh,
            workspace=workspace,
        )

        try:
            await self._producer.send_and_wait(self.topic, message.to_json())
        except Exception as exc:  # pragma: no cover - defensive
            raise KafkaPublishError(f"Failed to publish to Kafka: {exc}") from exc

    async def publish_brain_event(
        self,
        *,
        entity_type: str,
        entity_id: str,
        company_id: str,
        project_id: Optional[str] = None,
        text_content: str,
        title: str,
        action: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish a BrainEvent to the brain_events topic.

        Fire-and-forget: logs a warning on failure instead of raising,
        so the caller's request is never blocked by Kafka issues.
        """
        if BrainEvent is None:
            logger.warning(
                "BrainEvent schema not available (shared.kafka_schemas import failed) — skipping brain event for %s/%s",
                entity_type,
                entity_id,
            )
            return

        if self._producer is None:
            logger.warning(
                "Kafka producer not running — skipping brain event for %s/%s",
                entity_type,
                entity_id,
            )
            return

        settings = get_settings()
        topic = settings.kafka_brain_events_topic

        event = BrainEvent(
            event_id=str(uuid.uuid4()),
            entity_type=entity_type,
            entity_id=entity_id,
            company_id=company_id,
            project_id=project_id,
            text_content=text_content,
            title=title,
            action=action,
            metadata=metadata or {},
        )

        try:
            await self._producer.send_and_wait(topic, event.to_json_bytes())
        except Exception:
            logger.warning(
                "Failed to publish brain event for %s/%s — event dropped",
                entity_type,
                entity_id,
                exc_info=True,
            )


# Singleton-style helpers for FastAPI dependency injection
_service_instance: Optional[KafkaProducerService] = None


def _get_or_create_service() -> KafkaProducerService:
    global _service_instance

    if _service_instance is None:
        settings = get_settings()
        _service_instance = KafkaProducerService(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_incoming_topic,
            client_id=settings.kafka_client_id,
        )
    return _service_instance


async def init_kafka_service() -> KafkaProducerService:
    service = _get_or_create_service()
    await service.start()
    return service


async def shutdown_kafka_service() -> None:
    global _service_instance
    if _service_instance is None:
        return

    await _service_instance.stop()
    _service_instance = None


async def get_kafka_service() -> KafkaProducerService:
    service = _get_or_create_service()
    if not service.is_running:
        await service.start()
    return service
