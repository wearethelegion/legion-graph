"""Kafka consumer for BrainEvent messages from brain_events topic.

Owns Kafka lifecycle (consumer/producer start/stop), message deserialization,
and delegates processing to DocumentProcessor.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.kafka_schemas import BrainEvent

from document_preprocessor.config import DocumentPreprocessorSettings, get_settings
from document_preprocessor.event_emitter import DocumentEventEmitter
from document_preprocessor.processor import DocumentProcessor

logger = logging.getLogger(__name__)


class DocumentPreprocessorConsumer:
    """Consume BrainEvent messages and route through document preprocessor."""

    def __init__(
        self,
        settings: Optional[DocumentPreprocessorSettings] = None,
        db_pool: Optional[asyncpg.Pool] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._db_pool = db_pool
        self._processor: Optional[DocumentProcessor] = None

    async def __aenter__(self) -> "DocumentPreprocessorConsumer":
        await self.start()
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        """Start Kafka consumer and producer, initialise processor."""
        if self._consumer is not None:
            return

        logger.info(
            "Starting document preprocessor consumer: topic=%s bootstrap=%s group=%s",
            self._settings.kafka_input_topic,
            self._settings.kafka_bootstrap_servers,
            self._settings.kafka_group_id,
        )

        # Ensure Postgres schema exists
        if self._db_pool:
            await _ensure_schema(self._db_pool)

        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_input_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=_deserialize_message,
        )
        await self._consumer.start()

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=self._settings.kafka_producer_client_id,
            max_request_size=self._settings.kafka_max_request_size,
            value_serializer=lambda value: value,
        )
        await self._producer.start()

        emitter = DocumentEventEmitter(self._producer, self._settings)
        self._processor = DocumentProcessor(
            settings=self._settings,
            emitter=emitter,
            db_pool=self._db_pool,
        )

    async def stop(self) -> None:
        """Stop consumer and producer."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def run(self) -> None:
        """Main consume loop — process messages one at a time."""
        if self._consumer is None:
            raise RuntimeError("Consumer must be started before run()")

        async for message in self._consumer:
            payload: Dict[str, Any] = message.value
            if not payload:
                continue

            # Parse BrainEvent
            try:
                event = BrainEvent(**payload)
            except Exception as exc:
                logger.warning(
                    "doc_consumer.invalid_message: %s (error=%s)",
                    str(payload)[:200],
                    exc,
                )
                continue

            # Validate required fields
            if not event.entity_id or not event.company_id:
                logger.warning(
                    "doc_consumer.missing_fields: entity_id=%s company_id=%s",
                    event.entity_id,
                    event.company_id,
                )
                continue

            # Process
            try:
                result = await self._processor.process(event)
                log_level = logging.DEBUG if result.get("skipped") else logging.INFO
                logger.log(
                    log_level,
                    "doc_consumer.processed: action=%s entity=%s/%s result=%s",
                    event.action,
                    event.entity_type,
                    event.entity_id,
                    result.get("status"),
                )
            except Exception as exc:
                logger.error(
                    "doc_consumer.process_failed: entity=%s/%s action=%s error=%s",
                    event.entity_type,
                    event.entity_id,
                    event.action,
                    exc,
                    exc_info=True,
                )


def _deserialize_message(value: bytes) -> Dict[str, Any]:
    """Deserialize Kafka message value from JSON bytes."""
    try:
        return json.loads(value.decode("utf-8"))
    except json.JSONDecodeError:
        logger.error("Failed to decode Kafka message: %r", value[:200])
        return {}


async def _ensure_schema(pool: asyncpg.Pool) -> None:
    """Create document_processing schema and tables if they don't exist."""
    try:
        async with pool.acquire() as conn:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS document_processing")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS document_processing.document_versions (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    company_id TEXT NOT NULL,
                    project_id TEXT NOT NULL DEFAULT '',
                    content_hash TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    chunk_count INTEGER NOT NULL DEFAULT 0,
                    ingestion_id TEXT NOT NULL DEFAULT '',
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS document_processing.document_chunks (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    version_id UUID REFERENCES document_processing.document_versions(id),
                    chunk_index INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    chunk_hash TEXT NOT NULL,
                    section_heading TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Indexes for common lookups
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doc_versions_entity
                    ON document_processing.document_versions (entity_id, deleted)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doc_versions_company
                    ON document_processing.document_versions (company_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doc_chunks_version
                    ON document_processing.document_chunks (version_id)
            """)
        logger.info("document_processing schema and tables ready")
    except Exception as exc:
        logger.error("Failed to initialise document_processing schema: %s", exc)
