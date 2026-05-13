"""Runner for Entity Extraction Kafka Consumer.

Entrypoint: python -m cognee_service.kafka_consumer.entity_extraction.runner
"""

import cognee_service.cognee_patches  # noqa: F401 — must be first

import asyncio
import logging
import os
import sys

import asyncpg
import structlog
from dotenv import load_dotenv

from cognee_service.config import configure_cognee
from .config import EntityExtractionConsumerConfig
from .consumer import EntityExtractionKafkaConsumer
from .processor import EntityExtractionProcessor
from ..stats_tracker import CogniStatsTracker
from shared.project_name_resolver import ProjectNameResolver

load_dotenv()

# ── Logging ────────────────────────────────────────────────────
_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=getattr(logging, _log_level, logging.INFO),
)
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, _log_level, logging.INFO)),
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


async def main() -> None:
    """Start the entity extraction Kafka consumer."""
    config = EntityExtractionConsumerConfig
    config.validate()

    logger.info("runner.configuring_cognee")
    configure_cognee()
    logger.info("runner.cognee_configured")

    # Ensure ALL Cognee relational tables exist (idempotent — CREATE IF NOT EXISTS).
    from cognee.infrastructure.databases.relational import (
        create_db_and_tables,
        get_relational_engine,
    )

    await create_db_and_tables()
    logger.info("runner.cognee_tables_ensured")

    # Seed permission records (required for ACL creation on datasets)
    db_engine = get_relational_engine()
    async with db_engine.get_async_session() as session:
        from sqlalchemy import text

        await session.execute(
            text(
                "INSERT INTO permissions (id, name) VALUES "
                "(gen_random_uuid(), 'read'), "
                "(gen_random_uuid(), 'write'), "
                "(gen_random_uuid(), 'delete'), "
                "(gen_random_uuid(), 'share') "
                "ON CONFLICT (name) DO NOTHING"
            )
        )
        await session.commit()
    logger.info("runner.permissions_seeded")

    # Initialize stats tracker (Postgres)
    stats_tracker = None
    try:
        stats_tracker = await CogniStatsTracker.create(config)
        logger.info("runner.stats_tracker_initialized")
    except Exception as e:
        logger.warning("runner.stats_tracker_failed", error=str(e))

    # Initialize metadata writer (Cognee Postgres)
    metadata_writer = None
    try:
        from ..enriched_chunks.metadata_writer import CogneeMetadataWriter

        metadata_writer = await CogneeMetadataWriter.create(config.COGNEE_POSTGRES_DSN)
        logger.info("runner.metadata_writer_initialized")
    except Exception as e:
        logger.warning("runner.metadata_writer_failed", error=str(e))

    # Initialize project name resolver (for node set naming)
    project_resolver = None
    _api_pool = None
    try:
        if config.POSTGRES_DSN:
            _api_pool = await asyncpg.create_pool(config.POSTGRES_DSN, min_size=1, max_size=3)
            project_resolver = ProjectNameResolver(_api_pool)
            logger.info("runner.project_resolver_initialized")
    except Exception as e:
        logger.warning("runner.project_resolver_failed", error=str(e))

    # Initialize processor with custom prompt, metadata writer, and project resolver
    processor = EntityExtractionProcessor(
        custom_prompt_path=config.CUSTOM_PROMPT_PATH,
        metadata_writer=metadata_writer,
        project_resolver=project_resolver,
    )

    # Initialize consumer
    consumer = EntityExtractionKafkaConsumer(
        processor=processor,
        stats_tracker=stats_tracker,
        config=config,
    )
    consumer.setup_signal_handlers()

    try:
        await consumer.start()
        logger.info("runner.consumer_ready")
        await consumer.consume()
    except KeyboardInterrupt:
        logger.info("runner.keyboard_interrupt")
    except Exception as e:
        logger.error("runner.fatal_error", error=str(e), exc_info=True)
        raise
    finally:
        await consumer.stop()
        if stats_tracker:
            await stats_tracker.close()
        if metadata_writer:
            await metadata_writer.close()
        if _api_pool:
            await _api_pool.close()
        logger.info("runner.shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error("runner.exit_error", error=str(e))
        sys.exit(1)
