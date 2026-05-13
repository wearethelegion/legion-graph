"""Entrypoint for Entity Extraction Service (Service 2).

Usage:
    python -m entity_extraction_service.main
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

from .config import EntityExtractionConfig
from .consumer import EntityExtractionConsumer
from .pipeline_store import EntityExtractionStore
from .processor import EntityExtractionProcessor

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
    """Start the Entity Extraction Service."""
    config = EntityExtractionConfig
    config.validate()

    logger.info(
        "main.starting",
        input_topic=config.KAFKA_INPUT_TOPIC,
        output_topic=config.KAFKA_OUTPUT_TOPIC,
        batch_size=config.BATCH_SIZE,
        max_workers=config.MAX_PARALLEL_WORKERS,
    )

    # Configure Cognee (LLM settings, env vars)
    logger.info("main.configuring_cognee")
    configure_cognee()
    logger.info("main.cognee_configured")

    # Initialize Postgres connection pool
    logger.info("main.connecting_postgres", dsn=config.POSTGRES_DSN[:30] + "...")
    pool = await asyncpg.create_pool(
        config.POSTGRES_DSN,
        min_size=config.POSTGRES_MIN_POOL,
        max_size=config.POSTGRES_MAX_POOL,
    )
    logger.info("main.postgres_connected")

    # Initialize store and ensure tables exist
    store = EntityExtractionStore(pool)
    await store.ensure_tables()
    logger.info("main.tables_ensured")

    # Initialize processor
    processor = EntityExtractionProcessor(
        store=store,
        config=config,
    )

    # Initialize consumer
    consumer = EntityExtractionConsumer(
        processor=processor,
        config=config,
    )
    consumer.setup_signal_handlers()

    try:
        await consumer.start()
        logger.info("main.consumer_ready")
        await consumer.consume()
    except KeyboardInterrupt:
        logger.info("main.keyboard_interrupt")
    except Exception as e:
        logger.error("main.fatal_error", error=str(e), exc_info=True)
        raise
    finally:
        try:
            await consumer.stop()
        except Exception as e:
            logger.error("cleanup.consumer_stop_failed", error=str(e))
        try:
            await pool.close()
        except Exception as e:
            logger.error("cleanup.pool_close_failed", error=str(e))
        logger.info("main.shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error("main.exit_error", error=str(e))
        sys.exit(1)
