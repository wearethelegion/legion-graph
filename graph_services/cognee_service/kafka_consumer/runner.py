"""Runner for Cogni Kafka Consumer.

Entrypoint: python -m cognee_service.kafka_consumer.runner
"""

import cognee_service.cognee_patches  # noqa: F401 — must be first

import asyncio
import logging
import os
import sys

import structlog
from dotenv import load_dotenv

from cognee_service.config import configure_cognee
from .config import CogniConsumerConfig
from .consumer import CogniKafkaConsumer
from .processor import CogneeProcessor
from .stats_tracker import CogniStatsTracker

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
    """Start the Cogni Kafka consumer."""
    config = CogniConsumerConfig
    config.validate()

    logger.info("runner.configuring_cognee")
    configure_cognee()
    logger.info("runner.cognee_configured")

    # Ensure Cognee relational tables exist (idempotent — CREATE IF NOT EXISTS).
    # Must run AFTER configure_cognee() so the DB URL is set.
    from cognee.infrastructure.databases.relational import create_db_and_tables

    await create_db_and_tables()
    logger.info("runner.db_tables_ensured")

    # Initialize stats tracker (Postgres)
    stats_tracker = None
    try:
        stats_tracker = await CogniStatsTracker.create(config)
        logger.info("runner.stats_tracker_initialized")
    except Exception as e:
        logger.warning("runner.stats_tracker_failed", error=str(e))

    # Initialize processor
    processor = CogneeProcessor()

    # Initialize consumer
    consumer = CogniKafkaConsumer(
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
        logger.info("runner.shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error("runner.exit_error", error=str(e))
        sys.exit(1)
