"""Runner for Cognee Registration Kafka Consumer.

Entrypoint: python -m cognee_service.kafka_consumer.cognee_registration.runner

This consumer subscribes to enriched-code-chunks and calls cognee.add() for
each chunk, registering file content in Cognee's metadata tables so that
cognee.search() can discover these files.

It uses consumer group cognee-registration-group, independent from other
consumers on the same topic.
"""

import cognee_service.cognee_patches  # noqa: F401 — must be first

import asyncio
import logging
import os
import sys

import structlog
from dotenv import load_dotenv

from cognee_service.config import configure_cognee
from .config import CogneeRegistrationConsumerConfig
from .consumer import CogneeRegistrationKafkaConsumer
from .processor import CogneeRegistrationProcessor

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
    """Start the cognee registration Kafka consumer."""
    config = CogneeRegistrationConsumerConfig
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

    processor = CogneeRegistrationProcessor()
    consumer = CogneeRegistrationKafkaConsumer(
        processor=processor,
        config=config,
    )
    consumer.setup_signal_handlers()

    try:
        await consumer.start()
        logger.info(
            "runner.consumer_ready",
            group=config.KAFKA_CONSUMER_GROUP_ID,
            topic=config.KAFKA_TOPIC,
        )
        await consumer.consume()
    except KeyboardInterrupt:
        logger.info("runner.keyboard_interrupt")
    except Exception as e:
        logger.error("runner.fatal_error", error=str(e), exc_info=True)
        raise
    finally:
        await consumer.stop()
        logger.info("runner.shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error("runner.exit_error", error=str(e))
        sys.exit(1)
