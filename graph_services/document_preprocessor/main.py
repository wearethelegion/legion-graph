"""Document Preprocessor Service — entry point.

Consumes BrainEvent messages from brain_events topic, chunks document text,
and produces EnrichedChunkMessage messages to enriched-code-chunks topic.
"""

import asyncio
import logging
import os
import signal
import sys

import asyncpg

from document_preprocessor.consumer import DocumentPreprocessorConsumer
from document_preprocessor.db_init import init_document_extraction_prompts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def create_db_pool() -> asyncpg.Pool | None:
    """Initialise Postgres connection pool."""
    postgres_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if not postgres_url:
        logger.warning("DATABASE_URL/POSTGRES_URL not set — document version tracking disabled")
        return None

    try:
        pool = await asyncpg.create_pool(dsn=postgres_url, min_size=2, max_size=10)
        logger.info("Database pool initialised")
        return pool
    except Exception as exc:
        logger.error("Failed to create database pool: %s", exc)
        return None


async def main() -> None:
    """Main entry point."""
    logger.info("Starting Document Preprocessor Service")

    db_pool = await create_db_pool()

    # Ensure code_processing.document_extraction_prompts exists and is seeded.
    # Idempotent — uses CREATE TABLE IF NOT EXISTS + INSERT ... ON CONFLICT.
    # Without this, every BrainEvent fails with prompt_load_failed and search
    # returns NoDataError ("No valid chunks loaded") forever.
    try:
        await init_document_extraction_prompts(db_pool)
    except Exception as exc:
        logger.error(
            "Document extraction prompts bootstrap failed (non-fatal, "
            "but ingestion will not produce chunks): %s",
            exc,
            exc_info=True,
        )

    consumer = DocumentPreprocessorConsumer(db_pool=db_pool)

    # Graceful shutdown via signals
    stop_event = asyncio.Event()

    def signal_handler(sig, _frame):
        logger.info("Received signal %s, initiating shutdown...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await consumer.start()
        logger.info("Consumer started, processing messages...")

        consumer_task = asyncio.create_task(consumer.run())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [consumer_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

    except Exception as exc:
        logger.error("Service failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        await consumer.stop()
        if db_pool:
            await db_pool.close()
            logger.info("Database pool closed")
        logger.info("Document Preprocessor Service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as exc:
        logger.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
