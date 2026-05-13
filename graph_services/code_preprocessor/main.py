"""
Code Intelligence Preprocessor Service

This service consumes repository ingestion messages from Kafka and preprocesses them
for enrichment by other services.
"""

import os
import sys
import signal
import asyncio
import logging
import asyncpg

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def create_db_pool() -> asyncpg.Pool:
    """Initialize database connection pool."""
    postgres_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if not postgres_url:
        logger.warning("DATABASE_URL/POSTGRES_URL not set - GitHub token lookup from DB disabled")
        return None

    try:
        pool = await asyncpg.create_pool(dsn=postgres_url, min_size=2, max_size=10)
        logger.info("Database pool initialized for token lookup")
        return pool
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        return None


async def main():
    """Main entry point"""
    # Use the proper RepositoryIngestionConsumer that handles repository messages
    from code_preprocessor.kafka_processing_service.consumer import (
        RepositoryIngestionConsumer,
    )
    from code_preprocessor.storage.db_init import ensure_tracking_tables

    logger.info("Starting Code Intelligence Preprocessor Service")
    logger.info("Using RepositoryIngestionConsumer for processing incoming_requests topic")

    # Initialize database pool
    db_pool = await create_db_pool()

    # Initialize tracking tables if pool is available
    if db_pool:
        try:
            await ensure_tracking_tables(db_pool)
        except Exception as e:
            logger.error(f"Failed to initialize tracking tables: {e}", exc_info=True)

    # Create and start the consumer
    consumer = RepositoryIngestionConsumer(db_pool=db_pool)

    # Set up signal handlers for graceful shutdown
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the consumer
        await consumer.start()
        logger.info("Consumer started successfully, processing messages...")

        # Run consumer until stop signal
        consumer_task = asyncio.create_task(consumer.run())
        stop_task = asyncio.create_task(stop_event.wait())

        # Wait for either consumer to finish or stop signal
        done, pending = await asyncio.wait(
            [consumer_task, stop_task], return_when=asyncio.FIRST_COMPLETED
        )

        # Cancel pending tasks
        for task in pending:
            task.cancel()

    except Exception as e:
        logger.error(f"Service failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Clean shutdown
        await consumer.stop()
        if db_pool:
            await db_pool.close()
            logger.info("Database pool closed")
        logger.info("Preprocessor service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
