#!/usr/bin/env python3
"""Unified entry point for Code Intelligence Preprocessor."""

import os
import sys
import asyncio
import logging

import asyncpg

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_repository_processor():
    """Run the repository ingestion processor (default mode)."""
    from code_preprocessor.kafka_processing_service.consumer import (
        RepositoryIngestionConsumer,
    )

    # Create asyncpg pool for DB access (GitHub token lookup, version tracking, etc.)
    db_pool = None
    db_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    if db_url:
        try:
            db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
            logger.info("Database pool created for project token lookup")
        except Exception as exc:
            logger.warning("Failed to create database pool: %s — continuing without DB", exc)

    logger.info("Starting Repository Ingestion Processor")
    consumer = RepositoryIngestionConsumer(db_pool=db_pool)

    try:
        async with consumer:
            await consumer.run()
    finally:
        if db_pool:
            await db_pool.close()


async def main():
    """Main entry point - selects processor based on environment."""
    processor_mode = os.getenv("PREPROCESSOR_MODE", "repository").lower()
    logger.info("Preprocessor mode: %s", processor_mode)

    try:
        await run_repository_processor()
    except KeyboardInterrupt:
        logger.info("Preprocessor interrupted by user")
    except Exception as e:
        logger.error("Processor failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
