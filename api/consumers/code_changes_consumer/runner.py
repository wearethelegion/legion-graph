"""Runner for Code Changes Consumer.

Orchestrates initialization of all dependencies and starts the consumer.
Can be run as a standalone script or imported.
"""

import asyncio
import sys
from loguru import logger

from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.services.code_service_v2 import CodeAnalyzerService

from .config import CodeChangesConsumerConfig
from .consumer import CodeChangesConsumer
from .ingestion_metrics import IngestionMetricsTracker
from .message_handler import MessageHandler


class ConsumerRunner:
    """Orchestrates consumer startup and shutdown."""

    def __init__(self):
        """Initialize runner."""
        self.config = CodeChangesConsumerConfig
        self.consumer: CodeChangesConsumer | None = None
        self.code_service: CodeAnalyzerService | None = None
        self.metrics_tracker: IngestionMetricsTracker | None = None

    async def run(self) -> None:
        """Run the consumer.

        Initializes all dependencies, starts consumer, and runs until
        interrupted.
        """
        # Validate configuration
        self.config.validate()

        # Configure logging
        logger.remove()  # Remove default handler
        logger.add(
            sys.stderr,
            level=self.config.LOG_LEVEL,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )

        logger.info("=" * 80)
        logger.info("Starting Code Changes Consumer")
        logger.info("=" * 80)
        logger.info(f"Kafka Topic: {self.config.KAFKA_TOPIC}")
        logger.info(f"Consumer Group: {self.config.KAFKA_CONSUMER_GROUP_ID}")
        logger.info(f"Bootstrap Servers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Skip Deleted Files: {self.config.SKIP_DELETED_FILES}")
        logger.info("=" * 80)

        try:
            # Initialize repositories
            logger.info("Initializing repositories...")
            neo4j_repo = Neo4jRepository()
            qdrant_repo = QdrantRepository()

            # Note: ProjectRepository not needed for consumer since we use
            # default tenant IDs from config (not database lookup).
            # In production, implement proper tenant ID lookup from workspace.

            # Initialize code analyzer service
            logger.info("Initializing Code Analyzer Service...")
            self.code_service = CodeAnalyzerService(
                neo4j_repository=neo4j_repo,
                qdrant_repository=qdrant_repo,
                project_repository=None,  # Not used in consumer context
            )

            # Initialize ingestion metrics tracker
            logger.info("Initializing Ingestion Metrics Tracker...")
            try:
                self.metrics_tracker = IngestionMetricsTracker(config=self.config)
                logger.info("Ingestion metrics tracker initialized")
            except Exception as e:
                logger.warning("Failed to initialize metrics tracker: {}. LLM stats will not be tracked.", e)
                self.metrics_tracker = None

            # Initialize message handler
            logger.info("Initializing Message Handler...")
            message_handler = MessageHandler(
                code_analyzer_service=self.code_service,
                config=self.config,
                metrics_tracker=self.metrics_tracker,
            )

            # Initialize consumer
            logger.info("Initializing Kafka Consumer...")
            self.consumer = CodeChangesConsumer(
                message_handler=message_handler,
                config=self.config,
            )

            # Setup signal handlers
            self.consumer.setup_signal_handlers()

            # Start consumer
            await self.consumer.start()

            # Run consumer loop
            logger.success("Consumer ready - waiting for messages...")
            await self.consumer.consume()

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error("Fatal error: {}", e, exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Shutdown consumer and cleanup resources."""
        logger.info("Shutting down...")

        # Stop consumer
        if self.consumer:
            await self.consumer.stop()

        # Cleanup code service
        if self.code_service:
            await self.code_service.close()

        # Cleanup metrics tracker
        if self.metrics_tracker:
            self.metrics_tracker.close()

        logger.success("Shutdown complete")


async def main() -> None:
    """Main entry point."""
    runner = ConsumerRunner()
    await runner.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")
        sys.exit(0)
    except Exception as e:
        logger.error("Fatal error: {}", e, exc_info=True)
        sys.exit(1)
