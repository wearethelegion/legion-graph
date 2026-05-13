"""Kafka consumer for code change events.

Consumes messages from data_enrichment topic and delegates
processing to MessageHandler.
"""

import asyncio
import json
import signal
from typing import Any

from aiokafka import AIOKafkaConsumer
from loguru import logger
from shared.kafka_schemas import DataEnrichmentEvent

from .config import CodeChangesConsumerConfig
from .message_handler import MessageHandler


class CodeChangesConsumer:
    """Kafka consumer for code change events."""

    def __init__(
        self,
        message_handler: MessageHandler,
        config: type[CodeChangesConsumerConfig] = CodeChangesConsumerConfig,
    ):
        """Initialize consumer.

        Args:
            message_handler: Message handler instance
            config: Configuration class
        """
        self.message_handler = message_handler
        self.config = config
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Initialize and start Kafka consumer."""
        if self.consumer is not None:
            logger.warning("Consumer already started")
            return

        logger.info(
            f"Starting Kafka consumer: "
            f"topic={self.config.KAFKA_TOPIC}, "
            f"group={self.config.KAFKA_CONSUMER_GROUP_ID}, "
            f"bootstrap={self.config.KAFKA_BOOTSTRAP_SERVERS}"
        )

        self.consumer = AIOKafkaConsumer(
            self.config.KAFKA_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.KAFKA_CONSUMER_GROUP_ID,
            enable_auto_commit=self.config.KAFKA_AUTO_COMMIT,
            auto_offset_reset=self.config.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=self._deserialize_message,
        )

        await self.consumer.start()
        logger.success("Kafka consumer started successfully")

    async def stop(self) -> None:
        """Stop Kafka consumer gracefully."""
        if self.consumer is None:
            return

        logger.info("Stopping Kafka consumer...")
        self._running = False
        self._shutdown_event.set()

        try:
            await self.consumer.stop()
            logger.success("Kafka consumer stopped successfully")
        except Exception as e:
            logger.error("Error stopping consumer: {}", e)
        finally:
            self.consumer = None

    async def consume(self) -> None:
        """Streaming consumer loop with dynamic worker pool.

        Continuously processes messages with backpressure management.
        Runs until stop() is called or SIGINT/SIGTERM received.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True
        logger.info(
            f"Starting streaming message consumption "
            f"(max_workers={self.config.MAX_CONCURRENT_WORKERS}, "
            f"timeout={self.config.FETCH_TIMEOUT_MS}ms)..."
        )

        # Track active tasks for backpressure
        active_tasks = set()

        try:
            while self._running:
                # Clean completed tasks (non-blocking)
                if active_tasks:
                    done_tasks = {task for task in active_tasks if task.done()}
                    for task in done_tasks:
                        try:
                            await task  # Retrieve any exceptions
                        except Exception as e:
                            logger.error("Task failed: {}", e, exc_info=True)
                    active_tasks -= done_tasks

                # Only fetch if we have capacity
                current_load = len(active_tasks)
                if current_load < self.config.MAX_CONCURRENT_WORKERS:
                    # Fetch messages (non-blocking with short timeout)
                    available_capacity = self.config.MAX_CONCURRENT_WORKERS - current_load
                    messages = await self.consumer.getmany(
                        timeout_ms=self.config.FETCH_TIMEOUT_MS,
                        max_records=available_capacity,
                    )

                    # Create tasks for new messages (non-blocking)
                    for partition_msgs in messages.values():
                        for msg in partition_msgs:
                            task = asyncio.create_task(self._process_message(msg))
                            active_tasks.add(task)

                    if messages:
                        total_fetched = sum(len(msgs) for msgs in messages.values())
                        logger.debug(
                            f"Fetched {total_fetched} message(s), "
                            f"active tasks: {len(active_tasks)}/{self.config.MAX_CONCURRENT_WORKERS}"
                        )
                else:
                    # At capacity, wait briefly before checking again
                    await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception as e:
            logger.error("Fatal error in consumer loop: {}", e, exc_info=True)
            raise
        finally:
            # Graceful shutdown: wait for active tasks to complete
            if active_tasks:
                logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
                await asyncio.gather(*active_tasks, return_exceptions=True)

            logger.info("Consumer loop exited")

    async def _process_message(self, message: Any) -> None:
        """Process a single Kafka message.

        Args:
            message: Kafka message from AIOKafkaConsumer
        """
        try:
            event = message.value
            if not isinstance(event, DataEnrichmentEvent):
                logger.error(
                    f"Invalid message type: {type(event)}. "
                    f"Expected DataEnrichmentEvent"
                )
                return

            logger.debug(
                f"Received message: offset={message.offset}, "
                f"partition={message.partition}, "
                f"event_id={event.event_id}"
            )

            # Process message with retries
            result = await self._process_with_retries(event)

            # Log result
            if result["status"] == "success":
                logger.success(
                    f"Processed {event.file_path}: "
                    f"nodes={result.get('neo4j_nodes', 0)}, "
                    f"vectors={result.get('qdrant_points', 0)}, "
                    f"duration={result.get('duration', 0):.2f}s"
                )
            elif result["status"] == "skipped":
                logger.debug(
                    f"Skipped {event.file_path}: {result.get('reason', 'unknown')}"
                )
            else:  # error
                logger.error(
                    f"Failed {event.file_path}: {result.get('error', 'unknown')}"
                )

        except Exception as e:
            logger.error(
                f"Error processing message at offset {message.offset}: {e}",
                exc_info=True
            )
            # Continue processing next message (don't crash consumer)

    async def _process_with_retries(
        self, event: DataEnrichmentEvent
    ) -> dict[str, Any]:
        """Process event with retry logic.

        Args:
            event: Data enrichment event

        Returns:
            Processing result
        """
        last_error = None

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                result = await self.message_handler.handle_message(event)
                return result

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Attempt {attempt}/{self.config.MAX_RETRIES} failed "
                    f"for {event.file_path}: {e}"
                )

                if attempt < self.config.MAX_RETRIES:
                    # Exponential backoff
                    delay = self.config.RETRY_DELAY * (2 ** (attempt - 1))
                    logger.debug(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)

        # All retries exhausted
        return {
            "status": "error",
            "error": f"Failed after {self.config.MAX_RETRIES} retries: {last_error}",
            "event_id": event.event_id,
            "file_path": event.file_path,
        }

    def _deserialize_message(self, raw_value: bytes) -> DataEnrichmentEvent:
        """Deserialize Kafka message to DataEnrichmentEvent.

        Handles both legacy messages (without tenant IDs) and new messages.

        Args:
            raw_value: Raw message bytes from Kafka

        Returns:
            Deserialized event

        Raises:
            ValueError: If deserialization fails
        """
        try:
            data = json.loads(raw_value.decode("utf-8"))
            event = DataEnrichmentEvent(**data)

            # Warn about legacy messages without tenant IDs
            if not event.project_id or not event.company_id:
                logger.warning(
                    f"Legacy message without tenant IDs: {event.file_path} "
                    f"(event_id={data.get('event_id', 'unknown')})"
                )

            return event

        except Exception as e:
            logger.error("Failed to deserialize message: {}", e)
            raise ValueError(f"Invalid message format: {e}") from e

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        logger.debug("Signal handlers registered (SIGINT, SIGTERM)")
