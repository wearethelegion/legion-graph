"""Cognee Registration Kafka Consumer.

Subscribes to enriched-code-chunks topic and calls cognee.add() for each
message via CogneeRegistrationProcessor.

Consumer group: cognee-registration-group — independent from:
- cognee-enriched-chunks-processor (entity extraction + graph)
- other consumers on the same topic

Design:
- Processes messages one-by-one (not batched) to keep cognee.add() simple
- Uses its own asyncio.Lock inside the processor (not re-entrant from here)
- Skip "delete" action messages — handled by the enriched chunks consumer
- Errors in cognee.add() are swallowed by the processor; the consumer loop
  continues regardless
"""

import asyncio
import json
import signal
from typing import Optional

from aiokafka import AIOKafkaConsumer
import structlog

from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage
from .config import CogneeRegistrationConsumerConfig
from .processor import CogneeRegistrationProcessor

logger = structlog.get_logger(__name__)


class CogneeRegistrationKafkaConsumer:
    """Kafka consumer that reads EnrichedChunkMessage and calls cognee.add()."""

    def __init__(
        self,
        processor: Optional[CogneeRegistrationProcessor] = None,
        config: type[CogneeRegistrationConsumerConfig] = CogneeRegistrationConsumerConfig,
    ):
        self.processor = processor or CogneeRegistrationProcessor()
        self.config = config
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Initialize and start Kafka consumer."""
        if self.consumer is not None:
            logger.warning("consumer.already_started")
            return

        logger.info(
            "consumer.starting",
            topic=self.config.KAFKA_TOPIC,
            group=self.config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap=self.config.KAFKA_BOOTSTRAP_SERVERS,
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
        logger.info("consumer.started")

    async def stop(self) -> None:
        """Stop Kafka consumer gracefully."""
        if self.consumer is None:
            return

        logger.info("consumer.stopping")
        self._running = False
        self._shutdown_event.set()

        try:
            await self.consumer.stop()
            logger.info("consumer.stopped")
        except Exception as e:
            logger.error("consumer.stop_error", error=str(e))
        finally:
            self.consumer = None

    async def consume(self) -> None:
        """Main consumption loop.

        Reads messages from the topic and calls cognee.add() for each
        "process" action message. Errors in cognee.add() are swallowed
        by the processor — the loop always continues.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True

        try:
            while self._running:
                messages = await self.consumer.getmany(
                    timeout_ms=self.config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=100,
                )

                for partition_msgs in messages.values():
                    for msg in partition_msgs:
                        event = msg.value
                        if not isinstance(event, EnrichedChunkMessage):
                            logger.error(
                                "consumer.invalid_message_type",
                                type=str(type(event)),
                            )
                            continue

                        if not event.company_id or (
                            event.content_type == "code" and not event.project_id
                        ):
                            logger.warning(
                                "consumer.missing_tenant_ids",
                                file_path=event.file_path,
                                chunk_id=getattr(event, "chunk_id", None),
                            )
                            continue

                        # Delete actions: skip — not calling cognee.add()
                        # Remove is handled by the enriched chunks consumer
                        if event.action == "delete":
                            continue

                        await self._process_with_retries(event)

                # Idle check: if shutdown was requested and no messages, exit
                if not messages and self._shutdown_event.is_set():
                    break

        except asyncio.CancelledError:
            logger.info("consumer.cancelled")
        except Exception as e:
            logger.error("consumer.fatal_error", error=str(e), exc_info=True)
            raise
        finally:
            logger.info("consumer.loop_exited")

    async def _process_with_retries(self, event: EnrichedChunkMessage) -> None:
        """Process a single message with retries.

        cognee.add() failures are caught inside the processor and only
        logged — we retry here to handle transient Neo4j or network errors
        that may be raised before cognee.add() itself is called.
        """
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                await self.processor.register(event)
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    "consumer.retry",
                    attempt=attempt,
                    max_retries=self.config.MAX_RETRIES,
                    file_path=event.file_path,
                    error=str(e),
                )
                if attempt < self.config.MAX_RETRIES:
                    delay = self.config.RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

        logger.error(
            "consumer.message_failed",
            file_path=event.file_path,
            chunk_id=event.chunk_id,
            max_retries=self.config.MAX_RETRIES,
            last_error=str(last_error),
        )
        # Do not re-raise — continue processing the next message

    def _deserialize_message(self, raw_value: bytes) -> EnrichedChunkMessage:
        """Deserialize Kafka message bytes to EnrichedChunkMessage."""
        try:
            data = json.loads(raw_value.decode("utf-8"))
            return EnrichedChunkMessage(**data)
        except Exception as e:
            logger.error("consumer.deserialize_error", error=str(e))
            raise ValueError(f"Invalid message format: {e}") from e

    def setup_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM for graceful shutdown."""

        def handler(signum, frame):
            logger.info("consumer.signal_received", signal=signum)
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
