"""Code Changes Kafka Consumer.

Consumes code change events from the data_enrichment Kafka topic,
decodes base64 content, and analyzes code using CodeServiceV2.

Public API:
    ConsumerRunner: Main orchestrator
    CodeChangesConsumer: Kafka consumer
    MessageHandler: Business logic
    CodeChangesConsumerConfig: Configuration

Example usage:
    ```python
    from api.consumers.code_changes_consumer import ConsumerRunner

    runner = ConsumerRunner()
    await runner.run()
    ```

Or run directly:
    ```bash
    python -m api.consumers.code_changes_consumer.runner
    ```
"""

from .config import CodeChangesConsumerConfig
from .consumer import CodeChangesConsumer
from .message_handler import MessageHandler
from .runner import ConsumerRunner

__all__ = [
    "ConsumerRunner",
    "CodeChangesConsumer",
    "MessageHandler",
    "CodeChangesConsumerConfig",
]

__version__ = "1.0.0"
