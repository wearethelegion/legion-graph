"""
Brain v2 CRUD module for the Cognee gRPC microservice.

Provides Postgres-first storage for KGRAG knowledge entities
(knowledge, expertise, lessons, engagements, entries) with Kafka
async enrichment via the brain_events topic.

Infrastructure:
  - db.py            Asyncpg connection pool for KGRAG Postgres
  - kafka_producer.py Thin AIOKafkaProducer wrapper for brain_events
  - servicer.py      BrainServicer with 29 RPC implementations
"""

from cognee_service.brain.servicer import BrainServicer

__all__ = ["BrainServicer"]
