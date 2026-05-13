"""Ingestion metrics tracking for LLM analysis results.

Writes LLM success/error counts back to MongoDB ingestion documents.
"""

import os
from typing import Optional
from urllib.parse import quote_plus

from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from .config import CodeChangesConsumerConfig


class IngestionMetricsTracker:
    """Track LLM analysis metrics in MongoDB ingestion documents."""

    def __init__(self, config: type[CodeChangesConsumerConfig] = CodeChangesConsumerConfig):
        """Initialize tracker with MongoDB connection.

        Args:
            config: Configuration class with MongoDB settings
        """
        self.config = config
        self._client: Optional[MongoClient] = None
        self._collection: Optional[Collection] = None

    def _get_collection(self) -> Collection:
        """Get or create MongoDB connection to ingestions collection."""
        if self._collection is None:
            host = self.config.MONGODB_HOST
            port = self.config.MONGODB_PORT
            username = self.config.MONGODB_USERNAME
            password = self.config.MONGODB_PASSWORD
            database = self.config.MONGODB_DATABASE
            auth_db = self.config.MONGODB_AUTH_DATABASE

            if username and password:
                uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{auth_db}"
            else:
                uri = f"mongodb://{host}:{port}/{auth_db}"

            logger.info(f"Connecting to MongoDB at {host}:{port} for ingestion metrics")
            self._client = MongoClient(uri)
            self._collection = self._client[database]["ingestions"]
            logger.info("MongoDB ingestions collection initialized for metrics tracking")

        return self._collection

    def record_llm_success(self, ingestion_id: str) -> bool:
        """Record a successful LLM analysis.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            True if update succeeded
        """
        if not ingestion_id:
            return False

        try:
            collection = self._get_collection()
            result = collection.update_one(
                {"ingestion_id": ingestion_id},
                {"$inc": {"llm_successful": 1}}
            )
            return result.modified_count > 0
        except PyMongoError as exc:
            logger.warning(
                f"Failed to record LLM success for ingestion {ingestion_id}: {exc}"
            )
            return False

    def record_llm_error(self, ingestion_id: str) -> bool:
        """Record an LLM analysis error.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            True if update succeeded
        """
        if not ingestion_id:
            return False

        try:
            collection = self._get_collection()
            result = collection.update_one(
                {"ingestion_id": ingestion_id},
                {"$inc": {"llm_errors": 1}}
            )
            return result.modified_count > 0
        except PyMongoError as exc:
            logger.warning(
                f"Failed to record LLM error for ingestion {ingestion_id}: {exc}"
            )
            return False

    def record_llm_fallback(self, ingestion_id: str) -> bool:
        """Record a file that used fallback text chunker instead of LLM.

        Note: This is also tracked by the preprocessor as files_llm_fallback.
        This method can be used for additional tracking if needed.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            True if update succeeded
        """
        if not ingestion_id:
            return False

        try:
            collection = self._get_collection()
            result = collection.update_one(
                {"ingestion_id": ingestion_id},
                {"$inc": {"llm_fallback": 1}}
            )
            return result.modified_count > 0
        except PyMongoError as exc:
            logger.warning(
                f"Failed to record LLM fallback for ingestion {ingestion_id}: {exc}"
            )
            return False

    def close(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None
            logger.info("Ingestion metrics MongoDB connection closed")
