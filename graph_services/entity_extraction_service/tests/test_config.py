"""Tests for EntityExtractionConfig."""

import os
from unittest.mock import patch

import pytest


class TestEntityExtractionConfig:
    """Test config defaults and env var parsing."""

    def _fresh_config(self):
        """Import a fresh config class (re-evaluate class attrs from env)."""
        # Config reads os.getenv at class definition time, so we need
        # to reload the module to pick up patched env vars.
        import importlib
        import entity_extraction_service.config as cfg_module

        importlib.reload(cfg_module)
        return cfg_module.EntityExtractionConfig

    # ── Default values ───────────────────────────────────────────

    def test_default_kafka_bootstrap(self):
        cfg = self._fresh_config()
        assert cfg.KAFKA_BOOTSTRAP_SERVERS == os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

    def test_default_input_topic(self):
        cfg = self._fresh_config()
        assert cfg.KAFKA_INPUT_TOPIC == os.getenv(
            "ENTITY_EXTRACTION_INPUT_TOPIC", "enriched-code-chunks"
        )

    def test_default_output_topic(self):
        cfg = self._fresh_config()
        assert cfg.KAFKA_OUTPUT_TOPIC == os.getenv(
            "ENTITY_EXTRACTION_OUTPUT_TOPIC", "extracted-entities"
        )

    def test_default_consumer_group(self):
        cfg = self._fresh_config()
        assert cfg.KAFKA_CONSUMER_GROUP_ID == os.getenv(
            "ENTITY_EXTRACTION_CONSUMER_GROUP_ID", "entity-extraction-processor-v2"
        )

    def test_default_batch_size(self):
        cfg = self._fresh_config()
        expected = int(os.getenv("ENTITY_EXTRACTION_BATCH_SIZE", "50"))
        assert cfg.BATCH_SIZE == expected

    def test_default_max_workers(self):
        cfg = self._fresh_config()
        expected = int(os.getenv("ENTITY_EXTRACTION_MAX_WORKERS", "50"))
        assert cfg.MAX_PARALLEL_WORKERS == expected

    def test_default_max_retries(self):
        cfg = self._fresh_config()
        expected = int(os.getenv("ENTITY_EXTRACTION_MAX_RETRIES", "3"))
        assert cfg.MAX_RETRIES == expected

    def test_default_retry_base_delay(self):
        cfg = self._fresh_config()
        expected = float(os.getenv("ENTITY_EXTRACTION_RETRY_BASE_DELAY", "2.0"))
        assert cfg.RETRY_BASE_DELAY == expected

    def test_default_auto_commit(self):
        cfg = self._fresh_config()
        expected = os.getenv("ENTITY_EXTRACTION_KAFKA_AUTO_COMMIT", "true").lower() == "true"
        assert cfg.KAFKA_AUTO_COMMIT == expected

    def test_default_auto_offset_reset(self):
        cfg = self._fresh_config()
        assert cfg.KAFKA_AUTO_OFFSET_RESET == os.getenv(
            "ENTITY_EXTRACTION_KAFKA_AUTO_OFFSET_RESET", "earliest"
        )

    def test_default_fetch_timeout(self):
        cfg = self._fresh_config()
        expected = int(os.getenv("ENTITY_EXTRACTION_KAFKA_FETCH_TIMEOUT_MS", "1000"))
        assert cfg.KAFKA_FETCH_TIMEOUT_MS == expected

    def test_default_postgres_pool_sizes(self):
        cfg = self._fresh_config()
        assert cfg.POSTGRES_MIN_POOL == int(os.getenv("ENTITY_EXTRACTION_PG_MIN_POOL", "2"))
        assert cfg.POSTGRES_MAX_POOL == int(os.getenv("ENTITY_EXTRACTION_PG_MAX_POOL", "10"))

    def test_default_log_level(self):
        cfg = self._fresh_config()
        assert cfg.LOG_LEVEL == os.getenv("LOG_LEVEL", "INFO")

    # ── Env var override ─────────────────────────────────────────

    def test_env_override_batch_size(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_BATCH_SIZE": "100"}):
            cfg = self._fresh_config()
            assert cfg.BATCH_SIZE == 100

    def test_env_override_max_workers(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_MAX_WORKERS": "25"}):
            cfg = self._fresh_config()
            assert cfg.MAX_PARALLEL_WORKERS == 25

    def test_env_override_max_retries(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_MAX_RETRIES": "5"}):
            cfg = self._fresh_config()
            assert cfg.MAX_RETRIES == 5

    def test_env_override_kafka_bootstrap(self):
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "kafka:29092"}):
            cfg = self._fresh_config()
            assert cfg.KAFKA_BOOTSTRAP_SERVERS == "kafka:29092"

    def test_env_override_auto_commit_false(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_KAFKA_AUTO_COMMIT": "false"}):
            cfg = self._fresh_config()
            assert cfg.KAFKA_AUTO_COMMIT is False

    # ── Validation ───────────────────────────────────────────────

    def test_validate_passes_with_defaults(self):
        with patch.dict(
            os.environ, {"CODE_PROCESSING_POSTGRES_DSN": "postgresql://test:test@localhost/test"}
        ):
            cfg = self._fresh_config()
            # Should not raise
            cfg.validate()

    def test_validate_fails_on_zero_batch_size(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_BATCH_SIZE": "0"}):
            cfg = self._fresh_config()
            with pytest.raises(ValueError, match="BATCH_SIZE must be >= 1"):
                cfg.validate()

    def test_validate_fails_on_zero_max_workers(self):
        with patch.dict(os.environ, {"ENTITY_EXTRACTION_MAX_WORKERS": "0"}):
            cfg = self._fresh_config()
            with pytest.raises(ValueError, match="MAX_WORKERS must be >= 1"):
                cfg.validate()
