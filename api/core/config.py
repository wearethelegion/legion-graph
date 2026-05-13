"""
Configuration management for Code Intelligence Service

Updated to use unified ConfigManager following Core Principle #2 (single global configuration manager)
"""

import os
from pydantic import BaseModel


class Settings(BaseModel):
    """
    Application settings loaded from ConfigManager
    Updated to use unified ConfigManager following Core Principle #2 (no environment auto-loading)
    """

    # model_config = ConfigDict(extra='ignore')

    # # Service Configuration (loaded from ConfigManager)
    # service_name: str
    # service_version: str
    # debug: bool

    # API Configuration (loaded from ConfigManager)
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_prefix: str = "/api/v1"

    # Kafka messaging configuration
    kafka_bootstrap_servers: str = "redpanda:9092"
    kafka_incoming_topic: str = "incoming_requests"
    kafka_brain_events_topic: str = "brain_events"
    kafka_client_id: str = "ingestion-api"

    # MongoDB configuration
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "code_intel")

    # # File Processing (loaded from ConfigManager)
    # max_file_size: int
    # temp_directory: str
    # output_directory: str

    # LightRAG Integration (loaded from ConfigManager)
    # lightrag_url: Optional[str]
    # lightrag_timeout: int

    # GitIngest Integration (loaded from ConfigManager)
    # gitingest_url: Optional[str]

    # Processing Configuration (loaded from ConfigManager)
    max_concurrent_tasks: int = 2
    # task_cleanup_hours: int

    # Webhook Configuration
    github_webhook_secret: str = os.getenv("GITHUB_WEBHOOK_SECRET", "")
    gitlab_webhook_secret: str = os.getenv("GITLAB_WEBHOOK_SECRET", "")

    # Workspace Configuration
    workspace_default: str = os.getenv("WORKSPACE_DEFAULT", "code_intel")

    # # Security (loaded from ConfigManager)
    # api_key: Optional[str]
    # cors_origins: list

    model_config = {
        "env_file": ".env",
    }


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings"""
    return settings
