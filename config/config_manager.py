#!/usr/bin/env python3
"""
Global Configuration Manager for Code Intelligence Preprocessor

Unified configuration manager that loads all settings from environment variables
and provides service-specific configuration objects.

Following Core Principle #1 (ONE CLASS → ONE FILE) and Core Principle #2 (YAGNI OOD principle)
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum

from .database_config import DatabaseConfig


@dataclass
class KafkaConfig:
    """Kafka configuration for all services"""
    bootstrap_servers: str = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'redpanda:9092'))
    incoming_topic: str = field(default_factory=lambda: os.getenv('KAFKA_INCOMING_TOPIC', 'incoming_requests'))
    data_enrichment_topic: str = field(default_factory=lambda: os.getenv('KAFKA_DATA_ENRICHMENT_TOPIC', 'data_enrichment'))
    consumer_group_id: str = field(default_factory=lambda: os.getenv('KAFKA_GROUP_ID', 'code-preprocessor'))
    producer_client_id: str = field(default_factory=lambda: os.getenv('KAFKA_PRODUCER_CLIENT_ID', 'code-preprocessor-producer'))
    max_request_size: int = field(default_factory=lambda: int(os.getenv('KAFKA_MAX_REQUEST_SIZE', str(8 * 1024 * 1024))))


# Collection name constants
class CollectionNames:
    """MongoDB collection name constants"""
    SCIP_DOCUMENTS = "scip_documents"
    INGESTED_DOCUMENTS = "ingested_documents"  # Renamed from "gitingest_documents"
    REPOSITORY_VERSIONS = "repository_versions"


@dataclass
class GlobalConfig:
    """Global application configuration"""
    project_name: str = field(default_factory=lambda: os.getenv('PROJECT_NAME', 'vetlyx-code-intelligence'))
    environment: str = field(default_factory=lambda: os.getenv('ENVIRONMENT', 'development'))
    debug: bool = field(default_factory=lambda: os.getenv('DEBUG', 'False').lower() == 'true')
    branch: str = field(default_factory=lambda: os.getenv('BRANCH', 'main'))
    output_dir: str = field(default_factory=lambda: os.getenv('OUTPUT_DIR', './output'))
    batch_size: int = field(default_factory=lambda: int(os.getenv('BATCH_SIZE', '100')))
    max_file_size_mb: int = field(default_factory=lambda: int(os.getenv('MAX_FILE_SIZE_MB', '50')))
    dry_run: bool = field(default_factory=lambda: os.getenv('DRY_RUN', 'False').lower() == 'true')


@dataclass
class SCIPConfig:
    """SCIP Processing Service Configuration"""
    processing_mode: str = field(default_factory=lambda: os.getenv('SCIP_PROCESSING_MODE', 'both'))

@dataclass
class GitIngestConfig:
    """GitIngest Processing Service Configuration"""
    repo_url: str = field(default_factory=lambda: os.getenv('GITHUB_REPO_URL', ''))
    github_token: str = field(default_factory=lambda: os.getenv('GITHUB_TOKEN', ''))
    api_url: str = field(default_factory=lambda: "http://localhost:8000/api/ingest")
    
    # File filtering settings
    exclude_patterns: List[str] = field(default_factory=lambda: [
        # Version control
        '.git/', '.gitignore', '.gitkeep', '.gitmodules', '.gitattributes',
        # Dependencies  
        'node_modules/', 'vendor/', 'bower_components/', 'packages/',
        # Build artifacts
        'dist/', 'build/', 'target/', 'out/', '.next/', 'coverage/',
        # Cache and temp
        '.cache/', '.tmp/', 'tmp/', '__pycache__/', '.pytest_cache/', '.tox/',
        # IDE and editor files
        '.vscode/', '.idea/', '*.swp', '*.swo', '*~', '*.bak',
        # OS files
        '.DS_Store', 'Thumbs.db', 'desktop.ini',
        # Resources and assets
        'resources/', 'assets/images/', 'public/images/', 'static/images/',
        # Log files
        '*.log', 'logs/', 'log/',
        # Documentation (non-code)
        '*.md', '*.txt', '*.pdf', '*.doc', '*.docx',
        # Binary files
        '*.exe', '*.bin', '*.dll', '*.so', '*.dylib',
        # Image/media files  
        '*.jpg', '*.jpeg', '*.png', '*.gif', '*.svg', '*.ico',
        '*.mp4', '*.avi', '*.mov', '*.mp3', '*.wav',
        # Archive files
        '*.zip', '*.tar', '*.gz', '*.rar', '*.7z',
        # Package manager files
        'package-lock.json', 'yarn.lock', 'Gemfile.lock', 'composer.lock'
    ])
    
    # Include only specific file types (empty means include all)
    include_file_types: List[str] = field(default_factory=list)
    
    def should_exclude_file(self, file_path: str) -> tuple[bool, Optional[str]]:
        """
        Check if a file should be excluded based on patterns
        
        Args:
            file_path: Path to check
            
        Returns:
            tuple: (should_exclude, exclusion_reason)
        """
        file_path_lower = file_path.lower()
        
        # Check include patterns first (if specified)
        if self.include_file_types:
            from pathlib import Path
            file_ext = Path(file_path).suffix.lower()
            if file_ext not in self.include_file_types:
                return True, f"file_type_not_included:{file_ext}"
        
        # Check exclude patterns
        for pattern in self.exclude_patterns:
            if pattern.endswith('/'):
                # Directory pattern
                if f"/{pattern}" in f"/{file_path_lower}" or file_path_lower.startswith(pattern):
                    return True, f"directory_pattern:{pattern}"
            elif '*' in pattern:
                # Wildcard pattern
                if pattern.startswith('*.'):
                    extension = pattern[1:]  # Remove *
                    if file_path_lower.endswith(extension):
                        return True, f"extension_pattern:{extension}"
            else:
                # Exact match or substring
                if pattern in file_path_lower or file_path_lower.endswith(f"/{pattern}"):
                    return True, f"exact_pattern:{pattern}"
        
        return False, None

# LightRAGConfig removed - using EnhancedConfig instead

@dataclass
class IngestionAPIConfig:
    """Ingestion API Service Configuration"""
    service_name: str = "Code Intelligence Service"
    service_version: str = "1.0.0"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_prefix: str = "/api/v1"
    max_file_size: int = 104857600  # 100MB
    temp_directory: str = "/tmp/scip_processing"
    output_directory: str = "./processed_scip"
    lightrag_url: Optional[str] = None
    lightrag_timeout: int = 300
    mongodb_url: Optional[str] = None
    mongodb_database: str = "code_intelligence"
    gitingest_url: Optional[str] = None
    max_concurrent_tasks: int = 5
    task_cleanup_hours: int = 24
    api_key: Optional[str] = None
    cors_origins: List[str] = field(default_factory=lambda: ["*"])


class ConfigManager:
    """
    Global configuration manager for all services
    
    Following Core Principle #2 (single global configuration manager instead of multiple service-specific configs)
    """
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration manager"""
        self._env_file = env_file or ".env"
        self._load_env_file()

        # Initialize all configurations
        self.global_config = self._load_global_config()
        self.database_config = self._load_database_config()
        self.kafka_config = self._load_kafka_config()
        self.scip_config = self._load_scip_config()
        self.gitingest_config = self._load_gitingest_config()
        # self.lightrag_config = self._load_lightrag_config()  # Removed - using EnhancedConfig
        self.api_config = self._load_api_config()
    
    def _load_env_file(self) -> None:
        """Load environment variables from .env file if it exists"""
        env_path = Path(self._env_file)
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(env_path)
    
    def _get_env(self, key: str, default: Any = None, cast_type: type = str) -> Any:
        """Get environment variable with type casting"""
        value = os.getenv(key, default)
        
        if value is None:
            return default
            
        if cast_type == bool:
            return str(value).lower() in ('true', '1', 'yes', 'on')
        elif cast_type == int:
            return int(value)
        elif cast_type == float:
            return float(value)
        elif cast_type == list:
            # If value is already a list (from default), return as-is
            if isinstance(value, list):
                return value
            # Handle JSON-like list strings
            if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                import json
                return json.loads(value)
            return value.split(',') if value else []
        else:
            return cast_type(value)
    
    def _load_global_config(self) -> GlobalConfig:
        """Load global configuration from environment"""
        return GlobalConfig(
            project_name=self._get_env('PROJECT_NAME', 'vetlyx-code-intelligence'),
            environment=self._get_env('ENVIRONMENT', 'development'),
            debug=self._get_env('DEBUG', False, bool),
            branch=self._get_env('BRANCH', 'main'),
            output_dir=self._get_env('OUTPUT_DIR', './output'),
            batch_size=self._get_env('BATCH_SIZE', 100, int),
            max_file_size_mb=self._get_env('MAX_FILE_SIZE_MB', 50, int),
            dry_run=self._get_env('DRY_RUN', False, bool)
        )
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment"""
        # Handle MongoDB URL override
        mongodb_url = self._get_env('MONGODB_URL')
        if mongodb_url:
            # Parse MongoDB URL to extract components
            from urllib.parse import urlparse
            parsed = urlparse(mongodb_url)
            return DatabaseConfig(
                use_mongodb=True,
                mongodb_host=parsed.hostname or 'localhost',
                mongodb_port=parsed.port or 27017,
                mongodb_username=parsed.username or '',
                mongodb_password=parsed.password or '',
                mongodb_database=parsed.path.lstrip('/') or 'code_knowledge_preprocessor',
                mongodb_auth_database=parsed.path.lstrip('/') or 'admin'
            )

        return DatabaseConfig(
            use_mongodb=self._get_env('USE_MONGODB', True, bool),
            mongodb_host=self._get_env('MONGODB_HOST', 'localhost'),
            mongodb_port=self._get_env('MONGODB_PORT', 27017, int),
            mongodb_username=self._get_env('MONGODB_USERNAME', 'data_preprocessor_user'),
            mongodb_password=self._get_env('MONGODB_PASSWORD', 'data_preprocessor_password'),
            mongodb_database=self._get_env('MONGODB_DATABASE', 'code_knowledge_preprocessor'),
            mongodb_auth_database=self._get_env('MONGODB_AUTH_DATABASE', 'admin'),
            mongodb_timeout_ms=self._get_env('MONGODB_TIMEOUT_MS', 5000, int),
            mongodb_max_pool_size=self._get_env('MONGODB_MAX_POOL_SIZE', 10, int),
            mongodb_min_pool_size=self._get_env('MONGODB_MIN_POOL_SIZE', 1, int)
        )

    def _load_kafka_config(self) -> KafkaConfig:
        """Load Kafka configuration from environment"""
        return KafkaConfig(
            bootstrap_servers=self._get_env('KAFKA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
            incoming_topic=self._get_env('KAFKA_INCOMING_TOPIC', 'incoming_requests'),
            data_enrichment_topic=self._get_env('KAFKA_DATA_ENRICHMENT_TOPIC', 'data_enrichment'),
            consumer_group_id=self._get_env('KAFKA_GROUP_ID', 'code-preprocessor'),
            producer_client_id=self._get_env('KAFKA_PRODUCER_CLIENT_ID', 'code-preprocessor-producer'),
            max_request_size=self._get_env('KAFKA_MAX_REQUEST_SIZE', 8 * 1024 * 1024, int)
        )
    
    def _load_scip_config(self) -> SCIPConfig:
        """Load SCIP processing configuration from environment"""
        config = SCIPConfig(
            # processing_mode=ProcessingMode(self._get_env('SCIP_PROCESSING_MODE', 'both')),
            # max_memory_mb=self._get_env('SCIP_MAX_MEMORY_MB', 1024, int),
            # batch_size=self._get_env('SCIP_BATCH_SIZE', 1000, int),
            # max_file_size_mb=self._get_env('SCIP_MAX_FILE_SIZE_MB', 500, int),
            # fail_fast=self._get_env('SCIP_FAIL_FAST', False, bool),
            # max_errors=self._get_env('SCIP_MAX_ERRORS', 100, int),
            # fail_if_no_source=self._get_env('SCIP_FAIL_IF_NO_SOURCE', False, bool),
            # log_progress_interval=self._get_env('SCIP_LOG_PROGRESS_INTERVAL', 1000, int),
            # include_source_in_scip=self._get_env('SCIP_INCLUDE_SOURCE_IN_SCIP', False, bool),
            # json_indent=self._get_env('SCIP_JSON_INDENT', 2, int),
            # ensure_ascii=self._get_env('SCIP_ENSURE_ASCII', False, bool),
            # output_dir=self._get_env('SCIP_OUTPUT_DIR', './scip_processing_temp'),
            # default_branch=self._get_env('SCIP_DEFAULT_BRANCH', 'main')
        )
        
        # Load source and path fields from environment if provided
        source_fields_env = self._get_env('SCIP_SOURCE_FIELDS')
        if source_fields_env:
            config.source_fields = source_fields_env.split(',') if isinstance(source_fields_env, str) else source_fields_env
        
        path_fields_env = self._get_env('SCIP_PATH_FIELDS')
        if path_fields_env:
            config.path_fields = path_fields_env.split(',') if isinstance(path_fields_env, str) else path_fields_env
        
        return config
    
    def _load_gitingest_config(self) -> GitIngestConfig:
        """Load GitIngest processing configuration from environment"""
        config = GitIngestConfig(
            repo_url=self._get_env('GITHUB_REPO_URL', ''),
            github_token=self._get_env('GITHUB_TOKEN', ''),
            api_url=self._get_env('GITINGEST_API_URL', 'http://localhost:8000/api/ingest'),
            # polling_interval=self._get_env('GITINGEST_POLLING_INTERVAL', 5, int),
            # max_wait_time=self._get_env('GITINGEST_MAX_WAIT_TIME', 300, int),
            # request_timeout=self._get_env('GITINGEST_REQUEST_TIMEOUT', 30, int),
            # processing_mode=ProcessingMode(self._get_env('GITINGEST_PROCESSING_MODE', 'both')),
            # batch_size=self._get_env('GITINGEST_BATCH_SIZE', 100, int),
            # max_file_size_mb=self._get_env('GITINGEST_MAX_FILE_SIZE_MB', 50, int),
            # output_dir=self._get_env('GITINGEST_OUTPUT_DIR', './gitingest_processing_temp'),
            # default_branch=self._get_env('GITINGEST_DEFAULT_BRANCH', 'main')
        )
        
        # Load file filtering patterns from environment if provided
        exclude_patterns_env = self._get_env('GITINGEST_EXCLUDE_PATTERNS')
        if exclude_patterns_env:
            config.exclude_patterns = exclude_patterns_env.split(',') if isinstance(exclude_patterns_env, str) else exclude_patterns_env
        
        include_types_env = self._get_env('GITINGEST_INCLUDE_FILE_TYPES')
        if include_types_env:
            config.include_file_types = include_types_env.split(',') if isinstance(include_types_env, str) else include_types_env
        
        return config
    
    # def _load_lightrag_config removed - using EnhancedConfig instead
    
    def _load_api_config(self) -> IngestionAPIConfig:
        """Load Ingestion API configuration from environment"""
        return IngestionAPIConfig(
            service_name=self._get_env('CI_SERVICE_NAME', 'Code Intelligence Service'),
            service_version=self._get_env('CI_SERVICE_VERSION', '1.0.0'),
            api_host=self._get_env('CI_API_HOST', '0.0.0.0'),
            api_port=self._get_env('CI_API_PORT', 8000, int),
            api_prefix=self._get_env('CI_API_PREFIX', '/api/v1'),
            max_file_size=self._get_env('CI_MAX_FILE_SIZE', 104857600, int),
            temp_directory=self._get_env('CI_TEMP_DIRECTORY', '/tmp/scip_processing'),
            output_directory=self._get_env('CI_OUTPUT_DIRECTORY', './processed_scip'),
            lightrag_url=self._get_env('CI_LIGHTRAG_URL'),
            lightrag_timeout=self._get_env('CI_LIGHTRAG_TIMEOUT', 300, int),
            mongodb_url=self._get_env('CI_MONGODB_URL'),
            mongodb_database=self._get_env('CI_MONGODB_DATABASE', 'code_intelligence'),
            gitingest_url=self._get_env('CI_GITINGEST_URL'),
            max_concurrent_tasks=self._get_env('CI_MAX_CONCURRENT_TASKS', 5, int),
            task_cleanup_hours=self._get_env('CI_TASK_CLEANUP_HOURS', 24, int),
            api_key=self._get_env('CI_API_KEY'),
            cors_origins=self._get_env('CI_CORS_ORIGINS', ["*"], list)
        )
    
    def validate(self) -> None:
        """Validate all configurations"""
        self.database_config.validate()
        
        # Validate required fields based on configuration
        if self.database_config.use_mongodb and not self.global_config.project_name:
            raise ValueError("project_name is required when using MongoDB")
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get summary of all configurations"""
        return {
            "global": {
                "project_name": self.global_config.project_name,
                "environment": self.global_config.environment,
                "debug": self.global_config.debug
            },
            "database": {
                "use_mongodb": self.database_config.use_mongodb,
                "mongodb_host": self.database_config.mongodb_host,
                "mongodb_database": self.database_config.mongodb_database
            },
            "kafka": {
                "bootstrap_servers": self.kafka_config.bootstrap_servers,
                "incoming_topic": self.kafka_config.incoming_topic,
                "data_enrichment_topic": self.kafka_config.data_enrichment_topic
            },
            "scip": {
                "processing_mode": self.scip_config.processing_mode
            },
            "gitingest": {
                "api_url": self.gitingest_config.api_url,
                "exclude_patterns_count": len(self.gitingest_config.exclude_patterns),
                "include_file_types": self.gitingest_config.include_file_types
            },
            "api": {
                "service_name": self.api_config.service_name,
                "api_port": self.api_config.api_port,
                "mongodb_database": self.api_config.mongodb_database
            }
        }


# Global instance - singleton pattern following Core Principle #2 (single global configuration manager)
_global_config_manager: Optional[ConfigManager] = None


def get_config_manager(env_file: Optional[str] = None) -> ConfigManager:
    """Get or create global configuration manager instance"""
    global _global_config_manager
    
    if _global_config_manager is None:
        _global_config_manager = ConfigManager(env_file)
    
    return _global_config_manager


def reset_config_manager() -> None:
    """Reset global configuration manager (useful for testing)"""
    global _global_config_manager
    _global_config_manager = None
