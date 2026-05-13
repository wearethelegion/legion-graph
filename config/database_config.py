#!/usr/bin/env python3
"""
Shared database configuration for code intelligence preprocessor services
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Shared database configuration - using unified environment variables"""

    # MongoDB settings (loaded from environment via ConfigManager)
    use_mongodb: bool = True
    mongodb_host: str = "localhost"
    mongodb_port: int = 27017
    mongodb_username: str = ""  # Load from environment, never hardcode
    mongodb_password: str = ""  # Load from environment, never hardcode
    mongodb_database: str = "code_intel"
    mongodb_auth_database: str = "admin"

    # Connection settings
    mongodb_timeout_ms: int = 5000
    mongodb_max_pool_size: int = 10
    mongodb_min_pool_size: int = 1
    
    def validate(self) -> None:
        """Validate database configuration"""
        if self.use_mongodb:
            if not self.mongodb_host:
                raise ValueError("mongodb_host cannot be empty")
            
            if not self.mongodb_database:
                raise ValueError("mongodb_database cannot be empty")
            
            if self.mongodb_port <= 0 or self.mongodb_port > 65535:
                raise ValueError("mongodb_port must be between 1 and 65535")
    
    def get_mongodb_uri(self) -> Optional[str]:
        """Get MongoDB connection URI"""
        if not self.use_mongodb:
            return None
            
        if self.mongodb_username and self.mongodb_password:
            from urllib.parse import quote_plus
            username = quote_plus(self.mongodb_username)
            password = quote_plus(self.mongodb_password)
            return f"mongodb://{username}:{password}@{self.mongodb_host}:{self.mongodb_port}/{self.mongodb_auth_database}"
        else:
            return f"mongodb://{self.mongodb_host}:{self.mongodb_port}/{self.mongodb_auth_database}"
