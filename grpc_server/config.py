"""
gRPC Server Configuration
"""

import os
from dataclasses import dataclass


@dataclass
class GrpcServerConfig:
    """Configuration for gRPC server."""

    # Server settings
    host: str = "0.0.0.0"
    port: int = 50051
    max_workers: int = 10

    # Cache settings
    idempotency_cache_ttl: int = 3600  # 1 hour

    # Health check settings
    enable_health_check: bool = True

    @classmethod
    def from_env(cls) -> "GrpcServerConfig":
        """
        Load configuration from environment variables.

        Returns:
            GrpcServerConfig instance
        """
        return cls(
            host=os.getenv("GRPC_SERVER_HOST", "0.0.0.0"),
            port=int(os.getenv("GRPC_SERVER_PORT", "50051")),
            max_workers=int(os.getenv("GRPC_MAX_WORKERS", "10")),
            idempotency_cache_ttl=int(os.getenv("GRPC_IDEMPOTENCY_CACHE_TTL", "3600")),
            enable_health_check=os.getenv("GRPC_ENABLE_HEALTH_CHECK", "true").lower() == "true",
        )
