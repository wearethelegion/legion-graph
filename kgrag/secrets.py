"""
Secrets management for Kgrag.
Supports multiple backends: environment variables, Docker secrets, Vault.
Implements graceful fallback chain with security best practices.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from enum import Enum
import json
from loguru import logger


class SecretBackend(Enum):
    """Available secret backends"""
    ENVIRONMENT = "environment"
    DOCKER_SECRETS = "docker_secrets"
    VAULT = "vault"


class SecretsManager:
    """
    Multi-backend secrets manager with graceful fallback.

    Fallback chain: Vault → Docker Secrets → Environment → None

    Security features:
    - Never logs actual secret values
    - Caching for performance (in-memory only)
    - Graceful degradation
    - Clear audit trail
    """

    def __init__(
        self,
        backends: Optional[list[SecretBackend]] = None,
        docker_secrets_path: Path = Path("/run/secrets"),
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        cache_enabled: bool = True
    ):
        """
        Initialize secrets manager.

        Args:
            backends: List of backends to use (in priority order).
                     Defaults to [DOCKER_SECRETS, ENVIRONMENT]
            docker_secrets_path: Path to Docker secrets directory
            vault_url: Vault server URL (if using Vault)
            vault_token: Vault authentication token (if using Vault)
            cache_enabled: Enable in-memory caching of secrets
        """
        self.backends = backends or [
            SecretBackend.DOCKER_SECRETS,
            SecretBackend.ENVIRONMENT
        ]
        self.docker_secrets_path = docker_secrets_path
        self.vault_url = vault_url
        self.vault_token = vault_token
        self.cache_enabled = cache_enabled
        self._cache: Dict[str, Any] = {}
        self._vault_client = None

        # Initialize Vault client if configured
        if SecretBackend.VAULT in self.backends:
            self._init_vault_client()

        logger.info(
            f"SecretsManager initialized with backends: {[b.value for b in self.backends]}"
        )

    def _init_vault_client(self):
        """Initialize Vault client if available"""
        if not self.vault_url or not self.vault_token:
            logger.warning("Vault backend requested but URL/token not provided")
            return

        try:
            import hvac
            self._vault_client = hvac.Client(
                url=self.vault_url,
                token=self.vault_token
            )
            if self._vault_client.is_authenticated():
                logger.info("Vault client authenticated successfully")
            else:
                logger.error("Vault authentication failed")
                self._vault_client = None
        except ImportError:
            logger.warning("hvac library not installed, Vault backend unavailable")
            self._vault_client = None
        except Exception as e:
            logger.error(f"Failed to initialize Vault client: {type(e).__name__}")
            self._vault_client = None

    def get_secret(
        self,
        key: str,
        path: Optional[str] = None,
        default: Optional[str] = None
    ) -> Optional[str]:
        """
        Retrieve secret with fallback chain.

        Args:
            key: Secret key/name
            path: Optional path for Vault backend (e.g., "secret/data/kgrag")
            default: Default value if secret not found in any backend

        Returns:
            Secret value or None if not found
        """
        cache_key = f"{path}/{key}" if path else key

        # Check cache first
        if self.cache_enabled and cache_key in self._cache:
            logger.debug(f"Secret '{key}' retrieved from cache")
            return self._cache[cache_key]

        # Try each backend in order
        for backend in self.backends:
            try:
                value = None

                if backend == SecretBackend.VAULT:
                    value = self._get_from_vault(key, path)
                elif backend == SecretBackend.DOCKER_SECRETS:
                    value = self._get_from_docker_secrets(key)
                elif backend == SecretBackend.ENVIRONMENT:
                    value = self._get_from_environment(key)

                if value is not None:
                    logger.info(f"Secret '{key}' loaded from {backend.value}")
                    if self.cache_enabled:
                        self._cache[cache_key] = value
                    return value

            except Exception as e:
                logger.warning(
                    f"Failed to retrieve '{key}' from {backend.value}: "
                    f"{type(e).__name__}"
                )
                continue

        # No backend succeeded
        if default is not None:
            logger.warning(f"Secret '{key}' not found in any backend, using default")
            return default

        logger.error(f"Secret '{key}' not found in any backend")
        return None

    def _get_from_vault(self, key: str, path: Optional[str]) -> Optional[str]:
        """Retrieve secret from Vault"""
        if not self._vault_client or not path:
            return None

        try:
            response = self._vault_client.secrets.kv.v2.read_secret_version(
                path=path
            )
            secret_data = response['data']['data']
            return secret_data.get(key)
        except Exception:
            return None

    def _get_from_docker_secrets(self, key: str) -> Optional[str]:
        """
        Retrieve secret from Docker secrets.

        Docker secrets are mounted as files in /run/secrets/<secret_name>
        """
        secret_file = self.docker_secrets_path / key.lower()

        if not secret_file.exists():
            return None

        try:
            # Read secret file
            with open(secret_file, 'r') as f:
                value = f.read().strip()

            if not value:
                return None

            return value
        except Exception:
            return None

    def _get_from_environment(self, key: str) -> Optional[str]:
        """Retrieve secret from environment variable"""
        value = os.getenv(key)
        return value if value else None

    def clear_cache(self):
        """Clear secrets cache (useful for rotation)"""
        if self.cache_enabled:
            cache_size = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared {cache_size} cached secrets")

    def set_cache(self, key: str, value: str, path: Optional[str] = None):
        """
        Manually set cached value (useful for testing).

        Args:
            key: Secret key
            value: Secret value
            path: Optional path (for Vault-style keys)
        """
        if self.cache_enabled:
            cache_key = f"{path}/{key}" if path else key
            self._cache[cache_key] = value
            logger.debug(f"Secret '{key}' added to cache")

    def get_backend_status(self) -> Dict[str, bool]:
        """
        Check status of all configured backends.

        Returns:
            Dict mapping backend name to availability status
        """
        status = {}

        for backend in self.backends:
            if backend == SecretBackend.VAULT:
                status['vault'] = (
                    self._vault_client is not None and
                    self._vault_client.is_authenticated()
                )
            elif backend == SecretBackend.DOCKER_SECRETS:
                status['docker_secrets'] = self.docker_secrets_path.exists()
            elif backend == SecretBackend.ENVIRONMENT:
                status['environment'] = True  # Always available

        return status

    def validate_required_secrets(self, required_keys: list[str]) -> tuple[bool, list[str]]:
        """
        Validate that all required secrets are available.

        Args:
            required_keys: List of required secret keys

        Returns:
            Tuple of (all_found: bool, missing_keys: list)
        """
        missing = []

        for key in required_keys:
            value = self.get_secret(key)
            if not value:
                missing.append(key)

        all_found = len(missing) == 0

        if all_found:
            logger.info(f"All {len(required_keys)} required secrets validated")
        else:
            logger.error(
                f"Missing {len(missing)} required secrets: "
                f"{', '.join(missing)}"
            )

        return all_found, missing


# Global secrets manager instance
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """
    Get global secrets manager instance.
    Initializes on first call with default configuration.
    """
    global _secrets_manager

    if _secrets_manager is None:
        # Check for Vault configuration in environment
        vault_url = os.getenv("VAULT_ADDR")
        vault_token = os.getenv("VAULT_TOKEN")

        backends = []

        # Add Vault if configured
        if vault_url and vault_token:
            backends.append(SecretBackend.VAULT)

        # Always add Docker Secrets and Environment
        backends.extend([
            SecretBackend.DOCKER_SECRETS,
            SecretBackend.ENVIRONMENT
        ])

        _secrets_manager = SecretsManager(
            backends=backends,
            vault_url=vault_url,
            vault_token=vault_token
        )

    return _secrets_manager


def set_secrets_manager(manager: SecretsManager):
    """Set custom secrets manager instance (useful for testing)"""
    global _secrets_manager
    _secrets_manager = manager
