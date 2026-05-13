"""
OAuth2 module for KGRAG authentication.

Provides provider configurations and client utilities for OAuth2 flows.
"""

from auth.oauth.providers import (
    OAuthProviderConfig,
    OAUTH_PROVIDERS,
    get_provider,
)
from auth.oauth.client import (
    OAuthClient,
    OAuthStateError,
    OAuthTokenError,
    OAuthUserInfoError,
    OAuthUserInfo,
)

__all__ = [
    "OAuthProviderConfig",
    "OAUTH_PROVIDERS",
    "get_provider",
    "OAuthClient",
    "OAuthStateError",
    "OAuthTokenError",
    "OAuthUserInfoError",
    "OAuthUserInfo",
]
