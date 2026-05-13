"""
OAuth2 Provider configurations.

Defines supported OAuth providers (Google, GitHub) with their endpoints and scopes.
Configuration values are loaded from environment variables.

Location: auth/oauth/providers.py
"""
import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class OAuthProviderConfig:
    """OAuth2 provider configuration.

    Attributes:
        name: Provider identifier (google, github)
        client_id: OAuth client ID from provider
        client_secret: OAuth client secret from provider
        authorize_url: Provider's authorization endpoint
        token_url: Provider's token exchange endpoint
        userinfo_url: Provider's user info endpoint
        scopes: Required OAuth scopes
    """
    name: str
    client_id: str
    client_secret: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scopes: List[str]

    def is_configured(self) -> bool:
        """Check if provider has required credentials configured."""
        return bool(self.client_id and self.client_secret)

    def get_scope_string(self) -> str:
        """Return scopes as space-separated string for OAuth requests."""
        return " ".join(self.scopes)


# Supported OAuth providers
OAUTH_PROVIDERS: Dict[str, OAuthProviderConfig] = {
    "google": OAuthProviderConfig(
        name="google",
        client_id=os.getenv("GOOGLE_CLIENT_ID", ""),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET", ""),
        authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
        token_url="https://oauth2.googleapis.com/token",
        userinfo_url="https://www.googleapis.com/oauth2/v3/userinfo",
        scopes=["openid", "email", "profile"]
    ),
    "github": OAuthProviderConfig(
        name="github",
        client_id=os.getenv("GITHUB_CLIENT_ID", ""),
        client_secret=os.getenv("GITHUB_CLIENT_SECRET", ""),
        authorize_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        userinfo_url="https://api.github.com/user",
        scopes=["user:email"]
    )
}


def get_provider(name: str) -> OAuthProviderConfig:
    """
    Get OAuth provider configuration by name.

    Args:
        name: Provider name (google, github)

    Returns:
        OAuthProviderConfig for the requested provider

    Raises:
        ValueError: If provider name is not supported
    """
    if name not in OAUTH_PROVIDERS:
        raise ValueError(f"Unknown OAuth provider: {name}. Supported: {list(OAUTH_PROVIDERS.keys())}")
    return OAUTH_PROVIDERS[name]


def get_configured_providers() -> List[str]:
    """
    Get list of providers that have credentials configured.

    Returns:
        List of provider names with valid configuration
    """
    return [name for name, config in OAUTH_PROVIDERS.items() if config.is_configured()]
