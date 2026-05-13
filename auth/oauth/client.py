"""
OAuth2 Client utilities.

Handles OAuth state generation/validation, token exchange, and user info fetching.
Uses Redis for state storage (same pattern as token_blacklist.py).

Location: auth/oauth/client.py
"""
import os
import secrets
import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode

import httpx
import redis

from auth.oauth.providers import get_provider, OAuthProviderConfig

logger = logging.getLogger(__name__)

# Redis configuration from environment (same as token_blacklist.py)
REDIS_URI = os.getenv("REDIS_URI", "redis://localhost:6379/0")

# OAuth configuration
OAUTH_REDIRECT_BASE_URL = os.getenv("OAUTH_REDIRECT_BASE_URL", "http://localhost:8000")
OAUTH_STATE_TTL_SECONDS = int(os.getenv("OAUTH_STATE_TTL_SECONDS", "600"))  # 10 minutes

# Redis prefixes for OAuth
OAUTH_STATE_PREFIX = "oauth:state:"

# Synchronous Redis client (same pattern as token_blacklist.py)
redis_client = redis.Redis.from_url(REDIS_URI, decode_responses=True)


class OAuthStateError(Exception):
    """Raised when OAuth state validation fails."""
    pass


class OAuthTokenError(Exception):
    """Raised when OAuth token exchange fails."""
    pass


class OAuthUserInfoError(Exception):
    """Raised when fetching user info fails."""
    pass


@dataclass
class OAuthUserInfo:
    """Standardized OAuth user information across providers.

    Attributes:
        provider: OAuth provider name (google, github)
        provider_id: User's unique ID from the provider
        email: User's email address
        name: User's display name (may be None)
        picture: URL to user's profile picture (may be None)
    """
    provider: str
    provider_id: str
    email: str
    name: Optional[str] = None
    picture: Optional[str] = None


@dataclass
class OAuthTokens:
    """OAuth tokens received from provider.

    Attributes:
        access_token: OAuth access token
        token_type: Token type (usually "Bearer")
        expires_in: Token expiration in seconds (may be None)
        refresh_token: Refresh token (may be None, provider-dependent)
        scope: Granted scopes
    """
    access_token: str
    token_type: str
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None


class OAuthClient:
    """OAuth2 client for handling authentication flows.

    Handles state generation/validation via Redis, token exchange,
    and user info fetching for supported providers.
    """

    def __init__(self, provider_name: str):
        """
        Initialize OAuth client for a provider.

        Args:
            provider_name: Name of the OAuth provider (google, github)

        Raises:
            ValueError: If provider is not supported
        """
        self.provider = get_provider(provider_name)
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def redirect_uri(self) -> str:
        """Get the OAuth callback redirect URI for this provider."""
        return f"{OAUTH_REDIRECT_BASE_URL}/oauth/{self.provider.name}/callback"

    # =========================================================================
    # State Management (Redis-backed)
    # =========================================================================

    def generate_state(self, metadata: Optional[dict] = None) -> str:
        """
        Generate secure OAuth state token and store in Redis.

        State tokens prevent CSRF attacks by ensuring the callback
        originated from our authorization request.

        Args:
            metadata: Optional metadata to store with state (e.g., redirect_after)

        Returns:
            32-character URL-safe state token
        """
        state = secrets.token_urlsafe(32)
        key = f"{OAUTH_STATE_PREFIX}{state}"

        try:
            # Store state with optional metadata
            value = "valid"
            if metadata:
                import json
                value = json.dumps(metadata)

            redis_client.setex(key, OAUTH_STATE_TTL_SECONDS, value)
            logger.debug(f"OAuth state generated: {state[:8]}...")
            return state
        except redis.RedisError as e:
            logger.error(f"Failed to store OAuth state: {e}")
            raise OAuthStateError("Failed to generate OAuth state") from e

    def validate_state(self, state: str) -> Optional[dict]:
        """
        Validate OAuth state token and return stored metadata.

        Consumes the state (single-use) to prevent replay attacks.

        Args:
            state: State token from OAuth callback

        Returns:
            Stored metadata dict if any, empty dict if valid but no metadata

        Raises:
            OAuthStateError: If state is invalid or expired
        """
        key = f"{OAUTH_STATE_PREFIX}{state}"

        try:
            value = redis_client.get(key)
            if not value:
                raise OAuthStateError("Invalid or expired OAuth state")

            # Delete state (single-use)
            redis_client.delete(key)
            logger.debug(f"OAuth state validated and consumed: {state[:8]}...")

            # Parse metadata if stored
            if value == "valid":
                return {}
            try:
                import json
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return {}

        except redis.RedisError as e:
            logger.error(f"Failed to validate OAuth state: {e}")
            raise OAuthStateError("Failed to validate OAuth state") from e

    # =========================================================================
    # Authorization URL
    # =========================================================================

    def get_authorization_url(self, state: Optional[str] = None) -> str:
        """
        Build the OAuth authorization URL.

        Args:
            state: Optional pre-generated state token (generates new if None)

        Returns:
            Full authorization URL to redirect user to
        """
        if state is None:
            state = self.generate_state()

        params = {
            "client_id": self.provider.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.provider.get_scope_string(),
            "state": state,
            "response_type": "code",
        }

        # Google-specific: request offline access for refresh token
        if self.provider.name == "google":
            params["access_type"] = "offline"
            params["prompt"] = "consent"

        return f"{self.provider.authorize_url}?{urlencode(params)}"

    # =========================================================================
    # Token Exchange
    # =========================================================================

    async def exchange_code_for_tokens(self, code: str) -> OAuthTokens:
        """
        Exchange authorization code for access/refresh tokens.

        Args:
            code: Authorization code from OAuth callback

        Returns:
            OAuthTokens containing access_token and optional refresh_token

        Raises:
            OAuthTokenError: If token exchange fails
        """
        data = {
            "client_id": self.provider.client_id,
            "client_secret": self.provider.client_secret,
            "code": code,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code",
        }

        headers = {"Accept": "application/json"}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.provider.token_url,
                    data=data,
                    headers=headers,
                    timeout=10.0
                )

                if response.status_code != 200:
                    error_detail = response.text
                    logger.error(f"OAuth token exchange failed: {response.status_code} - {error_detail}")
                    raise OAuthTokenError(f"Token exchange failed: {response.status_code}")

                token_data = response.json()

                # Handle error response in JSON body (GitHub returns 200 with error)
                if "error" in token_data:
                    error_desc = token_data.get("error_description", token_data["error"])
                    logger.error(f"OAuth token error: {error_desc}")
                    raise OAuthTokenError(f"Token exchange failed: {error_desc}")

                return OAuthTokens(
                    access_token=token_data["access_token"],
                    token_type=token_data.get("token_type", "Bearer"),
                    expires_in=token_data.get("expires_in"),
                    refresh_token=token_data.get("refresh_token"),
                    scope=token_data.get("scope"),
                )

            except httpx.RequestError as e:
                logger.error(f"OAuth token request failed: {e}")
                raise OAuthTokenError(f"Token request failed: {e}") from e

    # =========================================================================
    # User Info Fetching
    # =========================================================================

    async def fetch_user_info(self, access_token: str) -> OAuthUserInfo:
        """
        Fetch user information from OAuth provider.

        Args:
            access_token: OAuth access token

        Returns:
            OAuthUserInfo with standardized user data

        Raises:
            OAuthUserInfoError: If fetching user info fails
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        # GitHub requires User-Agent header
        if self.provider.name == "github":
            headers["User-Agent"] = "KGRAG-Auth"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self.provider.userinfo_url,
                    headers=headers,
                    timeout=10.0
                )

                if response.status_code != 200:
                    logger.error(f"OAuth user info failed: {response.status_code}")
                    raise OAuthUserInfoError(f"Failed to fetch user info: {response.status_code}")

                user_data = response.json()
                return self._parse_user_info(user_data)

            except httpx.RequestError as e:
                logger.error(f"OAuth user info request failed: {e}")
                raise OAuthUserInfoError(f"User info request failed: {e}") from e

    def _parse_user_info(self, data: dict) -> OAuthUserInfo:
        """
        Parse provider-specific user data into standardized format.

        Args:
            data: Raw user data from provider

        Returns:
            OAuthUserInfo with normalized fields
        """
        if self.provider.name == "google":
            return OAuthUserInfo(
                provider="google",
                provider_id=data["sub"],  # Google uses 'sub' for user ID
                email=data["email"],
                name=data.get("name"),
                picture=data.get("picture"),
            )

        elif self.provider.name == "github":
            # GitHub might not include email in main response
            email = data.get("email")
            if not email:
                logger.warning("GitHub user has no public email - may need separate API call")

            return OAuthUserInfo(
                provider="github",
                provider_id=str(data["id"]),  # GitHub uses numeric 'id'
                email=email or "",
                name=data.get("name") or data.get("login"),
                picture=data.get("avatar_url"),
            )

        raise OAuthUserInfoError(f"Unsupported provider: {self.provider.name}")

    # =========================================================================
    # GitHub Email Fallback
    # =========================================================================

    async def fetch_github_email(self, access_token: str) -> Optional[str]:
        """
        Fetch primary email from GitHub's emails API.

        Used when user's email is not public. Requires user:email scope.

        Args:
            access_token: GitHub OAuth access token

        Returns:
            Primary verified email address, or None if not found
        """
        if self.provider.name != "github":
            return None

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": "KGRAG-Auth",
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    "https://api.github.com/user/emails",
                    headers=headers,
                    timeout=10.0
                )

                if response.status_code != 200:
                    logger.warning(f"Failed to fetch GitHub emails: {response.status_code}")
                    return None

                emails = response.json()
                # Find primary verified email
                for email_data in emails:
                    if email_data.get("primary") and email_data.get("verified"):
                        return email_data["email"]

                # Fallback to any verified email
                for email_data in emails:
                    if email_data.get("verified"):
                        return email_data["email"]

                return None

            except httpx.RequestError as e:
                logger.warning(f"GitHub emails request failed: {e}")
                return None

    # =========================================================================
    # Complete OAuth Flow
    # =========================================================================

    async def complete_oauth_flow(self, code: str, state: str) -> OAuthUserInfo:
        """
        Complete the OAuth flow: validate state, exchange code, fetch user info.

        This is a convenience method that performs the full callback handling.

        Args:
            code: Authorization code from callback
            state: State token from callback

        Returns:
            OAuthUserInfo with user's data

        Raises:
            OAuthStateError: If state validation fails
            OAuthTokenError: If token exchange fails
            OAuthUserInfoError: If user info fetch fails
        """
        # 1. Validate state (CSRF protection)
        self.validate_state(state)

        # 2. Exchange code for tokens
        tokens = await self.exchange_code_for_tokens(code)

        # 3. Fetch user info
        user_info = await self.fetch_user_info(tokens.access_token)

        # 4. GitHub email fallback if needed
        if self.provider.name == "github" and not user_info.email:
            email = await self.fetch_github_email(tokens.access_token)
            if email:
                user_info = OAuthUserInfo(
                    provider=user_info.provider,
                    provider_id=user_info.provider_id,
                    email=email,
                    name=user_info.name,
                    picture=user_info.picture,
                )

        return user_info
