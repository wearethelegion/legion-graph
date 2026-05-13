"""
Integration tests for API Token endpoints.

Tests:
- POST /users/me/api-tokens (create)
- GET /users/me/api-tokens (list)
- GET /users/me/api-tokens/{id} (get)
- PATCH /users/me/api-tokens/{id} (update)
- DELETE /users/me/api-tokens/{id} (revoke)
- Dual auth: JWT + API token verification
- /verify endpoint dual auth support
"""
import pytest
from uuid import uuid4

from auth.api_token_utils import MAX_TOKENS_PER_USER
from auth.tests.conftest import auth_header


# ============================================================================
# Create API Token Endpoint Tests
# ============================================================================

class TestCreateApiToken:
    """Tests for POST /users/me/api-tokens."""

    def test_create_token_success(self, test_client, test_user_jwt):
        """Creating token with valid JWT returns 201 and token."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": "My Test Token", "scopes": ["read:knowledge"]}
        )

        assert response.status_code == 201
        data = response.json()
        assert "id" in data
        assert data["name"] == "My Test Token"
        assert "token" in data  # Plaintext token shown once
        assert data["token"].startswith("lgn_")
        assert "warning" in data  # "Save this token" warning
        assert data["scopes"] == ["read:knowledge"]

    def test_create_token_no_scopes(self, test_client, test_user_jwt):
        """Creating token without scopes defaults to empty list."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": "Minimal Token"}
        )

        assert response.status_code == 201
        assert response.json()["scopes"] == []

    def test_create_token_with_expiry(self, test_client, test_user_jwt):
        """Creating token with expires_in_days sets expiration."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": "Expiring Token", "expires_in_days": 30}
        )

        assert response.status_code == 201
        assert response.json()["expires_at"] is not None

    def test_create_token_invalid_scope(self, test_client, test_user_jwt):
        """Invalid scope returns 400 error."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": "Bad Token", "scopes": ["invalid:scope"]}
        )

        assert response.status_code == 400
        assert "invalid" in response.json()["detail"].lower()

    def test_create_token_requires_jwt(self, test_client, test_api_token):
        """Cannot create token using API token auth (requires JWT)."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_api_token["plaintext"]),
            json={"name": "Should Fail"}
        )

        assert response.status_code == 403

    def test_create_token_no_auth(self, test_client):
        """Request without auth returns 403."""
        response = test_client.post(
            "/users/me/api-tokens",
            json={"name": "No Auth Token"}
        )

        assert response.status_code == 403

    def test_create_token_empty_name(self, test_client, test_user_jwt):
        """Empty token name returns validation error."""
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": ""}
        )

        assert response.status_code == 422  # Validation error


# ============================================================================
# List API Tokens Endpoint Tests
# ============================================================================

class TestListApiTokens:
    """Tests for GET /users/me/api-tokens."""

    def test_list_tokens_empty(self, test_client, test_user_jwt):
        """Empty token list returns empty array."""
        response = test_client.get(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 200
        data = response.json()
        assert "tokens" in data
        assert data["tokens"] == []
        assert data["total"] == 0

    def test_list_tokens_returns_tokens(self, test_client, test_user_jwt, test_api_token):
        """List includes user's tokens."""
        response = test_client.get(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["tokens"]) == 1
        assert data["tokens"][0]["id"] == test_api_token["record"].id

    def test_list_tokens_no_plaintext(self, test_client, test_user_jwt, test_api_token):
        """Listed tokens do not include plaintext."""
        response = test_client.get(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt)
        )

        token = response.json()["tokens"][0]
        assert "token" not in token
        assert "token_hash" not in token
        assert "token_hint" in token  # Hint is included

    def test_list_tokens_excludes_revoked(self, test_client, test_user_jwt, revoked_api_token):
        """Revoked tokens not included in list."""
        response = test_client.get(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt)
        )

        assert response.json()["total"] == 0

    def test_list_tokens_user_isolation(self, test_client, test_user_jwt, second_user_jwt, test_api_token):
        """User can only see their own tokens."""
        response = test_client.get(
            "/users/me/api-tokens",
            headers=auth_header(second_user_jwt)
        )

        assert response.json()["total"] == 0


# ============================================================================
# Get Single API Token Endpoint Tests
# ============================================================================

class TestGetApiToken:
    """Tests for GET /users/me/api-tokens/{id}."""

    def test_get_token_success(self, test_client, test_user_jwt, test_api_token):
        """Getting own token returns details."""
        token_id = test_api_token["record"].id

        response = test_client.get(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == token_id
        assert data["name"] == "Test Token"

    def test_get_token_not_found(self, test_client, test_user_jwt):
        """Getting non-existent token returns 404."""
        response = test_client.get(
            f"/users/me/api-tokens/{uuid4()}",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 404

    def test_get_token_user_isolation(self, test_client, second_user_jwt, test_api_token):
        """Cannot get another user's token."""
        token_id = test_api_token["record"].id

        response = test_client.get(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(second_user_jwt)
        )

        assert response.status_code == 404


# ============================================================================
# Update API Token Endpoint Tests
# ============================================================================

class TestUpdateApiToken:
    """Tests for PATCH /users/me/api-tokens/{id}."""

    def test_update_token_name(self, test_client, test_user_jwt, test_api_token):
        """Updating token name succeeds."""
        token_id = test_api_token["record"].id

        response = test_client.patch(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt),
            json={"name": "Updated Name"}
        )

        assert response.status_code == 200
        assert response.json()["name"] == "Updated Name"

    def test_update_token_scopes(self, test_client, test_user_jwt, test_api_token):
        """Updating token scopes succeeds."""
        token_id = test_api_token["record"].id

        response = test_client.patch(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt),
            json={"scopes": ["read:code"]}
        )

        assert response.status_code == 200
        assert response.json()["scopes"] == ["read:code"]

    def test_update_token_invalid_scope(self, test_client, test_user_jwt, test_api_token):
        """Updating with invalid scope returns 400."""
        token_id = test_api_token["record"].id

        response = test_client.patch(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt),
            json={"scopes": ["invalid:scope"]}
        )

        assert response.status_code == 400

    def test_update_token_not_found(self, test_client, test_user_jwt):
        """Updating non-existent token returns 404."""
        response = test_client.patch(
            f"/users/me/api-tokens/{uuid4()}",
            headers=auth_header(test_user_jwt),
            json={"name": "New Name"}
        )

        assert response.status_code == 404

    def test_update_token_user_isolation(self, test_client, second_user_jwt, test_api_token):
        """Cannot update another user's token."""
        token_id = test_api_token["record"].id

        response = test_client.patch(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(second_user_jwt),
            json={"name": "Hacked Name"}
        )

        assert response.status_code == 404


# ============================================================================
# Revoke API Token Endpoint Tests
# ============================================================================

class TestRevokeApiToken:
    """Tests for DELETE /users/me/api-tokens/{id}."""

    def test_revoke_token_success(self, test_client, test_user_jwt, test_api_token):
        """Revoking token returns success status."""
        token_id = test_api_token["record"].id

        response = test_client.delete(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 200
        assert response.json()["status"] == "revoked"

    def test_revoke_token_prevents_reuse(self, test_client, test_user_jwt, test_api_token, test_db):
        """Revoked token cannot be used for auth."""
        token_id = test_api_token["record"].id
        plaintext = test_api_token["plaintext"]

        # Revoke the token
        test_client.delete(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(test_user_jwt)
        )

        # Try to use revoked token
        response = test_client.post(
            "/verify",
            json={"token": plaintext}
        )

        assert response.json()["valid"] is False

    def test_revoke_token_not_found(self, test_client, test_user_jwt):
        """Revoking non-existent token returns 404."""
        response = test_client.delete(
            f"/users/me/api-tokens/{uuid4()}",
            headers=auth_header(test_user_jwt)
        )

        assert response.status_code == 404

    def test_revoke_token_user_isolation(self, test_client, second_user_jwt, test_api_token):
        """Cannot revoke another user's token."""
        token_id = test_api_token["record"].id

        response = test_client.delete(
            f"/users/me/api-tokens/{token_id}",
            headers=auth_header(second_user_jwt)
        )

        assert response.status_code == 404


# ============================================================================
# Dual Auth Tests (JWT + API Token)
# ============================================================================

class TestDualAuth:
    """Tests for dual authentication (JWT + API token)."""

    def test_verify_jwt_token(self, test_client, test_user_jwt):
        """JWT token passes /verify endpoint."""
        response = test_client.post(
            "/verify",
            json={"token": test_user_jwt}
        )

        assert response.status_code == 200
        assert response.json()["valid"] is True

    def test_verify_api_token(self, test_client, test_api_token):
        """API token passes /verify endpoint."""
        response = test_client.post(
            "/verify",
            json={"token": test_api_token["plaintext"]}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert "user_id" in data

    def test_verify_invalid_token(self, test_client):
        """Invalid token fails /verify endpoint."""
        response = test_client.post(
            "/verify",
            json={"token": "invalid_token"}
        )

        assert response.json()["valid"] is False

    def test_verify_expired_api_token(self, test_client, expired_api_token):
        """Expired API token fails verification."""
        response = test_client.post(
            "/verify",
            json={"token": expired_api_token["plaintext"]}
        )

        assert response.json()["valid"] is False

    def test_verify_revoked_api_token(self, test_client, revoked_api_token):
        """Revoked API token fails verification."""
        response = test_client.post(
            "/verify",
            json={"token": revoked_api_token["plaintext"]}
        )

        assert response.json()["valid"] is False


# ============================================================================
# Token Count Limit Tests
# ============================================================================

class TestTokenCountLimit:
    """Tests for user token count limit (Ragen's condition)."""

    def test_limit_enforced(self, test_client, test_user_jwt, test_db, test_user):
        """Cannot create more than MAX_TOKENS_PER_USER tokens."""
        from auth.api_token_utils import generate_api_token as gen
        from auth.database import ApiToken

        # Create MAX_TOKENS_PER_USER tokens directly in DB
        for i in range(MAX_TOKENS_PER_USER):
            _, prefix, hash_val, hint = gen()
            token = ApiToken(
                id=str(uuid4()),
                user_id=test_user.id,
                name=f"Token {i}",
                token_prefix=prefix,
                token_hash=hash_val,
                token_hint=hint,
                scopes=[],
            )
            test_db.add(token)
        test_db.commit()

        # Try to create one more via API
        response = test_client.post(
            "/users/me/api-tokens",
            headers=auth_header(test_user_jwt),
            json={"name": "Over Limit Token"}
        )

        assert response.status_code == 400
        assert "limit" in response.json()["detail"].lower() or "25" in response.json()["detail"]
