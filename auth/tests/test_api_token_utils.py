"""
Unit tests for auth/api_token_utils.py

Tests:
- Token generation (format, prefix, hash, hint)
- Token hashing (SHA-256)
- Token verification (valid, expired, revoked)
- Scope validation
- User token count limits
"""
import pytest
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from auth.api_token_utils import (
    TOKEN_PREFIX,
    TOKEN_BYTES,
    MAX_TOKENS_PER_USER,
    VALID_SCOPES,
    generate_api_token,
    hash_api_token,
    verify_api_token,
    verify_api_token_with_timing_safe,
    validate_scopes,
    check_scope,
    get_user_token_count,
    can_create_token,
)
from auth.database import ApiToken


# ============================================================================
# Token Generation Tests
# ============================================================================

class TestGenerateApiToken:
    """Tests for generate_api_token()."""

    def test_returns_tuple_of_four_strings(self):
        """Token generation returns (plaintext, prefix, hash, hint)."""
        result = generate_api_token()

        assert isinstance(result, tuple)
        assert len(result) == 4
        assert all(isinstance(item, str) for item in result)

    def test_plaintext_has_correct_prefix(self):
        """Plaintext token starts with 'lgn_' prefix."""
        plaintext, _, _, _ = generate_api_token()

        assert plaintext.startswith(TOKEN_PREFIX)

    def test_plaintext_length_is_consistent(self):
        """Token length should be ~47 chars (4 prefix + 43 base64)."""
        plaintext, _, _, _ = generate_api_token()

        # lgn_ (4) + base64(32 bytes) (~43) = ~47 chars
        assert len(plaintext) >= 40
        assert len(plaintext) <= 60

    def test_prefix_is_first_eight_chars(self):
        """Token prefix is first 8 characters of plaintext."""
        plaintext, prefix, _, _ = generate_api_token()

        assert prefix == plaintext[:8]
        assert prefix.startswith(TOKEN_PREFIX)

    def test_hash_is_sha256_hex(self):
        """Token hash is 64-character hex (SHA-256)."""
        _, _, hash_val, _ = generate_api_token()

        assert len(hash_val) == 64
        assert all(c in '0123456789abcdef' for c in hash_val)

    def test_hint_is_last_four_chars(self):
        """Token hint is '...' + last 4 chars of plaintext."""
        plaintext, _, _, hint = generate_api_token()

        assert hint.startswith("...")
        assert hint == f"...{plaintext[-4:]}"

    def test_tokens_are_unique(self):
        """Each generated token should be unique."""
        tokens = [generate_api_token()[0] for _ in range(100)]

        assert len(set(tokens)) == 100

    def test_hashes_are_unique(self):
        """Each generated hash should be unique."""
        hashes = [generate_api_token()[2] for _ in range(100)]

        assert len(set(hashes)) == 100


# ============================================================================
# Token Hashing Tests
# ============================================================================

class TestHashApiToken:
    """Tests for hash_api_token()."""

    def test_returns_64_char_hex(self):
        """Hash should be 64-character hex string."""
        hash_val = hash_api_token("lgn_test_token")

        assert len(hash_val) == 64
        assert all(c in '0123456789abcdef' for c in hash_val)

    def test_same_input_same_hash(self):
        """Same token should produce same hash (deterministic)."""
        token = "lgn_abc123xyz"

        hash1 = hash_api_token(token)
        hash2 = hash_api_token(token)

        assert hash1 == hash2

    def test_different_input_different_hash(self):
        """Different tokens should produce different hashes."""
        hash1 = hash_api_token("lgn_token_a")
        hash2 = hash_api_token("lgn_token_b")

        assert hash1 != hash2

    def test_hash_matches_generation(self):
        """Manual hash should match generated hash."""
        plaintext, _, expected_hash, _ = generate_api_token()

        computed_hash = hash_api_token(plaintext)

        assert computed_hash == expected_hash


# ============================================================================
# Token Verification Tests
# ============================================================================

class TestVerifyApiToken:
    """Tests for verify_api_token()."""

    def test_valid_token_returns_api_token(self, test_db, test_api_token):
        """Valid token should return ApiToken record."""
        plaintext = test_api_token["plaintext"]

        result = verify_api_token(plaintext, test_db)

        assert result is not None
        assert isinstance(result, ApiToken)
        assert result.id == test_api_token["record"].id

    def test_invalid_prefix_returns_none(self, test_db):
        """Token without 'lgn_' prefix should return None."""
        result = verify_api_token("invalid_token_without_prefix", test_db)

        assert result is None

    def test_wrong_token_returns_none(self, test_db, test_api_token):
        """Token with wrong random part should return None."""
        # Valid prefix but wrong random part
        result = verify_api_token("lgn_wrong_random_part_here", test_db)

        assert result is None

    def test_expired_token_returns_none(self, test_db, expired_api_token):
        """Expired token should return None."""
        plaintext = expired_api_token["plaintext"]

        result = verify_api_token(plaintext, test_db)

        assert result is None

    def test_revoked_token_returns_none(self, test_db, revoked_api_token):
        """Revoked token should return None."""
        plaintext = revoked_api_token["plaintext"]

        result = verify_api_token(plaintext, test_db)

        assert result is None

    def test_token_with_no_expiry_is_valid(self, test_db, test_api_token):
        """Token with expires_at=None should be valid."""
        plaintext = test_api_token["plaintext"]
        record = test_api_token["record"]

        # Ensure no expiry
        assert record.expires_at is None

        result = verify_api_token(plaintext, test_db)

        assert result is not None


class TestVerifyApiTokenWithTimingSafe:
    """Tests for verify_api_token_with_timing_safe()."""

    def test_matching_token_returns_true(self):
        """Matching token and stored hash should return True."""
        plaintext, _, stored_hash, _ = generate_api_token()

        result = verify_api_token_with_timing_safe(plaintext, stored_hash)

        assert result is True

    def test_non_matching_token_returns_false(self):
        """Non-matching token should return False."""
        _, _, stored_hash, _ = generate_api_token()

        result = verify_api_token_with_timing_safe("lgn_different_token", stored_hash)

        assert result is False


# ============================================================================
# Scope Validation Tests
# ============================================================================

class TestValidateScopes:
    """Tests for validate_scopes()."""

    def test_valid_scopes_return_true(self):
        """All valid scopes should return (True, [])."""
        scopes = ["read:knowledge", "write:knowledge"]

        is_valid, invalid = validate_scopes(scopes)

        assert is_valid is True
        assert invalid == []

    def test_empty_scopes_are_valid(self):
        """Empty scope list should be valid."""
        is_valid, invalid = validate_scopes([])

        assert is_valid is True
        assert invalid == []

    def test_invalid_scope_returns_false(self):
        """Invalid scope should return (False, [invalid])."""
        scopes = ["read:knowledge", "invalid:scope"]

        is_valid, invalid = validate_scopes(scopes)

        assert is_valid is False
        assert "invalid:scope" in invalid

    def test_all_invalid_scopes_returned(self):
        """All invalid scopes should be listed."""
        scopes = ["bad:scope1", "bad:scope2", "read:knowledge"]

        is_valid, invalid = validate_scopes(scopes)

        assert is_valid is False
        assert "bad:scope1" in invalid
        assert "bad:scope2" in invalid
        assert "read:knowledge" not in invalid


class TestCheckScope:
    """Tests for check_scope()."""

    def test_token_has_scope(self, test_api_token):
        """Token with scope should return True."""
        record = test_api_token["record"]

        result = check_scope(record, "read:knowledge")

        assert result is True

    def test_token_missing_scope(self, test_api_token):
        """Token without scope should return False."""
        record = test_api_token["record"]

        result = check_scope(record, "write:code")

        assert result is False

    def test_empty_scopes_returns_false(self, test_db, test_user):
        """Token with empty scopes should return False for any scope."""
        from auth.api_token_utils import generate_api_token as gen

        _, prefix, hash_val, hint = gen()
        token = ApiToken(
            id=str(uuid4()),
            user_id=test_user.id,
            name="No Scopes",
            token_prefix=prefix,
            token_hash=hash_val,
            token_hint=hint,
            scopes=[],
        )

        result = check_scope(token, "read:knowledge")

        assert result is False


# ============================================================================
# User Token Count Tests
# ============================================================================

class TestGetUserTokenCount:
    """Tests for get_user_token_count()."""

    def test_returns_zero_for_no_tokens(self, test_db, test_user):
        """User with no tokens should return 0."""
        count = get_user_token_count(test_user.id, test_db)

        assert count == 0

    def test_returns_correct_count(self, test_db, test_api_token, test_user):
        """User with tokens should return correct count."""
        count = get_user_token_count(test_user.id, test_db)

        assert count == 1

    def test_excludes_revoked_tokens(self, test_db, revoked_api_token, test_user):
        """Revoked tokens should not be counted."""
        count = get_user_token_count(test_user.id, test_db)

        assert count == 0

    def test_excludes_other_users_tokens(self, test_db, test_api_token, second_test_user):
        """Only counts tokens for specified user."""
        count = get_user_token_count(second_test_user.id, test_db)

        assert count == 0


class TestCanCreateToken:
    """Tests for can_create_token()."""

    def test_can_create_when_under_limit(self, test_db, test_user):
        """User under limit can create tokens."""
        can_create, count = can_create_token(test_user.id, test_db)

        assert can_create is True
        assert count == 0

    def test_cannot_create_when_at_limit(self, test_db, test_user):
        """User at limit cannot create tokens."""
        from auth.api_token_utils import generate_api_token as gen

        # Create MAX_TOKENS_PER_USER tokens
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

        can_create, count = can_create_token(test_user.id, test_db)

        assert can_create is False
        assert count == MAX_TOKENS_PER_USER

    def test_max_tokens_per_user_is_25(self):
        """Verify MAX_TOKENS_PER_USER constant is 25."""
        assert MAX_TOKENS_PER_USER == 25


# ============================================================================
# Constants Tests
# ============================================================================

class TestConstants:
    """Tests for module constants."""

    def test_token_prefix(self):
        """Token prefix should be 'lgn_'."""
        assert TOKEN_PREFIX == "lgn_"

    def test_token_bytes(self):
        """Token should use 32 bytes (256 bits) of entropy."""
        assert TOKEN_BYTES == 32

    def test_valid_scopes_defined(self):
        """Valid scopes should include expected values."""
        expected = [
            "read:knowledge",
            "write:knowledge",
            "read:code",
            "write:code",
            "read:engagement",
            "write:engagement",
        ]
        assert VALID_SCOPES == expected
