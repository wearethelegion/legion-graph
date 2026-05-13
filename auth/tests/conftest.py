"""
Pytest fixtures for KGRAG Auth Service tests.
"""
import pytest
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from auth.database import Base, User, ApiToken, Role, get_db
from auth.jwt_utils import create_access_token, hash_password


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def test_db():
    """
    Create a fresh in-memory SQLite database for each test.

    Note: SQLite doesn't support all PostgreSQL features, but works for most tests.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False}
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Create all tables
    Base.metadata.create_all(bind=engine)

    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def test_client(test_db):
    """
    Create a FastAPI TestClient with dependency override for database.
    """
    from auth.main import app

    def override_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


# ============================================================================
# User Fixtures
# ============================================================================

@pytest.fixture
def test_user(test_db):
    """Create a test user."""
    user = User(
        id=str(uuid4()),
        email="test@example.com",
        username="testuser",
        password_hash=hash_password("testpassword123"),
        is_active=True,
        is_superuser=False,
        email_verified=True,
    )
    test_db.add(user)
    test_db.commit()
    test_db.refresh(user)
    return user


@pytest.fixture
def test_user_jwt(test_user):
    """Create a valid JWT token for the test user."""
    return create_access_token({
        "sub": test_user.id,
        "email": test_user.email,
        "roles": [],
    })


@pytest.fixture
def second_test_user(test_db):
    """Create a second test user for isolation tests."""
    user = User(
        id=str(uuid4()),
        email="second@example.com",
        username="seconduser",
        password_hash=hash_password("password456"),
        is_active=True,
        is_superuser=False,
        email_verified=True,
    )
    test_db.add(user)
    test_db.commit()
    test_db.refresh(user)
    return user


@pytest.fixture
def second_user_jwt(second_test_user):
    """Create a valid JWT token for the second test user."""
    return create_access_token({
        "sub": second_test_user.id,
        "email": second_test_user.email,
        "roles": [],
    })


# ============================================================================
# API Token Fixtures
# ============================================================================

@pytest.fixture
def test_api_token(test_db, test_user):
    """Create a test API token for the test user."""
    from auth.api_token_utils import generate_api_token

    plaintext, prefix, hash_val, hint = generate_api_token()

    api_token = ApiToken(
        id=str(uuid4()),
        user_id=test_user.id,
        name="Test Token",
        token_prefix=prefix,
        token_hash=hash_val,
        token_hint=hint,
        scopes=["read:knowledge", "write:knowledge"],
        created_at=datetime.now(timezone.utc),
    )
    test_db.add(api_token)
    test_db.commit()
    test_db.refresh(api_token)

    # Return both the record and plaintext token
    return {
        "record": api_token,
        "plaintext": plaintext,
    }


@pytest.fixture
def expired_api_token(test_db, test_user):
    """Create an expired API token."""
    from auth.api_token_utils import generate_api_token

    plaintext, prefix, hash_val, hint = generate_api_token()

    api_token = ApiToken(
        id=str(uuid4()),
        user_id=test_user.id,
        name="Expired Token",
        token_prefix=prefix,
        token_hash=hash_val,
        token_hint=hint,
        scopes=[],
        created_at=datetime.now(timezone.utc) - timedelta(days=365),
        expires_at=datetime.now(timezone.utc) - timedelta(days=1),  # Expired yesterday
    )
    test_db.add(api_token)
    test_db.commit()
    test_db.refresh(api_token)

    return {
        "record": api_token,
        "plaintext": plaintext,
    }


@pytest.fixture
def revoked_api_token(test_db, test_user):
    """Create a revoked API token."""
    from auth.api_token_utils import generate_api_token

    plaintext, prefix, hash_val, hint = generate_api_token()

    api_token = ApiToken(
        id=str(uuid4()),
        user_id=test_user.id,
        name="Revoked Token",
        token_prefix=prefix,
        token_hash=hash_val,
        token_hint=hint,
        scopes=[],
        created_at=datetime.now(timezone.utc),
        revoked_at=datetime.now(timezone.utc),  # Already revoked
    )
    test_db.add(api_token)
    test_db.commit()
    test_db.refresh(api_token)

    return {
        "record": api_token,
        "plaintext": plaintext,
    }


# ============================================================================
# Helper Functions
# ============================================================================

def auth_header(token: str) -> dict:
    """Create Authorization header with Bearer token."""
    return {"Authorization": f"Bearer {token}"}
