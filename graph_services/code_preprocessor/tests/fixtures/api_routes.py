"""FastAPI route fixture for E2E pipeline tests."""

import json
from typing import Any


def get_health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


def get_user(user_id: str) -> dict:
    """Return a user by ID."""
    return {"id": user_id, "email": "user@example.com", "name": "Test User"}


def create_user(body: dict) -> dict:
    """Create a new user from request body."""
    required = ("email", "name")
    missing = [k for k in required if k not in body]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    return {"id": "new-id", **body}


def list_users(page: int = 1, per_page: int = 20) -> dict:
    """Paginated user list."""
    return {
        "items": [],
        "page": page,
        "per_page": per_page,
        "total": 0,
    }
