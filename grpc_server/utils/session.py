"""
Session ID utilities for gRPC servicers.

Provides:
- get_session_id_from_context(): reads x-session-id from interceptor ContextVar
- ensure_session(): creates a session record if one doesn't exist for the given session_id

Uses an in-memory set to avoid repeated DB lookups for the same session_id
within a single server lifetime. The set grows at most once per unique
session_id — bounded by total distinct sessions.
"""

from typing import Optional, Set
from loguru import logger

from grpc_server.interceptors.session_interceptor import get_session_id

# In-memory cache of session_ids we've already verified/created.
# Avoids a DB SELECT on every single gRPC call.
_known_sessions: Set[str] = set()


def get_session_id_from_context() -> Optional[str]:
    """
    Read the session ID injected by SessionContextInterceptor.
    Returns None if no x-session-id header was sent.
    """
    return get_session_id()


async def ensure_session(
    session_id: str,
    user_id: str,
    company_id: Optional[str] = None,
    project_id: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> str:
    """
    Ensure a session record exists for the given session_id.
    If already verified this server lifetime, returns immediately (cached).
    If not in cache, checks DB; if missing, creates a minimal record.

    company_id can be provided directly or resolved from project_id.

    Args:
        session_id: Client session UUID (from x-session-id header)
        user_id: Authenticated user UUID
        company_id: Company UUID (if known)
        project_id: Project UUID (used to resolve company_id if not provided)
        agent_id: Optional agent UUID

    Returns:
        The session_id (unchanged)
    """
    if session_id in _known_sessions:
        return session_id

    from api.repositories.session_repository import SessionRepository
    from api.database.connection import _db_pool

    pool = _db_pool.get_pool()
    repo = SessionRepository(pool)

    existing = await repo.get_session(session_id)
    if existing:
        _known_sessions.add(session_id)
        return session_id

    # Resolve company_id from project if not provided
    resolved_company_id = company_id
    if not resolved_company_id and project_id:
        from api.repositories.project_repository import ProjectRepository
        project_repo = ProjectRepository(pool)
        resolved_company_id = await project_repo.get_company_id(project_id)

    if not resolved_company_id:
        logger.warning(
            f"Cannot auto-create session {session_id}: no company_id available"
        )
        return session_id

    # Create a minimal session record
    logger.info(f"Auto-creating session record for session_id={session_id}")
    await repo.upsert_session(
        session_id=session_id,
        user_id=user_id,
        agent_id=agent_id or "",
        company_id=resolved_company_id,
        project_id=project_id,
    )
    _known_sessions.add(session_id)

    return session_id
