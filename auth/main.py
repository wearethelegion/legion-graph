"""
KGRAG Auth Service
Centralized authentication and authorization for KGRAG API and MCP servers.
"""

import os

from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from contextlib import asynccontextmanager

from auth.database import (
    get_db,
    User,
    Role,
    Permission,
    Company,
    Project,
    Repository,
    Branch,
    company_users,
    init_db,
    health_check as db_health,
    DeletionTask,
    ApiToken,
)
from auth.jwt_utils import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
    verify_access_token,
    verify_refresh_token,
    create_token_payload,
    SECRET_KEY,
    ALGORITHM,
)
from auth.token_blacklist import add_to_blacklist, revoke_all_user_tokens
from auth.api_token_utils import (
    generate_api_token,
    verify_api_token,
    validate_scopes,
    can_create_token,
    VALID_SCOPES,
    MAX_TOKENS_PER_USER,
)
from auth.utils.logging_filter import configure_sensitive_logging
from auth.services.email_service import EmailService
from auth.services.totp_service import decrypt_totp_secret, get_totp_service
from loguru import logger
import jwt
import json
import asyncio

from auth.oauth import (
    OAUTH_PROVIDERS,
    OAuthClient,
    OAuthStateError,
    OAuthTokenError,
    OAuthUserInfoError,
)


# ============================================================================
# Datetime Utilities
# ============================================================================


def ensure_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Ensure a datetime is timezone-aware (UTC).

    SQLite stores datetimes as naive (no timezone). PostgreSQL returns aware datetimes.
    This helper normalizes both to UTC-aware for consistent comparisons.

    Args:
        dt: Datetime that may be naive or aware (or None)

    Returns:
        UTC-aware datetime, or None if input is None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Naive datetime - assume UTC and make aware
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ============================================================================
# Pydantic Models
# ============================================================================


class UserCreate(BaseModel):
    """Create new user request."""

    email: EmailStr
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=8)
    roles: List[str] = Field(default_factory=lambda: ["user"])


class UserLogin(BaseModel):
    """User login request."""

    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    """JWT token response."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 1800  # 30 minutes in seconds


class TwoFactorChallengeResponse(BaseModel):
    """2FA challenge response when TOTP is enabled."""

    requires_2fa: bool = True
    challenge_token: str


class UserResponse(BaseModel):
    """User information response."""

    id: str
    email: str
    username: str
    roles: List[str]
    is_active: bool
    is_superuser: bool
    created_at: str
    # Profile fields
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    phone_number: Optional[str] = None
    timezone: Optional[str] = None
    locale: Optional[str] = None
    profile_completed_at: Optional[str] = None
    # Auth status fields
    email_verified: bool = False
    totp_enabled: bool = False
    oauth_provider: Optional[str] = None


class VerifyTokenRequest(BaseModel):
    """Token verification request."""

    token: str


class VerifyTokenResponse(BaseModel):
    """Token verification response."""

    valid: bool
    user_id: Optional[str] = None
    email: Optional[str] = None
    roles: Optional[List[str]] = None
    companies: Optional[List[str]] = None
    is_superuser: bool = False


class LogoutRequest(BaseModel):
    """Logout request model."""

    refresh_token: Optional[str] = None  # Optional for graceful degradation
    revoke_all_sessions: bool = False


class LogoutResponse(BaseModel):
    """Logout response model."""

    message: str
    revoked_tokens: int


class VerifyEmailRequest(BaseModel):
    """Email verification request with 6-digit code."""

    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6, pattern="^[0-9]{6}$")


class VerifyEmailResponse(BaseModel):
    """Email verification response."""

    message: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    remaining_attempts: Optional[int] = None


class RoleCreate(BaseModel):
    """Create new role."""

    name: str
    description: Optional[str] = None
    permissions: List[int] = []  # Permission IDs


class PermissionCreate(BaseModel):
    """Create new permission."""

    resource: str
    action: str
    description: Optional[str] = None


class ResendVerificationRequest(BaseModel):
    """Resend verification code request."""

    email: EmailStr


class ResendVerificationResponse(BaseModel):
    """Resend verification response."""

    message: str


# ============================================================================
# 2FA TOTP Models
# ============================================================================


class TOTPVerifyRequest(BaseModel):
    """Verify TOTP code for 2FA setup."""

    code: str = Field(..., min_length=6, max_length=6, pattern="^[0-9]{6}$")


class TOTPSetupResponse(BaseModel):
    """TOTP 2FA setup response with QR code and secret."""

    secret: str
    qr_code: str  # Base64 encoded PNG
    provisioning_uri: str


class TOTPVerifySetupResponse(BaseModel):
    """Response after successful 2FA setup verification."""

    enabled: bool
    backup_codes: List[str]


class RegenerateBackupRequest(BaseModel):
    """Request to regenerate backup codes - requires current TOTP code."""

    code: str = Field(..., min_length=6, max_length=6, pattern="^[0-9]{6}$")


class RegenerateBackupResponse(BaseModel):
    """Response with new backup codes (one-time display)."""

    backup_codes: List[str]
    message: str = "Backup codes regenerated. Store them securely."


class Login2FARequest(BaseModel):
    """Complete 2FA login with challenge token and code."""

    challenge_token: str
    code: str = Field(
        ..., min_length=6, max_length=14
    )  # 6-digit TOTP or 12-char backup code (XXXX-XXXX-XXXX)


class Disable2FARequest(BaseModel):
    """Request to disable 2FA - requires password + TOTP code."""

    password: str
    code: str = Field(..., min_length=6, max_length=6, pattern="^[0-9]{6}$")


class Disable2FAResponse(BaseModel):
    """Response after 2FA is disabled."""

    message: str
    totp_enabled: bool = False


# ============================================================================
# OAuth2 Models
# ============================================================================


class OAuthCallbackResponse(BaseModel):
    """OAuth callback response - either tokens or link confirmation required."""

    # Success case - user authenticated
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int = 1800

    # Link confirmation case - email matches existing account
    action: Optional[str] = None  # "confirm_link_required"
    email: Optional[str] = None
    provider: Optional[str] = None
    message: Optional[str] = None


class OAuthConfirmLinkRequest(BaseModel):
    """Request to confirm linking OAuth to existing account with password verification."""

    email: EmailStr
    password: str
    oauth_code: str = Field(..., description="Authorization code from OAuth provider to re-verify")


class OAuthLinkRequest(BaseModel):
    """Request to link OAuth provider to existing authenticated account."""

    code: str = Field(..., description="Authorization code from OAuth provider")


class OAuthLinkResponse(BaseModel):
    """Response after successful OAuth linking."""

    message: str
    provider: str
    linked_at: str


# ============================================================================
# Company/Project/Repository/Branch Models
# ============================================================================


class CompanyCreate(BaseModel):
    """Create new company."""

    name: str = Field(..., min_length=1, max_length=200)


class CompanyResponse(BaseModel):
    """Company information response."""

    id: str
    name: str
    created_at: str
    is_active: bool
    user_count: int = 0
    cognee_enabled: bool = False


class AddUserToCompany(BaseModel):
    """Add user to company."""

    user_id: str
    role: str = Field(default="member", pattern="^(owner|member)$")


class ProjectCreate(BaseModel):
    """Create new project."""

    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None


class ProjectResponse(BaseModel):
    """Project information response."""

    id: str
    company_id: str
    name: str
    description: Optional[str]
    created_at: str


class RepositoryCreate(BaseModel):
    """Create new repository."""

    name: str = Field(..., min_length=1, max_length=200)
    url: Optional[str] = None


class RepositoryResponse(BaseModel):
    """Repository information response."""

    id: str
    project_id: str
    name: str
    url: Optional[str]
    created_at: str


class BranchCreate(BaseModel):
    """Create new branch."""

    name: str = Field(..., min_length=1, max_length=200)
    commit_sha: Optional[str] = None


class BranchResponse(BaseModel):
    """Branch information response."""

    id: str
    repository_id: str
    name: str
    commit_sha: Optional[str]
    created_at: str


# ============================================================================
# User Profile & Deletion Models
# ============================================================================


class UserProfileUpdate(BaseModel):
    """Partial user profile update request."""

    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    avatar_url: Optional[str] = None
    bio: Optional[str] = Field(None, max_length=500)
    phone_number: Optional[str] = Field(None, max_length=20)
    timezone: Optional[str] = Field(None, max_length=50)
    locale: Optional[str] = Field(None, max_length=10)


class UserProfileResponse(BaseModel):
    """Extended user response with profile fields."""

    id: str
    email: str
    username: str
    roles: List[str]
    is_active: bool
    is_superuser: bool
    email_verified: bool
    totp_enabled: bool
    # Profile fields
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    phone_number: Optional[str] = None
    timezone: Optional[str] = None
    locale: Optional[str] = None
    profile_completed_at: Optional[str] = None
    created_at: str
    updated_at: Optional[str] = None


class DeleteUserRequest(BaseModel):
    """Request to delete user account."""

    confirmation: str = Field(..., description="Must match user email to confirm deletion")
    acknowledge_cascade: bool = Field(
        False, description="Must be true if user owns companies (acknowledges cascade deletion)"
    )


class DeleteUserResponse(BaseModel):
    """Response after initiating user deletion."""

    status: str  # "processing" or "deleted"
    message: str
    task_id: Optional[str] = None


class DeleteUserPreviewResponse(BaseModel):
    """Preview of cascade delete impact."""

    user_email: str
    owned_companies_count: int
    owned_companies: List[dict]  # [{id, name, project_count, agent_count}]
    affected_members_count: int
    total_projects: int
    total_agents: int
    requires_cascade_acknowledgment: bool


class DeletionStatusResponse(BaseModel):
    """Real-time deletion progress."""

    task_id: str
    status: str  # pending|session_invalidation|external_cleanup|postgres_deletion|completed|failed
    current_phase: Optional[str] = None
    progress: Optional[dict] = None
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    estimated_remaining_seconds: Optional[int] = None


# ============================================================================
# API Token Models
# ============================================================================


class ApiTokenCreate(BaseModel):
    """Create new API token request."""

    name: str = Field(..., min_length=1, max_length=100)
    scopes: List[str] = Field(default_factory=list)
    expires_in_days: Optional[int] = Field(None, ge=1, le=365)


class ApiTokenUpdate(BaseModel):
    """Update API token request (partial update)."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    scopes: Optional[List[str]] = None


class ApiTokenResponse(BaseModel):
    """API token metadata response (no plaintext token)."""

    id: str
    name: str
    token_hint: str  # "...AbCd"
    scopes: List[str]
    created_at: str
    last_used_at: Optional[str] = None
    expires_at: Optional[str] = None


class ApiTokenCreateResponse(ApiTokenResponse):
    """API token creation response (includes plaintext token ONCE)."""

    token: str  # Only returned at creation!
    warning: str = "Save this token now. You won't be able to see it again."


class ApiTokenListResponse(BaseModel):
    """List of API tokens."""

    tokens: List[ApiTokenResponse]
    total: int


class ApiTokenRevokeResponse(BaseModel):
    """Token revocation response."""

    status: str = "revoked"
    token_id: str


# ============================================================================
# Application Lifecycle
# ============================================================================

# Background task cancellation flag
_shutdown_event = asyncio.Event()


async def _api_token_usage_flush_task():
    """
    Background task to periodically flush API token usage buffer.

    Runs every 60 seconds. On shutdown, flushes any remaining updates.
    This satisfies Ragen's condition: async/buffered last_used_at updates.
    """
    from auth.database import SessionLocal

    while not _shutdown_event.is_set():
        try:
            # Wait 60 seconds or until shutdown
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass  # Timeout is expected, flush buffer

            # Flush the buffer
            db = SessionLocal()
            try:
                await flush_api_token_usage_buffer(db)
            finally:
                db.close()

        except Exception as e:
            logger.error(f"Error in API token usage flush task: {e}")

    # Final flush on shutdown
    logger.info("Final flush of API token usage buffer...")
    db = SessionLocal()
    try:
        await flush_api_token_usage_buffer(db)
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    logger.info("Starting KGRAG Auth Service...")
    configure_sensitive_logging()
    logger.info("✓ Sensitive data logging filter configured")
    init_db()
    logger.info("✓ Database initialized")

    # Start background task for API token usage updates
    _shutdown_event.clear()
    flush_task = asyncio.create_task(_api_token_usage_flush_task())
    logger.info("✓ API token usage flush task started")

    yield

    # Signal shutdown and wait for flush task
    logger.info("Shutting down KGRAG Auth Service...")
    _shutdown_event.set()
    try:
        await asyncio.wait_for(flush_task, timeout=5.0)
        logger.info("✓ API token usage flush task completed")
    except asyncio.TimeoutError:
        logger.warning("API token usage flush task timed out")
        flush_task.cancel()


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="KGRAG Auth Service",
    description="Centralized authentication and authorization for KGRAG",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()


# ============================================================================
# Dual Authentication Support (JWT + API Token)
# ============================================================================


async def get_current_user_dual_auth(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
) -> User:
    """
    Authenticate user via JWT OR API token (lgn_*).

    Fast path: API token (prefix check is O(1))
    Default path: JWT verification

    Note: last_used_at update for API tokens is handled asynchronously
    to avoid synchronous DB writes on every request (Ragen's condition).
    """
    token = credentials.credentials

    # Fast path: API token (prefix check is O(1))
    if token.startswith("lgn_"):
        api_token = verify_api_token(token, db)
        if not api_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired API token"
            )

        # Get user from API token
        user = db.query(User).filter(User.id == api_token.user_id).first()
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        # Schedule async last_used_at update (Ragen's condition: no sync DB writes)
        schedule_api_token_usage_update(api_token.id)

        # Store api_token on user for scope checking if needed
        user._api_token = api_token
        return user

    # Default path: JWT verification
    payload = verify_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user = db.query(User).filter(User.id == payload.get("sub")).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    user._api_token = None  # Mark as JWT authenticated
    return user


def schedule_api_token_usage_update(token_id: str, ip_address: str = None):
    """
    Schedule asynchronous update of API token last_used_at.

    This avoids synchronous DB writes on every API request.
    Implementation: Could use Redis queue, background task, or periodic batch update.

    For now, we implement a simple in-process buffer that gets flushed periodically.
    """
    # Simple implementation: store in memory buffer, flush on interval
    # In production, consider Redis or a proper job queue
    _api_token_usage_buffer.append(
        {"token_id": token_id, "used_at": datetime.now(timezone.utc), "ip_address": ip_address}
    )


# Buffer for async last_used_at updates
_api_token_usage_buffer = []


async def flush_api_token_usage_buffer(db: Session):
    """
    Flush buffered API token usage updates to database.

    Call this periodically (e.g., every 60 seconds) or on shutdown.
    """
    global _api_token_usage_buffer
    if not _api_token_usage_buffer:
        return

    buffer = _api_token_usage_buffer
    _api_token_usage_buffer = []

    for update in buffer:
        api_token = db.query(ApiToken).filter(ApiToken.id == update["token_id"]).first()
        if api_token:
            api_token.last_used_at = update["used_at"]
            if update.get("ip_address"):
                api_token.last_used_ip = update["ip_address"]

    db.commit()


# ============================================================================
# Health & Status
# ============================================================================


@app.get("/health")
async def health_check():
    """Check service health."""
    db_healthy = db_health()
    return {
        "status": "healthy" if db_healthy else "degraded",
        "services": {"database": db_healthy},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/")
async def root():
    """Service info."""
    return {"service": "KGRAG Auth Service", "version": "1.0.0", "docs": "/docs"}


# ============================================================================
# Helper Functions
# ============================================================================

# 2FA Challenge Token Settings
TWO_FA_CHALLENGE_EXPIRE_MINUTES = 5


def create_2fa_challenge_token(user_id: str) -> str:
    """
    Create a short-lived JWT for 2FA challenge flow.

    This token is returned after password verification when user has TOTP enabled.
    User must present this token along with TOTP code to /login/2fa endpoint.

    Args:
        user_id: User's unique identifier

    Returns:
        5-minute JWT containing user_id for the 2FA step
    """
    payload = {"sub": user_id, "type": "2fa_challenge"}
    return jwt.encode(
        {
            **payload,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=TWO_FA_CHALLENGE_EXPIRE_MINUTES),
            "iat": datetime.now(timezone.utc),
        },
        SECRET_KEY,
        algorithm=ALGORITHM,
    )


def validate_company_membership(db: Session, user: User, company: Company) -> None:
    """
    Validate user has active membership in company.
    Raises HTTPException if validation fails.

    Checks:
    - Company is active
    - User is a valid member (via company_users table)
    - Skips checks for super admins
    """
    if user.is_superuser:
        return  # Super admin bypasses all checks

    # Check company is active
    if not company.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Company is inactive")

    # Verify user is actually a member via junction table
    from sqlalchemy import select

    membership = db.execute(
        select(company_users).where(
            company_users.c.company_id == company.id, company_users.c.user_id == user.id
        )
    ).first()

    if not membership:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this company"
        )


# ============================================================================
# Profile Validation Helpers
# ============================================================================

# Valid IANA timezones (common subset - full list would be much larger)
VALID_TIMEZONES = {
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Toronto",
    "America/Vancouver",
    "America/Sao_Paulo",
    "America/Mexico_City",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Moscow",
    "Europe/Kiev",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Asia/Hong_Kong",
    "Asia/Singapore",
    "Asia/Seoul",
    "Asia/Dubai",
    "Asia/Kolkata",
    "Australia/Sydney",
    "Australia/Melbourne",
    "Pacific/Auckland",
    "Africa/Cairo",
    "Africa/Johannesburg",
}

# Valid locales (common subset)
VALID_LOCALES = {
    "en",
    "en-US",
    "en-GB",
    "en-AU",
    "en-CA",
    "es",
    "es-ES",
    "es-MX",
    "es-AR",
    "fr",
    "fr-FR",
    "fr-CA",
    "de",
    "de-DE",
    "de-AT",
    "de-CH",
    "pt",
    "pt-BR",
    "pt-PT",
    "it",
    "it-IT",
    "ja",
    "ja-JP",
    "zh",
    "zh-CN",
    "zh-TW",
    "ko",
    "ko-KR",
    "ru",
    "ru-RU",
    "ar",
    "ar-SA",
    "hi",
    "hi-IN",
    "nl",
    "nl-NL",
    "pl",
    "pl-PL",
    "uk",
    "uk-UA",
}


def is_valid_timezone(tz: str) -> bool:
    """Check if timezone is valid (common IANA timezones)."""
    if not tz:
        return True  # None/empty is valid (not set)
    return tz in VALID_TIMEZONES


def is_valid_locale(locale: str) -> bool:
    """Check if locale is valid."""
    if not locale:
        return True  # None/empty is valid (not set)
    return locale in VALID_LOCALES


def validate_phone_number(phone: str) -> bool:
    """
    Validate phone number format (E.164-like).
    Accepts: +1234567890, 1234567890 (7-15 digits)
    """
    if not phone:
        return True  # None/empty is valid (not set)
    import re

    # E.164 format: optional +, 7-15 digits
    pattern = r"^\+?[1-9]\d{6,14}$"
    return bool(re.match(pattern, phone.replace(" ", "").replace("-", "")))


def _build_user_profile_response(user: User) -> UserProfileResponse:
    """Build UserProfileResponse from User model."""
    return UserProfileResponse(
        id=user.id,
        email=user.email,
        username=user.username,
        roles=[role.name for role in user.roles],
        is_active=user.is_active,
        is_superuser=user.is_superuser,
        email_verified=user.email_verified,
        totp_enabled=user.totp_enabled,
        first_name=user.first_name,
        last_name=user.last_name,
        display_name=user.display_name,
        avatar_url=user.avatar_url,
        bio=user.bio,
        phone_number=user.phone_number,
        timezone=user.timezone,
        locale=user.locale,
        profile_completed_at=user.profile_completed_at.isoformat()
        if user.profile_completed_at
        else None,
        created_at=user.created_at.isoformat() if user.created_at else None,
        updated_at=user.updated_at.isoformat() if user.updated_at else None,
    )


# ============================================================================
# Deletion Helper Functions
# ============================================================================


async def _phase_0_invalidate_sessions(task: DeletionTask, db: Session) -> bool:
    """
    Phase 0: Invalidate all user sessions.
    CRITICAL: Prevents user from taking actions during deletion.
    """
    from auth.token_blacklist import revoke_all_user_tokens

    task.status = "session_invalidation"
    task.current_phase = "invalidating_all_sessions"
    db.commit()

    try:
        # Revoke all tokens for this user
        if not revoke_all_user_tokens(task.user_id):
            raise Exception("Failed to revoke user tokens in Redis")

        logger.info(f"Phase 0 complete: Sessions invalidated for user {task.user_id}")
        return True

    except Exception as e:
        task.error_message = f"Session invalidation failed: {str(e)}"
        task.error_phase = "session_invalidation"
        task.status = "failed"
        db.commit()
        logger.error(f"Phase 0 failed for user {task.user_id}: {e}")
        return False


async def _phase_1_external_cleanup(
    task: DeletionTask, owned_company_ids: List[str], db: Session
) -> bool:
    """
    Phase 1: Clean external storage (Qdrant, Neo4j).
    CRITICAL: Must complete BEFORE PostgreSQL deletion.
    """
    from sqlalchemy import text

    task.status = "external_cleanup"
    task.external_cleanup_state = {
        "qdrant_cleaned": [],
        "neo4j_cleaned": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    db.commit()

    # Get all project_ids for these companies
    project_ids = []
    if owned_company_ids:
        result = db.execute(
            text("SELECT id FROM projects WHERE company_id = ANY(:company_ids)"),
            {"company_ids": owned_company_ids},
        )
        project_ids = [row[0] for row in result.fetchall()]

    # Skip external cleanup if no owned companies (simple user delete)
    if not owned_company_ids:
        task.external_cleanup_state["completed_at"] = datetime.now(timezone.utc).isoformat()
        task.external_cleanup_state["skipped"] = "No owned companies"
        db.commit()
        logger.info(f"Phase 1 skipped: No owned companies for user {task.user_id}")
        return True

    # 1. Clean Qdrant collections
    qdrant_collections = ["knowledge_chunks", "expertise_chunks", "entry_chunks", "code_chunks"]

    try:
        from qdrant_client import QdrantClient, models
        import os

        qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
        qdrant = QdrantClient(url=qdrant_url)

        for collection in qdrant_collections:
            task.current_phase = f"qdrant:{collection}"
            db.commit()

            try:
                # Check if collection exists
                collections = qdrant.get_collections().collections
                collection_names = [c.name for c in collections]

                if collection not in collection_names:
                    task.external_cleanup_state["qdrant_cleaned"].append(
                        {
                            "collection": collection,
                            "skipped": "Collection does not exist",
                            "cleaned_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    continue

                # Delete by project_id filter
                filter_conditions = []
                if project_ids:
                    filter_conditions.append(
                        models.FieldCondition(
                            key="project_id", match=models.MatchAny(any=project_ids)
                        )
                    )
                if owned_company_ids:
                    filter_conditions.append(
                        models.FieldCondition(
                            key="company_id", match=models.MatchAny(any=owned_company_ids)
                        )
                    )

                if filter_conditions:
                    qdrant.delete(
                        collection_name=collection,
                        points_selector=models.FilterSelector(
                            filter=models.Filter(should=filter_conditions)
                        ),
                    )

                task.external_cleanup_state["qdrant_cleaned"].append(
                    {
                        "collection": collection,
                        "project_ids": project_ids,
                        "company_ids": owned_company_ids,
                        "cleaned_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                db.commit()

            except Exception as e:
                task.error_message = f"Qdrant cleanup failed on {collection}: {str(e)}"
                task.error_phase = f"qdrant:{collection}"
                task.status = "failed"
                task.external_cleanup_state["qdrant_error"] = {
                    "collection": collection,
                    "error": str(e),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                }
                db.commit()
                logger.error(f"Phase 1 Qdrant failed for user {task.user_id}: {e}")
                return False

    except ImportError:
        logger.warning("Qdrant client not available, skipping Qdrant cleanup")
        task.external_cleanup_state["qdrant_skipped"] = "Qdrant client not available"
        db.commit()
    except Exception as e:
        logger.warning(f"Qdrant connection failed, skipping: {e}")
        task.external_cleanup_state["qdrant_skipped"] = f"Connection failed: {str(e)}"
        db.commit()

    # 2. Clean Neo4j nodes
    task.current_phase = "neo4j:cleanup"
    db.commit()

    try:
        from neo4j import GraphDatabase
        import os

        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

        with driver.session() as session:
            result = session.run(
                """
                MATCH (n)
                WHERE n.company_id IN $company_ids
                   OR n.project_id IN $project_ids
                WITH n, labels(n) as labels, n.id as node_id
                DETACH DELETE n
                RETURN count(*) as deleted_count
            """,
                company_ids=owned_company_ids,
                project_ids=project_ids,
            )

            record = result.single()
            deleted_count = record["deleted_count"] if record else 0

            task.external_cleanup_state["neo4j_cleaned"].append(
                {
                    "company_ids": owned_company_ids,
                    "project_ids": project_ids,
                    "deleted_count": deleted_count,
                    "cleaned_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            db.commit()

        driver.close()

    except ImportError:
        logger.warning("Neo4j driver not available, skipping Neo4j cleanup")
        task.external_cleanup_state["neo4j_skipped"] = "Neo4j driver not available"
        db.commit()
    except Exception as e:
        logger.warning(f"Neo4j connection failed, skipping: {e}")
        task.external_cleanup_state["neo4j_skipped"] = f"Connection failed: {str(e)}"
        db.commit()

    task.external_cleanup_state["completed_at"] = datetime.now(timezone.utc).isoformat()
    db.commit()
    logger.info(f"Phase 1 complete: External cleanup for user {task.user_id}")
    return True


async def _phase_2_postgres_deletion(
    task: DeletionTask, owned_company_ids: List[str], user_id: str, db: Session
) -> bool:
    """
    Phase 2: PostgreSQL cascade delete.
    Only runs AFTER external storage is cleaned.
    """
    from sqlalchemy import text

    task.status = "postgres_deletion"
    task.progress = task.progress or {}
    task.progress["companies_total"] = len(owned_company_ids)
    task.progress["companies_deleted"] = 0
    db.commit()

    try:
        # Delete owned companies (CASCADE handles children)
        for i, company_id in enumerate(owned_company_ids):
            task.current_phase = f"company:{company_id}"
            db.commit()

            db.execute(
                text("DELETE FROM companies WHERE id = :company_id"), {"company_id": company_id}
            )

            task.progress["companies_deleted"] = i + 1
            db.commit()

        # Delete user memberships (for companies user doesn't own)
        task.current_phase = "memberships"
        db.execute(text("DELETE FROM company_users WHERE user_id = :user_id"), {"user_id": user_id})

        # Delete user roles
        task.current_phase = "roles"
        db.execute(text("DELETE FROM user_roles WHERE user_id = :user_id"), {"user_id": user_id})

        # Finally, delete user record
        task.current_phase = "user"
        db.execute(text("DELETE FROM users WHERE id = :user_id"), {"user_id": user_id})

        db.commit()
        logger.info(f"Phase 2 complete: PostgreSQL deletion for user {task.user_id}")
        return True

    except Exception as e:
        db.rollback()
        task.error_message = f"PostgreSQL deletion failed: {str(e)}"
        task.error_phase = task.current_phase
        task.status = "failed"
        db.commit()
        logger.error(f"Phase 2 failed for user {task.user_id}: {e}")
        return False


async def _create_deletion_audit(task: DeletionTask, db: Session) -> str:
    """Create audit record BEFORE any deletion occurs."""
    from sqlalchemy import text
    import json

    # Get user data before it's deleted
    user = db.query(User).filter(User.id == task.user_id).first()
    if not user:
        logger.warning(f"User {task.user_id} not found for audit")
        return None

    # Get owned company details
    owned_companies = db.execute(
        text("""
        SELECT c.id, c.name,
               (SELECT COUNT(*) FROM projects WHERE company_id = c.id) as project_count,
               (SELECT COUNT(*) FROM agents WHERE company_id = c.id) as agent_count
        FROM companies c
        JOIN company_users cu ON c.id = cu.company_id
        WHERE cu.user_id = :user_id AND cu.role = 'owner'
    """),
        {"user_id": task.user_id},
    ).fetchall()

    # Get affected members (will lose access)
    affected_members = []
    if task.owned_company_ids:
        affected_members = db.execute(
            text("""
            SELECT DISTINCT cu.user_id, u.email
            FROM company_users cu
            JOIN users u ON u.id = cu.user_id
            WHERE cu.company_id = ANY(:company_ids)
              AND cu.user_id != :owner_id
        """),
            {"company_ids": task.owned_company_ids, "owner_id": task.user_id},
        ).fetchall()

    audit_id = str(uuid4())

    db.execute(
        text("""
        INSERT INTO audit.deletion_audit_logs
        (id, task_id, user_id, user_email, user_created_at,
         owned_companies, entity_counts, affected_members, retention_until)
        VALUES
        (:id, :task_id, :user_id, :email, :created_at,
         :owned_companies, :entity_counts, :affected_members, :retention_until)
    """),
        {
            "id": audit_id,
            "task_id": task.id,
            "user_id": user.id,
            "email": user.email,
            "created_at": user.created_at,
            "owned_companies": json.dumps(
                [
                    {
                        "id": c.id,
                        "name": c.name,
                        "projects": c.project_count,
                        "agents": c.agent_count,
                    }
                    for c in owned_companies
                ]
            ),
            "entity_counts": json.dumps(
                {
                    "companies": len(owned_companies),
                    "projects": sum(c.project_count for c in owned_companies),
                    "agents": sum(c.agent_count for c in owned_companies),
                }
            ),
            "affected_members": json.dumps(
                [{"user_id": m.user_id, "email": m.email} for m in affected_members]
            ),
            "retention_until": datetime.now(timezone.utc)
            + timedelta(days=365 * 7),  # 7 year retention
        },
    )

    db.commit()
    logger.info(f"Audit record created: {audit_id} for user {task.user_id}")
    return audit_id


async def execute_user_deletion(task_id: str):
    """
    Main deletion orchestrator with proper phase ordering.

    Phase order:
    0. Invalidate sessions (FIRST - prevents user actions)
    1. Clean external storage (Qdrant, Neo4j)
    2. PostgreSQL cascade delete
    3. Complete and cleanup

    Each phase is idempotent and can be resumed from failure.
    """
    from auth.database import SessionLocal

    db = SessionLocal()

    try:
        task = db.query(DeletionTask).filter(DeletionTask.id == task_id).first()
        if not task:
            logger.error(f"Deletion task not found: {task_id}")
            return

        task.started_at = datetime.now(timezone.utc)
        owned_company_ids = task.owned_company_ids or []

        # Phase 0: Session invalidation
        if task.status in ("pending", "session_invalidation"):
            if not await _phase_0_invalidate_sessions(task, db):
                return  # Failed - stop here

        # Phase 1: External storage cleanup
        if task.status in ("session_invalidation", "external_cleanup"):
            if not await _phase_1_external_cleanup(task, owned_company_ids, db):
                return  # Failed - stop here, PostgreSQL intact

        # Phase 2: PostgreSQL deletion
        if task.status in ("external_cleanup", "postgres_deletion"):
            if not await _phase_2_postgres_deletion(task, owned_company_ids, task.user_id, db):
                return  # Failed - may have partial deletion

        # Phase 3: Complete
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.deletion_summary = task.progress
        db.commit()

        logger.info(f"User deletion completed: {task.user_id}")

    except Exception as e:
        logger.error(f"Unexpected error in deletion orchestrator: {e}")
        task.error_message = f"Unexpected error: {str(e)}"
        task.status = "failed"
        db.commit()
        raise
    finally:
        db.close()


# ============================================================================
# Authentication Endpoints
# ============================================================================


@app.post("/register", status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """Register a new user with email verification."""
    # Check if user exists
    existing = (
        db.query(User)
        .filter((User.email == user_data.email) | (User.username == user_data.username))
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Email or username already registered"
        )

    # Dev bypass — when SKIP_EMAIL_VERIFICATION=true, create the user already
    # active and verified, skip the email send. Default behaviour (false) is
    # unchanged: account is inactive until /verify-email is called.
    skip_verification = os.getenv("SKIP_EMAIL_VERIFICATION", "false").lower() in (
        "1",
        "true",
        "yes",
    )

    if skip_verification:
        user = User(
            id=str(uuid4()),
            email=user_data.email,
            username=user_data.username,
            password_hash=hash_password(user_data.password),
            is_active=True,
            is_superuser=False,
            email_verified=True,
            verification_code=None,
            verification_code_expires_at=None,
        )
    else:
        email_service = EmailService()
        plain_code, hashed_code = email_service.generate_verification_code()
        user = User(
            id=str(uuid4()),
            email=user_data.email,
            username=user_data.username,
            password_hash=hash_password(user_data.password),
            is_active=False,
            is_superuser=False,
            email_verified=False,
            verification_code=hashed_code,
            verification_code_expires_at=datetime.now(timezone.utc) + timedelta(minutes=15),
        )

    # Assign roles
    for role_name in user_data.roles:
        role = db.query(Role).filter(Role.name == role_name).first()
        if role:
            user.roles.append(role)

    db.add(user)
    db.commit()
    db.refresh(user)

    if skip_verification:
        logger.warning(
            f"User registered with email-verification bypass enabled: {user.email} "
            f"(SKIP_EMAIL_VERIFICATION=true — DEV ONLY)"
        )
        return {
            "user_id": user.id,
            "id": user.id,
            "email": user.email,
            "username": user.username,
            "is_active": True,
            "email_verified": True,
            "message": "User created and auto-verified (SKIP_EMAIL_VERIFICATION=true)",
        }

    # Production path — send verification email
    await email_service.send_verification_email(
        to_email=user.email,
        code=plain_code,
        username=user.username,
    )

    logger.info(f"User registered (pending verification): {user.email}")

    return {
        "user_id": user.id,
        "message": "Verification code sent to email",
    }


@app.post("/login")
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """
    Login and receive JWT tokens, or 2FA challenge if TOTP is enabled.

    Returns:
        - TokenResponse: If user has no 2FA enabled
        - TwoFactorChallengeResponse: If user has TOTP enabled (requires /login/2fa step)
    """
    # Find user
    user = db.query(User).filter(User.email == credentials.email).first()

    if not user or not verify_password(credentials.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="User account is disabled"
        )

    # Check if user has TOTP 2FA enabled
    if user.totp_enabled:
        # Return 2FA challenge - user must complete /login/2fa with TOTP code
        challenge_token = create_2fa_challenge_token(user.id)
        logger.info(f"2FA challenge issued for user: {user.email}")
        return TwoFactorChallengeResponse(requires_2fa=True, challenge_token=challenge_token)

    # No 2FA - create tokens directly
    token_payload = create_token_payload(
        user_id=user.id,
        email=user.email,
        roles=[role.name for role in user.roles],
        companies=[company.id for company in user.companies],
        is_superuser=user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    refresh_token = create_refresh_token({"sub": user.id})

    logger.info(f"User logged in: {user.email}")

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@app.post("/login/2fa", response_model=TokenResponse)
async def login_2fa(data: Login2FARequest, db: Session = Depends(get_db)):
    """
    Complete login with 2FA code after password verification.

    Accepts either:
    - 6-digit TOTP code from authenticator app
    - 12-char backup code (format: XXXX-XXXX-XXXX)

    Args:
        data: Login2FARequest containing:
            - challenge_token: Short-lived JWT from /login response
            - code: TOTP code (6 digits) or backup code (12 chars with dashes)

    Returns:
        TokenResponse with access_token and refresh_token on success

    Raises:
        401: Invalid/expired challenge token or invalid code
        404: User not found
        400: 2FA not enabled for user
    """
    import json

    # 1. Validate challenge_token (5-minute JWT)
    try:
        payload = jwt.decode(data.challenge_token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Challenge token has expired. Please login again.",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid challenge token"
        )

    # 2. Validate token type
    if payload.get("type") != "2fa_challenge":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    # 3. Extract user_id and get user
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid challenge token payload"
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Verify user has 2FA enabled (sanity check)
    if not user.totp_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="2FA is not enabled for this account"
        )

    if not user.totp_secret:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="2FA configuration error. Please contact support.",
        )

    # 4. Determine code type and verify
    code = data.code.strip()
    code_is_totp = code.isdigit() and len(code) == 6
    code_is_backup = len(code.replace("-", "")) == 12

    if not code_is_totp and not code_is_backup:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid code format. Provide 6-digit TOTP or 12-character backup code.",
        )

    totp_service = get_totp_service()
    code_valid = False

    if code_is_totp:
        # Verify TOTP code
        try:
            decrypted_secret = decrypt_totp_secret(user.totp_secret)
        except ValueError:
            logger.error(f"Failed to decrypt TOTP secret for user {user.email}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="2FA configuration error"
            )

        code_valid = totp_service.verify_totp(decrypted_secret, code)

        if not code_valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP code"
            )
    else:
        # Verify backup code (and consume it if valid)
        backup_codes = user.totp_backup_codes

        if not backup_codes:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid backup code"
            )

        # Handle both list and JSON string formats
        if isinstance(backup_codes, str):
            try:
                backup_codes = json.loads(backup_codes)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse backup codes for user {user.email}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="2FA configuration error",
                )

        code_valid, code_index = totp_service.verify_backup_code(code, backup_codes)

        if not code_valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid backup code"
            )

        # Consume the backup code (remove from list)
        backup_codes.pop(code_index)
        user.totp_backup_codes = backup_codes  # Native list - SQLAlchemy handles ARRAY conversion
        db.commit()

        logger.info(
            f"Backup code used for user: {user.email}. Remaining codes: {len(backup_codes)}"
        )

    # 5. Generate and return access/refresh tokens
    token_payload = create_token_payload(
        user_id=user.id,
        email=user.email,
        roles=[role.name for role in user.roles],
        companies=[company.id for company in user.companies],
        is_superuser=user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    refresh_token = create_refresh_token({"sub": user.id})

    logger.info(f"2FA login completed for user: {user.email}")

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@app.post("/refresh", response_model=TokenResponse)
async def refresh_token(token: str, db: Session = Depends(get_db)):
    """Refresh access token using refresh token."""
    payload = verify_refresh_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token"
        )

    # Get user
    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
        )

    # Create new tokens
    token_payload = create_token_payload(
        user_id=user.id,
        email=user.email,
        roles=[role.name for role in user.roles],
        companies=[company.id for company in user.companies],
        is_superuser=user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    new_refresh_token = create_refresh_token({"sub": user.id})

    return TokenResponse(access_token=access_token, refresh_token=new_refresh_token)


@app.post("/verify-email", response_model=VerifyEmailResponse)
async def verify_email(data: VerifyEmailRequest, db: Session = Depends(get_db)):
    """
    Verify email with 6-digit code.

    Security:
    - Uses constant-time comparison via bcrypt (EmailService.verify_code)
    - Generic error messages prevent email enumeration
    - Lockout after 5 failed attempts (30 min cooldown)

    Returns:
    - On success: JWT tokens (access + refresh)
    - On failure: Error with remaining attempts
    """
    # 30-minute lockout period
    LOCKOUT_MINUTES = 30
    MAX_ATTEMPTS = 5

    # 1. Find user by email (generic error to prevent enumeration)
    user = db.query(User).filter(User.email == data.email).first()

    if not user:
        # Return generic error - don't reveal if email exists
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid verification code"
        )

    # 2. Check lockout (verification_attempts >= 5)
    if user.verification_attempts >= MAX_ATTEMPTS:
        # Check if lockout period has passed
        if user.last_verification_request_at:
            last_request_aware = ensure_utc_aware(user.last_verification_request_at)
            lockout_expires = last_request_aware + timedelta(minutes=LOCKOUT_MINUTES)
            if datetime.now(timezone.utc) < lockout_expires:
                remaining_minutes = int(
                    (lockout_expires - datetime.now(timezone.utc)).total_seconds() / 60
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Too many failed attempts. Try again in {remaining_minutes} minutes.",
                )
            else:
                # Lockout expired, reset attempts
                user.verification_attempts = 0

    # 3. Check if user has a verification code
    if not user.verification_code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No verification code found. Please request a new one.",
        )

    # 4. Check code expiration
    if user.verification_code_expires_at:
        expires_at_aware = ensure_utc_aware(user.verification_code_expires_at)
        if datetime.now(timezone.utc) > expires_at_aware:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Verification code has expired. Please request a new one.",
            )

    # 5. Verify code using constant-time comparison
    email_service = EmailService()
    if not email_service.verify_code(data.code, user.verification_code):
        # Increment verification_attempts
        user.verification_attempts += 1
        user.last_verification_request_at = datetime.now(timezone.utc)
        db.commit()

        remaining_attempts = MAX_ATTEMPTS - user.verification_attempts

        if remaining_attempts <= 0:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Too many failed attempts. Account locked for {LOCKOUT_MINUTES} minutes.",
            )

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid verification code. {remaining_attempts} attempts remaining.",
        )

    # 6. Success: Update user state
    user.email_verified = True
    user.is_active = True
    user.verification_code = None
    user.verification_code_expires_at = None
    user.verification_attempts = 0
    user.last_verification_request_at = None
    db.commit()

    # 7. Generate and return tokens
    token_payload = create_token_payload(
        user_id=user.id,
        email=user.email,
        roles=[role.name for role in user.roles],
        companies=[company.id for company in user.companies],
        is_superuser=user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    refresh_token = create_refresh_token({"sub": user.id})

    logger.info(f"Email verified for user: {user.email}")

    return VerifyEmailResponse(
        message="Email verified successfully",
        access_token=access_token,
        refresh_token=refresh_token,
    )


@app.post("/verify", response_model=VerifyTokenResponse)
async def verify_token(request: VerifyTokenRequest, db: Session = Depends(get_db)):
    """
    Verify JWT or API token validity (for API/MCP servers).

    Supports dual auth:
    - JWT tokens: Full payload returned
    - API tokens (lgn_*): User info from database
    """
    token = request.token

    # Check if API token
    if token.startswith("lgn_"):
        api_token = verify_api_token(token, db)
        if not api_token:
            return VerifyTokenResponse(valid=False)

        # Get user details for API token
        user = db.query(User).filter(User.id == api_token.user_id).first()
        if not user:
            return VerifyTokenResponse(valid=False)

        # Schedule async usage update (Ragen's condition)
        schedule_api_token_usage_update(api_token.id)

        return VerifyTokenResponse(
            valid=True,
            user_id=user.id,
            email=user.email,
            roles=[role.name for role in user.roles],
            companies=[c.id for c in user.companies],
            is_superuser=user.is_superuser,
        )

    # Default: JWT verification
    payload = verify_access_token(token)

    if not payload:
        return VerifyTokenResponse(valid=False)

    return VerifyTokenResponse(
        valid=True,
        user_id=payload.get("sub"),
        email=payload.get("email"),
        roles=payload.get("roles", []),
        companies=payload.get("companies", []),
        is_superuser=payload.get("is_superuser", False),
    )


@app.post("/logout", response_model=LogoutResponse)
async def logout(
    request: LogoutRequest = LogoutRequest(),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Logout user by revoking current tokens (or all tokens).

    SECURITY: Blacklists BOTH access AND refresh tokens.
    GRACEFUL: Tokens without JTI are accepted (legacy support).

    Args:
        request: Contains optional refresh_token and revoke_all_sessions flag
        credentials: Bearer token from header
        db: Database session

    Returns:
        LogoutResponse with count of revoked tokens
    """
    access_token = credentials.credentials
    revoked_count = 0

    # First verify the access token to get user info
    payload = verify_access_token(access_token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")
    user_email = payload.get("email", "unknown")

    # Get user from database for additional context
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Handle revoke_all_sessions first (supersedes individual token handling)
    if request.revoke_all_sessions:
        if revoke_all_user_tokens(user_id):
            logger.info(f"All sessions revoked for user: {user_email}")
            return LogoutResponse(
                message="All sessions revoked",
                revoked_tokens=-1,  # -1 indicates "all"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to revoke all sessions",
            )

    # Decode access token without validation (we already validated above)
    # We need the raw payload for JTI and exp
    try:
        access_payload = jwt.decode(
            access_token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"verify_exp": False},  # Allow expired tokens to logout
        )
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid access token")

    # ISSUE #5 FIX: Handle tokens without JTI (graceful degradation)
    access_jti = access_payload.get("jti")
    if not access_jti:
        logger.info(f"Logout for legacy token (no JTI) - user: {user_email}")
        return LogoutResponse(
            message="Logged out (legacy token - blacklist skipped)", revoked_tokens=0
        )

    # Blacklist access token
    access_exp = access_payload.get("exp", 0)
    access_remaining_ttl = max(0, int(access_exp - datetime.now(timezone.utc).timestamp()))
    if add_to_blacklist(access_jti, access_remaining_ttl):
        revoked_count += 1

    # ISSUE #1 FIX: Blacklist refresh token if provided
    if request.refresh_token:
        try:
            refresh_payload = jwt.decode(
                request.refresh_token,
                SECRET_KEY,
                algorithms=[ALGORITHM],
                options={"verify_exp": False},
            )
            refresh_jti = refresh_payload.get("jti")
            if refresh_jti:
                refresh_exp = refresh_payload.get("exp", 0)
                refresh_remaining_ttl = max(
                    0, int(refresh_exp - datetime.now(timezone.utc).timestamp())
                )
                if add_to_blacklist(refresh_jti, refresh_remaining_ttl):
                    revoked_count += 1
                    logger.info(f"Refresh token blacklisted for user: {user_email}")
            else:
                logger.info(f"Refresh token has no JTI - skipping blacklist")
        except jwt.InvalidTokenError:
            logger.warning(f"Invalid refresh token provided - skipping blacklist")
            # Don't fail the request — access token is still logged out
    else:
        logger.warning(f"No refresh token provided - only access token revoked")

    logger.info(f"User logged out: {user_email}, tokens revoked: {revoked_count}")

    return LogoutResponse(message="Successfully logged out", revoked_tokens=revoked_count)


# ============================================================================
# Rate Limiting Constants
# ============================================================================
VERIFICATION_RATE_LIMIT_SECONDS = 60  # 1 minute between requests
VERIFICATION_DAILY_LIMIT = 10  # Max 10 requests per day


@app.post("/resend-verification", response_model=ResendVerificationResponse)
async def resend_verification(data: ResendVerificationRequest, db: Session = Depends(get_db)):
    """
    Resend verification code with rate limiting.

    Security: Always returns success to prevent email enumeration.
    Rate limits:
    - 1 request per minute
    - 10 requests per day
    """
    email_service = EmailService()
    now = datetime.now(timezone.utc)

    # Find unverified user by email
    user = db.query(User).filter(User.email == data.email, User.email_verified == False).first()

    # Always return success to prevent email enumeration
    if not user:
        logger.info(f"Resend verification requested for non-existent/verified email")
        return ResendVerificationResponse(
            message="If an unverified account exists with this email, a verification code has been sent."
        )

    # Rate limit check: 1 minute since last request
    if user.last_verification_request_at:
        last_request_aware = ensure_utc_aware(user.last_verification_request_at)
        time_since_last = (now - last_request_aware).total_seconds()
        if time_since_last < VERIFICATION_RATE_LIMIT_SECONDS:
            logger.warning(f"Rate limit (1min) hit for user: {user.email}")
            return ResendVerificationResponse(
                message="If an unverified account exists with this email, a verification code has been sent."
            )

    # Rate limit check: 10 per day (count requests since start of day)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    last_request_for_daily = (
        ensure_utc_aware(user.last_verification_request_at)
        if user.last_verification_request_at
        else None
    )
    if last_request_for_daily and last_request_for_daily >= start_of_day:
        # Check if verification_attempts counter exceeds daily limit
        if user.verification_attempts >= VERIFICATION_DAILY_LIMIT:
            logger.warning(f"Daily rate limit hit for user: {user.email}")
            return ResendVerificationResponse(
                message="If an unverified account exists with this email, a verification code has been sent."
            )
    else:
        # Reset daily counter if last request was before today
        user.verification_attempts = 0

    # Generate new verification code
    plain_code, hashed_code = email_service.generate_verification_code()

    # Update user with new code and rate limiting fields
    user.verification_code = hashed_code
    user.verification_code_expires_at = now + timedelta(minutes=15)
    user.last_verification_request_at = now
    user.verification_attempts = (user.verification_attempts or 0) + 1

    db.commit()

    # Send verification email (async but don't await - fire and forget for security)
    try:
        await email_service.send_verification_email(
            to_email=user.email, code=plain_code, username=user.username
        )
        logger.info(f"Verification email resent to: {user.email}")
    except Exception as e:
        logger.error(f"Failed to send verification email to {user.email}: {e}")
        # Don't reveal email sending failures to prevent enumeration

    return ResendVerificationResponse(
        message="If an unverified account exists with this email, a verification code has been sent."
    )


# ============================================================================
# TOTP 2FA Endpoints
# ============================================================================


@app.post("/2fa/setup", response_model=TOTPSetupResponse)
async def setup_2fa(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """
    Initialize TOTP 2FA setup - returns QR code and secret.

    The secret is stored encrypted but 2FA is NOT enabled yet.
    User must call /2fa/verify-setup with a valid TOTP code to enable 2FA.

    Returns:
    - secret: Base32 TOTP secret (for manual entry in authenticator app)
    - qr_code: Base64 encoded PNG QR code image
    - provisioning_uri: otpauth:// URI for authenticator apps
    """
    from auth.services.totp_service import TOTPService, encrypt_totp_secret

    # Verify token and get user
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 1. Check 2FA not already enabled
    if user.totp_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="2FA is already enabled for this account",
        )

    # 2. Generate secret
    totp = TOTPService()
    secret = totp.generate_secret()

    # 3. Encrypt and store (not enabled yet)
    encrypted_secret = encrypt_totp_secret(secret)
    user.totp_secret = encrypted_secret
    db.commit()

    # DEBUG: Log secret details (first/last 4 chars only for security)
    logger.warning(f"[2FA DEBUG] SETUP for {user.email}")
    logger.warning(f"[2FA DEBUG] Raw secret: {secret[:4]}...{secret[-4:]} (len={len(secret)})")
    logger.warning(
        f"[2FA DEBUG] Encrypted: {encrypted_secret[:20]}... (len={len(encrypted_secret)})"
    )
    logger.warning(f"[2FA DEBUG] Stored in DB as user.totp_secret")

    # 4. Generate provisioning URI and QR code
    provisioning_uri = totp.get_provisioning_uri(secret, user.email)
    qr_code = totp.generate_qr_code(provisioning_uri)

    logger.info(f"2FA setup initiated for user: {user.email}")

    # 5. Return setup data
    return TOTPSetupResponse(secret=secret, qr_code=qr_code, provisioning_uri=provisioning_uri)


@app.post("/2fa/disable", response_model=Disable2FAResponse)
async def disable_2fa(
    data: Disable2FARequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Disable 2FA for the authenticated user.

    Security requirements:
    - User must be authenticated (Bearer token)
    - Must provide current password
    - Must provide valid TOTP code

    This clears:
    - totp_secret (set to None)
    - totp_enabled (set to False)
    - totp_backup_codes (set to None)
    """
    # 1. Verify token and get user
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 2. Check if 2FA is actually enabled
    if not user.totp_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="2FA is not enabled for this account"
        )

    # 3. Verify password
    if not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid password")

    # 4. Verify current TOTP code
    if not user.totp_secret:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="TOTP secret not found")

    try:
        decrypted_secret = decrypt_totp_secret(user.totp_secret)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to decrypt TOTP secret",
        )

    totp_service = get_totp_service()
    if not totp_service.verify_totp(decrypted_secret, data.code):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP code")

    # 5. Disable 2FA - clear all TOTP fields
    user.totp_secret = None
    user.totp_enabled = False
    user.totp_backup_codes = None
    user.totp_enabled_at = None
    db.commit()

    logger.info(f"2FA disabled for user: {user.email}")

    return Disable2FAResponse(message="2FA has been disabled successfully", totp_enabled=False)


# ============================================================================
# User Management
# ============================================================================


@app.get("/users/me", response_model=UserResponse)
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """Get current user information."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return UserResponse(
        id=user.id,
        email=user.email,
        username=user.username,
        roles=[role.name for role in user.roles],
        is_active=user.is_active,
        is_superuser=user.is_superuser,
        created_at=user.created_at.isoformat(),
        # Profile fields
        first_name=user.first_name,
        last_name=user.last_name,
        display_name=user.display_name,
        avatar_url=user.avatar_url,
        bio=user.bio,
        phone_number=user.phone_number,
        timezone=user.timezone,
        locale=user.locale,
        profile_completed_at=user.profile_completed_at.isoformat()
        if user.profile_completed_at
        else None,
        # Auth status fields
        email_verified=user.email_verified,
        totp_enabled=user.totp_enabled,
        oauth_provider=user.oauth_provider,
    )


@app.get("/users", response_model=List[UserResponse])
async def list_users(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
):
    """List all users (admin only)."""
    payload = verify_access_token(credentials.credentials)

    if not payload or not payload.get("is_superuser"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")

    users = db.query(User).offset(skip).limit(limit).all()

    return [
        UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            roles=[role.name for role in user.roles],
            is_active=user.is_active,
            is_superuser=user.is_superuser,
            created_at=user.created_at.isoformat(),
            # Profile fields
            first_name=user.first_name,
            last_name=user.last_name,
            display_name=user.display_name,
            avatar_url=user.avatar_url,
            bio=user.bio,
            phone_number=user.phone_number,
            timezone=user.timezone,
            locale=user.locale,
            profile_completed_at=user.profile_completed_at.isoformat()
            if user.profile_completed_at
            else None,
            # Auth status fields
            email_verified=user.email_verified,
            totp_enabled=user.totp_enabled,
            oauth_provider=user.oauth_provider,
        )
        for user in users
    ]


# ============================================================================
# User Profile Endpoints
# ============================================================================


@app.get("/users/me/profile", response_model=UserProfileResponse)
async def get_current_user_profile(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """Get current user information with full profile fields."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return _build_user_profile_response(user)


@app.patch("/users/me", response_model=UserProfileResponse)
async def update_current_user_profile(
    profile_data: UserProfileUpdate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Partial update of current user's profile.

    Only provided fields are updated. Null/missing fields are ignored.
    Validates timezone, locale, and phone_number formats.
    """
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Validate and update fields
    update_data = profile_data.model_dump(exclude_unset=True)

    # Validate timezone if provided
    if "timezone" in update_data and update_data["timezone"]:
        if not is_valid_timezone(update_data["timezone"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timezone: {update_data['timezone']}. Use IANA timezone format.",
            )

    # Validate locale if provided
    if "locale" in update_data and update_data["locale"]:
        if not is_valid_locale(update_data["locale"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid locale: {update_data['locale']}. Use format like 'en-US'.",
            )

    # Validate phone number if provided
    if "phone_number" in update_data and update_data["phone_number"]:
        if not validate_phone_number(update_data["phone_number"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid phone number format. Use E.164 format (e.g., +1234567890).",
            )

    # Apply updates
    for field, value in update_data.items():
        setattr(user, field, value)

    # Check if profile is now complete (first_name and last_name set)
    if user.first_name and user.last_name and not user.profile_completed_at:
        user.profile_completed_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(user)

    logger.info(f"Profile updated for user: {user.email}")

    return _build_user_profile_response(user)


# ============================================================================
# User Deletion Endpoints
# ============================================================================


@app.get("/users/me/delete-preview", response_model=DeleteUserPreviewResponse)
async def preview_user_deletion(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """
    Preview cascade impact of deleting current user account.

    Shows:
    - Owned companies that will be deleted
    - Number of projects and agents affected
    - Members who will lose access
    - Whether cascade acknowledgment is required
    """
    from sqlalchemy import text

    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Get owned companies with entity counts
    owned_companies = db.execute(
        text("""
        SELECT c.id, c.name,
               (SELECT COUNT(*) FROM projects WHERE company_id = c.id) as project_count,
               (SELECT COUNT(*) FROM agents WHERE company_id = c.id) as agent_count
        FROM companies c
        JOIN company_users cu ON c.id = cu.company_id
        WHERE cu.user_id = :user_id AND cu.role = 'owner'
    """),
        {"user_id": user.id},
    ).fetchall()

    company_ids = [c.id for c in owned_companies]

    # Get affected members count
    affected_members_count = 0
    if company_ids:
        result = db.execute(
            text("""
            SELECT COUNT(DISTINCT cu.user_id)
            FROM company_users cu
            WHERE cu.company_id = ANY(:company_ids)
              AND cu.user_id != :owner_id
        """),
            {"company_ids": company_ids, "owner_id": user.id},
        ).scalar()
        affected_members_count = result or 0

    total_projects = sum(c.project_count for c in owned_companies)
    total_agents = sum(c.agent_count for c in owned_companies)

    return DeleteUserPreviewResponse(
        user_email=user.email,
        owned_companies_count=len(owned_companies),
        owned_companies=[
            {
                "id": c.id,
                "name": c.name,
                "project_count": c.project_count,
                "agent_count": c.agent_count,
            }
            for c in owned_companies
        ],
        affected_members_count=affected_members_count,
        total_projects=total_projects,
        total_agents=total_agents,
        requires_cascade_acknowledgment=len(owned_companies) > 0,
    )


@app.delete("/users/me", response_model=DeleteUserResponse)
async def delete_current_user(
    request: DeleteUserRequest,
    background_tasks: BackgroundTasks,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Hard delete current user account with phased cascade deletion.

    Security:
    - Requires email confirmation matching account email
    - Requires cascade acknowledgment if user owns companies
    - Creates persistent deletion task for tracking
    - Invalidates all sessions before deletion
    - Cleans external storage (Qdrant/Neo4j) before PostgreSQL
    - Creates audit record before any deletion

    Returns task_id for tracking progress via /users/me/deletion-status/{task_id}
    """
    from sqlalchemy import text

    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Verify confirmation matches email
    if request.confirmation != user.email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation does not match email address",
        )

    # Get owned companies
    owned_company_ids = (
        db.execute(
            text("""
        SELECT company_id FROM company_users
        WHERE user_id = :user_id AND role = 'owner'
    """),
            {"user_id": user.id},
        )
        .scalars()
        .all()
    )
    owned_company_ids = list(owned_company_ids)

    # Require cascade acknowledgment if owning companies
    if owned_company_ids and not request.acknowledge_cascade:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must acknowledge cascade deletion when owning companies. Call /users/me/delete-preview first.",
        )

    # Create deletion task
    task_id = str(uuid4())
    task = DeletionTask(
        id=task_id,
        user_id=user.id,
        user_email=user.email,
        status="pending",
        owned_company_ids=owned_company_ids,
        progress={},
    )
    db.add(task)
    db.commit()

    # Create audit record BEFORE any deletion
    await _create_deletion_audit(task, db)

    # Execute deletion
    if owned_company_ids:
        # Large cascade - use background task
        background_tasks.add_task(execute_user_deletion, task_id)
        logger.info(f"User deletion started (background): {user.email}, task: {task_id}")
        return DeleteUserResponse(
            status="processing",
            message="Account deletion started. Track progress via /users/me/deletion-status endpoint.",
            task_id=task_id,
        )
    else:
        # Simple delete - immediate
        await execute_user_deletion(task_id)
        logger.info(f"User deletion completed (immediate): {user.email}")
        return DeleteUserResponse(
            status="deleted", message="Account deleted successfully", task_id=task_id
        )


@app.get("/users/me/deletion-status/{task_id}", response_model=DeletionStatusResponse)
async def get_deletion_status(task_id: str, db: Session = Depends(get_db)):
    """
    Get real-time status of a deletion task.

    Note: Does NOT require authentication since user may be logged out
    during deletion. Task ID serves as the auth token.
    """
    task = db.query(DeletionTask).filter(DeletionTask.id == task_id).first()

    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deletion task not found")

    # Estimate remaining time based on progress
    remaining = None
    if task.progress and task.status == "postgres_deletion":
        total = task.progress.get("companies_total", 0)
        done = task.progress.get("companies_deleted", 0)
        if total > 0 and done > 0 and task.started_at:
            elapsed = (datetime.now(timezone.utc) - task.started_at).total_seconds()
            rate = done / elapsed
            remaining = int((total - done) / rate) if rate > 0 else None

    return DeletionStatusResponse(
        task_id=task.id,
        status=task.status,
        current_phase=task.current_phase,
        progress=task.progress,
        error_message=task.error_message,
        started_at=task.started_at.isoformat() if task.started_at else None,
        completed_at=task.completed_at.isoformat() if task.completed_at else None,
        estimated_remaining_seconds=remaining,
    )


# ============================================================================
# API Token Management
# ============================================================================


@app.post(
    "/users/me/api-tokens",
    response_model=ApiTokenCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_api_token_endpoint(
    token_data: ApiTokenCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Create a new API token for the authenticated user.

    IMPORTANT: The token is only returned ONCE in this response.
    Store it securely - you will not be able to see it again.

    API tokens CANNOT be created using another API token (JWT required).
    """
    # Verify JWT (API tokens cannot create other API tokens)
    token = credentials.credentials
    if token.startswith("lgn_"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API tokens cannot create other API tokens. Use JWT authentication.",
        )

    payload = verify_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Check token count limit (Ragen's condition: max 25 per user)
    can_create, current_count = can_create_token(user_id, db)
    if not can_create:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum number of API tokens ({MAX_TOKENS_PER_USER}) reached. Revoke existing tokens to create new ones.",
        )

    # Validate scopes
    if token_data.scopes:
        valid, invalid_scopes = validate_scopes(token_data.scopes)
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid scopes: {invalid_scopes}. Valid scopes: {VALID_SCOPES}",
            )

    # Generate token
    plaintext_token, token_prefix, token_hash, token_hint = generate_api_token()

    # Calculate expiration
    expires_at = None
    if token_data.expires_in_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=token_data.expires_in_days)

    # Create database record
    api_token = ApiToken(
        id=str(uuid4()),
        user_id=user_id,
        name=token_data.name,
        token_prefix=token_prefix,
        token_hash=token_hash,
        token_hint=token_hint,
        scopes=token_data.scopes or [],
        expires_at=expires_at,
    )

    db.add(api_token)
    db.commit()
    db.refresh(api_token)

    return ApiTokenCreateResponse(
        id=api_token.id,
        name=api_token.name,
        token_hint=api_token.token_hint,
        scopes=api_token.scopes or [],
        created_at=api_token.created_at.isoformat(),
        last_used_at=api_token.last_used_at.isoformat() if api_token.last_used_at else None,
        expires_at=api_token.expires_at.isoformat() if api_token.expires_at else None,
        token=plaintext_token,
        warning="Save this token now. You won't be able to see it again.",
    )


@app.get("/users/me/api-tokens", response_model=ApiTokenListResponse)
async def list_api_tokens(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """List all API tokens for the authenticated user (metadata only, no plaintext)."""
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")

    tokens = (
        db.query(ApiToken)
        .filter(ApiToken.user_id == user_id, ApiToken.revoked_at.is_(None))
        .order_by(ApiToken.created_at.desc())
        .all()
    )

    return ApiTokenListResponse(
        tokens=[
            ApiTokenResponse(
                id=t.id,
                name=t.name,
                token_hint=t.token_hint,
                scopes=t.scopes or [],
                created_at=t.created_at.isoformat(),
                last_used_at=t.last_used_at.isoformat() if t.last_used_at else None,
                expires_at=t.expires_at.isoformat() if t.expires_at else None,
            )
            for t in tokens
        ],
        total=len(tokens),
    )


@app.get("/users/me/api-tokens/{token_id}", response_model=ApiTokenResponse)
async def get_api_token(
    token_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Get details of a specific API token."""
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")

    api_token = (
        db.query(ApiToken)
        .filter(ApiToken.id == token_id, ApiToken.user_id == user_id, ApiToken.revoked_at.is_(None))
        .first()
    )

    if not api_token:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API token not found")

    return ApiTokenResponse(
        id=api_token.id,
        name=api_token.name,
        token_hint=api_token.token_hint,
        scopes=api_token.scopes or [],
        created_at=api_token.created_at.isoformat(),
        last_used_at=api_token.last_used_at.isoformat() if api_token.last_used_at else None,
        expires_at=api_token.expires_at.isoformat() if api_token.expires_at else None,
    )


@app.patch("/users/me/api-tokens/{token_id}", response_model=ApiTokenResponse)
async def update_api_token(
    token_id: str,
    update_data: ApiTokenUpdate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Update API token name and/or scopes."""
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")

    api_token = (
        db.query(ApiToken)
        .filter(ApiToken.id == token_id, ApiToken.user_id == user_id, ApiToken.revoked_at.is_(None))
        .first()
    )

    if not api_token:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API token not found")

    # Apply partial updates
    if update_data.name is not None:
        api_token.name = update_data.name

    if update_data.scopes is not None:
        # Validate new scopes
        valid, invalid_scopes = validate_scopes(update_data.scopes)
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid scopes: {invalid_scopes}. Valid scopes: {VALID_SCOPES}",
            )
        api_token.scopes = update_data.scopes

    db.commit()
    db.refresh(api_token)

    return ApiTokenResponse(
        id=api_token.id,
        name=api_token.name,
        token_hint=api_token.token_hint,
        scopes=api_token.scopes or [],
        created_at=api_token.created_at.isoformat(),
        last_used_at=api_token.last_used_at.isoformat() if api_token.last_used_at else None,
        expires_at=api_token.expires_at.isoformat() if api_token.expires_at else None,
    )


@app.delete("/users/me/api-tokens/{token_id}", response_model=ApiTokenRevokeResponse)
async def revoke_api_token(
    token_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Revoke (soft delete) an API token."""
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user_id = payload.get("sub")

    api_token = (
        db.query(ApiToken)
        .filter(ApiToken.id == token_id, ApiToken.user_id == user_id, ApiToken.revoked_at.is_(None))
        .first()
    )

    if not api_token:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API token not found")

    # Soft delete by setting revoked_at
    api_token.revoked_at = datetime.now(timezone.utc)
    db.commit()

    return ApiTokenRevokeResponse(status="revoked", token_id=token_id)


# ============================================================================
# Role & Permission Management
# ============================================================================


@app.post("/roles", status_code=status.HTTP_201_CREATED)
async def create_role(
    role_data: RoleCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Create a new role (admin only)."""
    payload = verify_access_token(credentials.credentials)

    if not payload or not payload.get("is_superuser"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")

    # Check if role exists
    existing = db.query(Role).filter(Role.name == role_data.name).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Role already exists")

    # Create role
    role = Role(name=role_data.name, description=role_data.description)

    # Assign permissions
    for perm_id in role_data.permissions:
        perm = db.query(Permission).filter(Permission.id == perm_id).first()
        if perm:
            role.permissions.append(perm)

    db.add(role)
    db.commit()

    return {"id": role.id, "name": role.name, "message": "Role created"}


@app.get("/roles")
async def list_roles(db: Session = Depends(get_db)):
    """List all roles."""
    roles = db.query(Role).all()
    return [
        {
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "permissions": [{"resource": p.resource, "action": p.action} for p in role.permissions],
        }
        for role in roles
    ]


@app.get("/permissions")
async def list_permissions(db: Session = Depends(get_db)):
    """List all permissions."""
    permissions = db.query(Permission).all()
    return [
        {
            "id": perm.id,
            "resource": perm.resource,
            "action": perm.action,
            "description": perm.description,
        }
        for perm in permissions
    ]


# ============================================================================
# Two-Factor Authentication (2FA) Endpoints
# ============================================================================


@app.post("/2fa/verify-setup", response_model=TOTPVerifySetupResponse)
async def verify_2fa_setup(
    data: TOTPVerifyRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Verify TOTP code and enable 2FA for the user.

    This endpoint completes the 2FA setup flow:
    1. User has already called /2fa/setup to get QR code and secret
    2. User scans QR in authenticator app
    3. User submits 6-digit code from authenticator to this endpoint
    4. On success: 2FA is enabled and backup codes are returned

    Security:
    - Requires authenticated user (Bearer token)
    - TOTP secret must already be stored (from /2fa/setup)
    - Secret is decrypted using Fernet encryption
    - Backup codes are one-time use, bcrypt hashed in DB

    Returns:
    - enabled: true if 2FA was successfully enabled
    - backup_codes: List of 10 one-time backup codes (format: XXXX-XXXX-XXXX)
    """
    # 1. Verify access token and get user
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 2. Check if user has a TOTP secret stored (from /2fa/setup)
    if not user.totp_secret:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="2FA setup not initiated. Call /2fa/setup first.",
        )

    # 3. Check if 2FA is already enabled
    if user.totp_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="2FA is already enabled for this account",
        )

    # 4. Decrypt the stored TOTP secret
    # DEBUG: Log what we're reading from DB
    logger.warning(f"[2FA DEBUG] VERIFY for {user.email}")
    logger.warning(
        f"[2FA DEBUG] DB totp_secret: {user.totp_secret[:20] if user.totp_secret else 'None'}... (len={len(user.totp_secret) if user.totp_secret else 0})"
    )

    try:
        decrypted_secret = decrypt_totp_secret(user.totp_secret)
        logger.warning(
            f"[2FA DEBUG] Decrypted: {decrypted_secret[:4]}...{decrypted_secret[-4:]} (len={len(decrypted_secret)})"
        )
    except ValueError as e:
        logger.error(f"Failed to decrypt TOTP secret for user {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process 2FA setup. Please try again.",
        )

    # 5. Verify the TOTP code
    totp_service = get_totp_service()
    logger.warning(f"[2FA DEBUG] User code: {data.code}")

    # DEBUG: Generate expected code for comparison
    import pyotp as _pyotp

    _debug_totp = _pyotp.TOTP(decrypted_secret)
    _expected_code = _debug_totp.now()
    logger.warning(f"[2FA DEBUG] Expected code: {_expected_code}")
    logger.warning(f"[2FA DEBUG] Codes match: {data.code == _expected_code}")

    if not totp_service.verify_totp(decrypted_secret, data.code):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid verification code"
        )

    # 6. Generate backup codes
    plain_codes, hashed_codes = totp_service.generate_backup_codes()

    # 7. Update user: enable 2FA and store hashed backup codes
    user.totp_enabled = True
    user.totp_enabled_at = datetime.now(timezone.utc)
    user.totp_backup_codes = hashed_codes  # Native list - SQLAlchemy handles ARRAY conversion

    db.commit()

    logger.info(f"2FA enabled for user: {user.email}")

    # 8. Return success with plain backup codes (shown to user once)
    return TOTPVerifySetupResponse(enabled=True, backup_codes=plain_codes)


# ============================================================================
# Company Management
# ============================================================================


@app.post("/companies", status_code=status.HTTP_201_CREATED, response_model=CompanyResponse)
async def create_company(
    company_data: CompanyCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Create a new company. User becomes owner."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Create company
    company = Company(id=str(uuid4()), name=company_data.name, is_active=True)

    db.add(company)
    db.commit()
    db.refresh(company)

    # Add creator as owner
    from sqlalchemy import insert

    db.execute(
        insert(company_users).values(
            company_id=company.id,
            user_id=user.id,
            role="owner",
            joined_at=datetime.now(timezone.utc),
        )
    )
    db.commit()

    logger.info(f"Company created: {company.name} by {user.email}")

    return CompanyResponse(
        id=company.id,
        name=company.name,
        created_at=company.created_at.isoformat(),
        is_active=company.is_active,
        user_count=1,
    )


@app.get("/companies", response_model=List[CompanyResponse])
async def list_companies(
    credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)
):
    """List companies user belongs to (or all if superuser)."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Super admin sees all companies
    if user.is_superuser:
        companies = db.query(Company).all()
    else:
        # Regular user sees only their companies
        companies = user.companies

    return [
        CompanyResponse(
            id=company.id,
            name=company.name,
            created_at=company.created_at.isoformat(),
            is_active=company.is_active,
            user_count=len(company.users),
        )
        for company in companies
    ]


@app.get("/companies/{company_id}", response_model=CompanyResponse)
async def get_company(
    company_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Get company details."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    # Check access (super admin or company member)
    validate_company_membership(db, user, company)

    return CompanyResponse(
        id=company.id,
        name=company.name,
        created_at=company.created_at.isoformat(),
        is_active=company.is_active,
        user_count=len(company.users),
    )


@app.post("/companies/{company_id}/users", status_code=status.HTTP_201_CREATED)
async def add_user_to_company(
    company_id: str,
    user_data: AddUserToCompany,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Add a user to company."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    current_user = db.query(User).filter(User.id == payload["sub"]).first()

    if not current_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Current user not found")

    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    # Check access (super admin or company owner)
    if not current_user.is_superuser:
        # Check if user is owner of this company
        result = db.execute(
            company_users.select().where(
                (company_users.c.company_id == company_id)
                & (company_users.c.user_id == current_user.id)
                & (company_users.c.role == "owner")
            )
        ).first()

        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Only company owners can add users"
            )

    # Get user to add
    user_to_add = db.query(User).filter(User.id == user_data.user_id).first()

    if not user_to_add:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User to add not found")

    # Check if user already in company
    existing = db.execute(
        company_users.select().where(
            (company_users.c.company_id == company_id)
            & (company_users.c.user_id == user_data.user_id)
        )
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="User already in company"
        )

    # Add user to company
    from sqlalchemy import insert

    db.execute(
        insert(company_users).values(
            company_id=company_id,
            user_id=user_data.user_id,
            role=user_data.role,
            joined_at=datetime.now(timezone.utc),
        )
    )
    db.commit()

    logger.info(f"User {user_to_add.email} added to company {company.name} as {user_data.role}")

    return {
        "message": "User added to company successfully",
        "user_id": user_data.user_id,
        "company_id": company_id,
        "role": user_data.role,
    }


@app.delete("/companies/{company_id}/users/{user_id}", status_code=status.HTTP_200_OK)
async def remove_user_from_company(
    company_id: str,
    user_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Remove a user from company."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    current_user = db.query(User).filter(User.id == payload["sub"]).first()

    if not current_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Current user not found")

    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    # Check access (super admin or company owner)
    if not current_user.is_superuser:
        result = db.execute(
            company_users.select().where(
                (company_users.c.company_id == company_id)
                & (company_users.c.user_id == current_user.id)
                & (company_users.c.role == "owner")
            )
        ).first()

        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Only company owners can remove users"
            )

    # Remove user from company
    from sqlalchemy import delete

    result = db.execute(
        delete(company_users).where(
            (company_users.c.company_id == company_id) & (company_users.c.user_id == user_id)
        )
    )
    db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not in company")

    logger.info(f"User {user_id} removed from company {company.name}")

    return {
        "message": "User removed from company successfully",
        "user_id": user_id,
        "company_id": company_id,
    }


# ============================================================================
# Project Management
# ============================================================================


@app.post(
    "/companies/{company_id}/projects",
    status_code=status.HTTP_201_CREATED,
    response_model=ProjectResponse,
)
async def create_project(
    company_id: str,
    project_data: ProjectCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Create a new project under company."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    # Check access - validate active company and membership
    validate_company_membership(db, user, company)

    # Create project
    project = Project(
        id=str(uuid4()),
        company_id=company_id,
        name=project_data.name,
        description=project_data.description,
    )

    db.add(project)
    db.commit()
    db.refresh(project)

    logger.info(f"Project created: {project.name} under company {company.name}")

    return ProjectResponse(
        id=project.id,
        company_id=project.company_id,
        name=project.name,
        description=project.description,
        created_at=project.created_at.isoformat(),
    )


@app.get("/companies/{company_id}/projects", response_model=List[ProjectResponse])
async def list_projects(
    company_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """List all projects under company."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    company = db.query(Company).filter(Company.id == company_id).first()

    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    # Check access - validate active company and membership
    validate_company_membership(db, user, company)

    projects = db.query(Project).filter(Project.company_id == company_id).all()

    return [
        ProjectResponse(
            id=project.id,
            company_id=project.company_id,
            name=project.name,
            description=project.description,
            created_at=project.created_at.isoformat(),
        )
        for project in projects
    ]


@app.get("/projects/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Get project details."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    project = (
        db.query(Project)
        .options(joinedload(Project.company))
        .filter(Project.id == project_id)
        .first()
    )

    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Check access via company
    validate_company_membership(db, user, project.company)

    return ProjectResponse(
        id=project.id,
        company_id=project.company_id,
        name=project.name,
        description=project.description,
        created_at=project.created_at.isoformat(),
    )


# ============================================================================
# Repository Management
# ============================================================================


@app.post(
    "/projects/{project_id}/repositories",
    status_code=status.HTTP_201_CREATED,
    response_model=RepositoryResponse,
)
async def create_repository(
    project_id: str,
    repo_data: RepositoryCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Create a new repository under project."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    project = (
        db.query(Project)
        .options(joinedload(Project.company))
        .filter(Project.id == project_id)
        .first()
    )

    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Check access via company
    validate_company_membership(db, user, project.company)

    # Create repository
    repository = Repository(
        id=str(uuid4()), project_id=project_id, name=repo_data.name, url=repo_data.url
    )

    db.add(repository)
    db.commit()
    db.refresh(repository)

    logger.info(f"Repository created: {repository.name} under project {project.name}")

    return RepositoryResponse(
        id=repository.id,
        project_id=repository.project_id,
        name=repository.name,
        url=repository.url,
        created_at=repository.created_at.isoformat(),
    )


@app.get("/projects/{project_id}/repositories", response_model=List[RepositoryResponse])
async def list_repositories(
    project_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """List all repositories under project."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    project = (
        db.query(Project)
        .options(joinedload(Project.company))
        .filter(Project.id == project_id)
        .first()
    )

    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Check access via company
    validate_company_membership(db, user, project.company)

    repositories = db.query(Repository).filter(Repository.project_id == project_id).all()

    return [
        RepositoryResponse(
            id=repo.id,
            project_id=repo.project_id,
            name=repo.name,
            url=repo.url,
            created_at=repo.created_at.isoformat(),
        )
        for repo in repositories
    ]


@app.get("/repositories/{repository_id}", response_model=RepositoryResponse)
async def get_repository(
    repository_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Get repository details."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    repository = (
        db.query(Repository)
        .options(joinedload(Repository.project).joinedload(Project.company))
        .filter(Repository.id == repository_id)
        .first()
    )

    if not repository:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Repository not found")

    # Check access via project -> company
    validate_company_membership(db, user, repository.project.company)

    return RepositoryResponse(
        id=repository.id,
        project_id=repository.project_id,
        name=repository.name,
        url=repository.url,
        created_at=repository.created_at.isoformat(),
    )


# ============================================================================
# Branch Management
# ============================================================================


@app.post(
    "/repositories/{repository_id}/branches",
    status_code=status.HTTP_201_CREATED,
    response_model=BranchResponse,
)
async def create_branch(
    repository_id: str,
    branch_data: BranchCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Create a new branch under repository."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    repository = (
        db.query(Repository)
        .options(joinedload(Repository.project).joinedload(Project.company))
        .filter(Repository.id == repository_id)
        .first()
    )

    if not repository:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Repository not found")

    # Check access via project -> company
    validate_company_membership(db, user, repository.project.company)

    # Create branch
    branch = Branch(
        id=str(uuid4()),
        repository_id=repository_id,
        name=branch_data.name,
        commit_sha=branch_data.commit_sha,
    )

    db.add(branch)
    db.commit()
    db.refresh(branch)

    logger.info(f"Branch created: {branch.name} under repository {repository.name}")

    return BranchResponse(
        id=branch.id,
        repository_id=branch.repository_id,
        name=branch.name,
        commit_sha=branch.commit_sha,
        created_at=branch.created_at.isoformat(),
    )


@app.get("/repositories/{repository_id}/branches", response_model=List[BranchResponse])
async def list_branches(
    repository_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """List all branches under repository."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    repository = (
        db.query(Repository)
        .options(joinedload(Repository.project).joinedload(Project.company))
        .filter(Repository.id == repository_id)
        .first()
    )

    if not repository:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Repository not found")

    # Check access via project -> company
    validate_company_membership(db, user, repository.project.company)

    branches = db.query(Branch).filter(Branch.repository_id == repository_id).all()

    return [
        BranchResponse(
            id=branch.id,
            repository_id=branch.repository_id,
            name=branch.name,
            commit_sha=branch.commit_sha,
            created_at=branch.created_at.isoformat(),
        )
        for branch in branches
    ]


@app.get("/branches/{branch_id}", response_model=BranchResponse)
async def get_branch(
    branch_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """Get branch details."""
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    branch = (
        db.query(Branch)
        .options(
            joinedload(Branch.repository).joinedload(Repository.project).joinedload(Project.company)
        )
        .filter(Branch.id == branch_id)
        .first()
    )

    if not branch:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Branch not found")

    # Check access via repository -> project -> company
    validate_company_membership(db, user, branch.repository.project.company)

    return BranchResponse(
        id=branch.id,
        repository_id=branch.repository_id,
        name=branch.name,
        commit_sha=branch.commit_sha,
        created_at=branch.created_at.isoformat(),
    )


# ============================================================================
# 2FA Backup Codes Management
# ============================================================================


@app.post("/2fa/backup-codes/regenerate", response_model=RegenerateBackupResponse)
async def regenerate_backup_codes(
    data: RegenerateBackupRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Generate new backup codes (invalidates old ones).

    Requires:
    - Valid access token
    - 2FA must be enabled
    - Current TOTP code for verification

    Returns:
    - 10 new backup codes (one-time display, store securely)
    """
    import json

    # 1. Authenticate user
    payload = verify_access_token(credentials.credentials)

    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 2. Check 2FA is enabled
    if not user.totp_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="2FA is not enabled. Enable 2FA first."
        )

    if not user.totp_secret:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="2FA configuration error. Please re-setup 2FA.",
        )

    # 3. Verify TOTP code
    totp_service = get_totp_service()
    try:
        decrypted_secret = decrypt_totp_secret(user.totp_secret)
    except ValueError as e:
        logger.error(f"Failed to decrypt TOTP secret for user {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="2FA configuration error"
        )

    if not totp_service.verify_totp(decrypted_secret, data.code):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP code")

    # 4. Generate 10 new backup codes
    plain_codes, hashed_codes = totp_service.generate_backup_codes(count=10)

    # 5. Store hashed codes (replaces old ones)
    user.totp_backup_codes = hashed_codes  # Native list - SQLAlchemy handles ARRAY conversion
    db.commit()

    logger.info(f"Backup codes regenerated for user: {user.email}")

    # 6. Return plain codes (one-time display)
    return RegenerateBackupResponse(
        backup_codes=plain_codes,
        message="Backup codes regenerated. Store them securely - they will not be shown again.",
    )


# ============================================================================
# OAuth Endpoints
# ============================================================================


class OAuthUnlinkResponse(BaseModel):
    """Response after unlinking OAuth provider."""

    message: str
    provider: str


@app.delete("/oauth/{provider}/unlink", response_model=OAuthUnlinkResponse)
async def unlink_oauth(
    provider: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Unlink OAuth provider from user account.

    Security requirements:
    - User must be authenticated (Bearer token)
    - User must have a password set (can still login after unlinking)
    - User must have the specified provider linked

    This clears:
    - oauth_provider (set to None)
    - oauth_provider_id (set to None)
    - oauth_linked_at (set to None)
    - oauth_access_token (set to None)
    - oauth_refresh_token (set to None)
    - oauth_token_expires_at (set to None)

    Args:
        provider: OAuth provider to unlink (google, github)

    Returns:
        OAuthUnlinkResponse with success message

    Raises:
        401: Invalid token
        400: No password set, provider not linked, or invalid provider
        404: User not found
    """
    # 1. Verify token and get user
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 2. Validate provider
    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid OAuth provider: {provider}. Supported: {list(OAUTH_PROVIDERS.keys())}",
        )

    # 3. Check user has password set (can still login after unlinking)
    if not user.password_hash:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Set a password before unlinking OAuth. You need a way to login after unlinking.",
        )

    # 4. Check user has this provider linked
    if user.oauth_provider != provider:
        if user.oauth_provider is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No OAuth provider is linked to this account",
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Provider '{provider}' is not linked to this account. Current provider: {user.oauth_provider}",
        )

    # 5. Clear all OAuth fields
    user.oauth_provider = None
    user.oauth_provider_id = None
    user.oauth_linked_at = None
    user.oauth_access_token = None
    user.oauth_refresh_token = None
    user.oauth_token_expires_at = None
    db.commit()

    logger.info(f"OAuth provider '{provider}' unlinked for user: {user.email}")

    return OAuthUnlinkResponse(
        message=f"Successfully unlinked {provider} from your account", provider=provider
    )


@app.post("/oauth/{provider}/link", response_model=OAuthLinkResponse)
async def link_oauth(
    provider: str,
    data: OAuthLinkRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Link OAuth provider to existing authenticated account.

    This endpoint allows already authenticated users to link their account
    with an OAuth provider (Google, GitHub). The user must have completed
    the OAuth flow separately to obtain an authorization code.

    Flow:
    1. User is already logged in (has valid access token)
    2. User initiates OAuth flow in a separate window/tab
    3. User completes OAuth consent at provider
    4. Frontend receives authorization code from callback
    5. Frontend calls this endpoint with the code
    6. Backend exchanges code for tokens and fetches user info
    7. Backend verifies OAuth email matches current user's email
    8. Backend links OAuth to user account

    Security:
    - Requires valid Bearer token (user must be authenticated)
    - OAuth email MUST match current user's email (prevents hijacking)
    - Fails if user already has OAuth linked for this provider

    Args:
        provider: OAuth provider name (google, github)
        data: OAuthLinkRequest containing authorization code
        credentials: Bearer token from Authorization header
        db: Database session

    Returns:
        OAuthLinkResponse with success message and link timestamp

    Raises:
        HTTPException 400: Invalid provider or already linked
        HTTPException 401: Invalid token or OAuth code exchange failed
        HTTPException 403: OAuth email doesn't match user email
        HTTPException 503: Provider not configured
    """
    # 1. Authenticate current user
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired access token"
        )

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # 2. Validate provider
    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid OAuth provider: {provider}. Supported: {list(OAUTH_PROVIDERS.keys())}",
        )

    provider_config = OAUTH_PROVIDERS[provider]
    if not provider_config.is_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"OAuth provider '{provider}' is not configured",
        )

    # 3. Check if user already has OAuth linked for this provider
    if user.oauth_provider == provider:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"OAuth provider '{provider}' is already linked to this account",
        )

    # 4. Exchange code for tokens and fetch user info
    client = OAuthClient(provider)

    try:
        tokens = await client.exchange_code_for_tokens(data.code)
    except OAuthTokenError as e:
        logger.warning(f"OAuth token exchange failed for user {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Failed to exchange OAuth code for tokens. Code may be invalid or expired.",
        )

    try:
        oauth_user_info = await client.fetch_user_info(tokens.access_token)

        # Handle GitHub email fallback if needed
        if provider == "github" and not oauth_user_info.email:
            email = await client.fetch_github_email(tokens.access_token)
            if email:
                oauth_user_info = type(oauth_user_info)(
                    provider=oauth_user_info.provider,
                    provider_id=oauth_user_info.provider_id,
                    email=email,
                    name=oauth_user_info.name,
                    picture=oauth_user_info.picture,
                )
    except OAuthUserInfoError as e:
        logger.warning(f"Failed to fetch OAuth user info for user {user.email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Failed to fetch user information from OAuth provider",
        )

    # 5. Verify OAuth email matches current user's email
    if oauth_user_info.email.lower() != user.email.lower():
        logger.warning(
            f"OAuth email mismatch for user {user.email}: OAuth email is {oauth_user_info.email}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"OAuth email ({oauth_user_info.email}) does not match your account email ({user.email})",
        )

    # 6. Check if this OAuth identity is already linked to another account
    existing_user_with_oauth = (
        db.query(User)
        .filter(
            User.oauth_provider == provider,
            User.oauth_provider_id == oauth_user_info.provider_id,
            User.id != user.id,  # Exclude current user
        )
        .first()
    )

    if existing_user_with_oauth:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"This {provider} account is already linked to another user",
        )

    # 7. Link OAuth to user account
    now = datetime.now(timezone.utc)
    user.oauth_provider = provider
    user.oauth_provider_id = oauth_user_info.provider_id
    user.oauth_linked_at = now

    # Store tokens if available (for future API calls)
    # Note: Consider encrypting with Fernet for production
    if tokens.access_token:
        user.oauth_access_token = tokens.access_token
    if tokens.refresh_token:
        user.oauth_refresh_token = tokens.refresh_token
    if tokens.expires_in:
        user.oauth_token_expires_at = now + timedelta(seconds=tokens.expires_in)

    # Mark email as verified since OAuth provider has verified it
    if not user.email_verified:
        user.email_verified = True

    db.commit()

    logger.info(f"OAuth provider '{provider}' linked to user: {user.email}")

    return OAuthLinkResponse(
        message=f"Successfully linked {provider} to your account",
        provider=provider,
        linked_at=now.isoformat(),
    )


@app.get("/oauth/{provider}")
async def oauth_redirect(provider: str):
    """
    Redirect to OAuth provider authorization page.

    Initiates the OAuth2 authorization code flow by:
    1. Validating the provider (google, github)
    2. Generating a secure state token (CSRF protection)
    3. Storing state in Redis with 10-minute TTL
    4. Building the authorization URL with required scopes
    5. Redirecting to the provider's authorization page

    Args:
        provider: OAuth provider name (google, github)

    Returns:
        RedirectResponse to the provider's authorization URL

    Raises:
        HTTPException 400: If provider is not supported
    """
    # 1. Validate provider
    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid OAuth provider: {provider}. Supported: {list(OAUTH_PROVIDERS.keys())}",
        )

    # 2. Check provider is configured
    provider_config = OAUTH_PROVIDERS[provider]
    if not provider_config.is_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"OAuth provider '{provider}' is not configured",
        )

    # 3. Create OAuth client and generate authorization URL
    # OAuthClient.get_authorization_url() handles:
    # - State generation with secrets.token_urlsafe(32)
    # - State storage in Redis with 10-minute TTL
    # - Building URL with scopes and redirect_uri
    client = OAuthClient(provider)
    authorization_url = client.get_authorization_url()

    logger.info(f"OAuth redirect initiated for provider: {provider}")

    # 4. Redirect to provider
    return RedirectResponse(url=authorization_url, status_code=status.HTTP_302_FOUND)


@app.get("/oauth/{provider}/callback", response_model=OAuthCallbackResponse)
async def oauth_callback(provider: str, code: str, state: str, db: Session = Depends(get_db)):
    """
    Handle OAuth callback with SECURE account linking.

    SECURITY CRITICAL: NO AUTO-LINK BY EMAIL
    This endpoint follows Ragen's security requirements - when OAuth returns
    an email that matches an existing user, we do NOT auto-link the accounts.
    Instead, we require explicit user confirmation via /oauth/{provider}/confirm-link
    with password verification.

    Decision tree:
    1. If user exists by oauth_provider_id: Login (return tokens)
    2. If user exists by email: Return {action: "confirm_link_required"}
       - User must explicitly confirm via /oauth/{provider}/confirm-link with password
    3. If new user: Create account (email_verified=true), return tokens

    Args:
        provider: OAuth provider name (google, github)
        code: Authorization code from OAuth provider
        state: State token for CSRF protection (single-use, validated and deleted from Redis)

    Returns:
        OAuthCallbackResponse with either:
        - JWT tokens (success case)
        - action: "confirm_link_required" (email matches existing account)

    Raises:
        HTTPException 400: Invalid provider or state
        HTTPException 401: OAuth flow failed
    """
    from auth.oauth import OAuthStateError

    # 1. Validate provider
    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid OAuth provider: {provider}"
        )

    # 2. Create OAuth client
    client = OAuthClient(provider)

    try:
        # 3. Validate state (CSRF protection - single-use, deletes from Redis)
        client.validate_state(state)
    except OAuthStateError as e:
        logger.warning(f"OAuth state validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OAuth state. Please try again.",
        )

    try:
        # 4. Exchange code for tokens
        tokens = await client.exchange_code_for_tokens(code)

        # 5. Fetch user info from provider
        user_info = await client.fetch_user_info(tokens.access_token)

        # 6. GitHub email fallback if needed
        if provider == "github" and not user_info.email:
            email = await client.fetch_github_email(tokens.access_token)
            if email:
                from auth.oauth import OAuthUserInfo

                user_info = OAuthUserInfo(
                    provider=user_info.provider,
                    provider_id=user_info.provider_id,
                    email=email,
                    name=user_info.name,
                    picture=user_info.picture,
                )

        if not user_info.email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not retrieve email from OAuth provider. Please ensure your email is public or grant email permissions.",
            )

    except OAuthTokenError as e:
        logger.error(f"OAuth token exchange failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth authentication failed. Please try again.",
        )
    except OAuthUserInfoError as e:
        logger.error(f"OAuth user info fetch failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Failed to retrieve user information from OAuth provider.",
        )

    # 7. Account linking decision tree (SECURITY CRITICAL)

    # Case 1: Check if user exists by OAuth provider ID (returning OAuth user)
    user_by_oauth = (
        db.query(User)
        .filter(User.oauth_provider == provider, User.oauth_provider_id == user_info.provider_id)
        .first()
    )

    if user_by_oauth:
        # Existing OAuth user - just login
        if not user_by_oauth.is_active:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is disabled")

        # Generate JWT tokens
        token_payload = create_token_payload(
            user_id=user_by_oauth.id,
            email=user_by_oauth.email,
            roles=[role.name for role in user_by_oauth.roles],
            companies=[company.id for company in user_by_oauth.companies],
            is_superuser=user_by_oauth.is_superuser,
        )

        access_token = create_access_token(token_payload)
        refresh_token = create_refresh_token({"sub": user_by_oauth.id})

        logger.info(
            f"OAuth login successful for existing user: {user_by_oauth.email} via {provider}"
        )

        return OAuthCallbackResponse(access_token=access_token, refresh_token=refresh_token)

    # Case 2: Check if user exists by email (SECURITY: NO AUTO-LINK!)
    user_by_email = db.query(User).filter(User.email == user_info.email).first()

    if user_by_email:
        # SECURITY CRITICAL: Do NOT auto-link by email!
        # This prevents account takeover where attacker creates OAuth account
        # with victim's email and gains access to their account.
        logger.info(
            f"OAuth callback - email match found, requiring confirmation: {user_info.email}"
        )

        return OAuthCallbackResponse(
            action="confirm_link_required",
            email=user_info.email,
            provider=provider,
            message=f"An account with this email already exists. Please confirm linking your {provider.title()} account by providing your password.",
        )

    # Case 3: New user - create account with OAuth
    new_user = User(
        id=str(uuid4()),
        email=user_info.email,
        username=user_info.name or user_info.email.split("@")[0],  # Fallback to email prefix
        password_hash=hash_password(str(uuid4())),  # Random password - OAuth-only user
        is_active=True,
        is_superuser=False,
        email_verified=True,  # OAuth providers verify email
        oauth_provider=provider,
        oauth_provider_id=user_info.provider_id,
        oauth_linked_at=datetime.now(timezone.utc),
    )

    # Assign default 'user' role
    user_role = db.query(Role).filter(Role.name == "user").first()
    if user_role:
        new_user.roles.append(user_role)

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # Generate JWT tokens
    token_payload = create_token_payload(
        user_id=new_user.id,
        email=new_user.email,
        roles=[role.name for role in new_user.roles],
        companies=[company.id for company in new_user.companies],
        is_superuser=new_user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    refresh_token = create_refresh_token({"sub": new_user.id})

    logger.info(f"New OAuth user created: {new_user.email} via {provider}")

    return OAuthCallbackResponse(access_token=access_token, refresh_token=refresh_token)


@app.post("/oauth/{provider}/confirm-link", response_model=TokenResponse)
async def oauth_confirm_link(
    provider: str, data: OAuthConfirmLinkRequest, db: Session = Depends(get_db)
):
    """
    Confirm linking OAuth account to existing user with password verification.

    SECURITY: This endpoint requires password verification before linking OAuth
    to an existing account. This prevents account takeover attacks where an
    attacker creates an OAuth account with a victim's email.

    Flow:
    1. User attempts OAuth login, gets "confirm_link_required" response
    2. Frontend presents password prompt with OAuth code
    3. User submits password + oauth_code to this endpoint
    4. We re-verify with OAuth provider, then link if password is correct

    Args:
        provider: OAuth provider name (google, github)
        data: OAuthConfirmLinkRequest with email, password, and oauth_code

    Returns:
        TokenResponse with JWT tokens after successful linking

    Raises:
        HTTPException 400: Invalid provider
        HTTPException 401: Invalid password or OAuth verification failed
        HTTPException 404: User not found
    """
    # 1. Validate provider
    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid OAuth provider: {provider}"
        )

    # 2. Find user by email
    user = db.query(User).filter(User.email == data.email).first()

    if not user:
        # Return generic error to prevent enumeration
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # 3. Verify password (CRITICAL SECURITY CHECK)
    if not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # 4. Check if user is active
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is disabled")

    # 5. Re-verify with OAuth provider to get provider_id
    # This ensures the oauth_code is valid and wasn't tampered with
    client = OAuthClient(provider)

    try:
        tokens = await client.exchange_code_for_tokens(data.oauth_code)
        user_info = await client.fetch_user_info(tokens.access_token)

        # GitHub email fallback if needed
        if provider == "github" and not user_info.email:
            email = await client.fetch_github_email(tokens.access_token)
            if email:
                from auth.oauth import OAuthUserInfo

                user_info = OAuthUserInfo(
                    provider=user_info.provider,
                    provider_id=user_info.provider_id,
                    email=email,
                    name=user_info.name,
                    picture=user_info.picture,
                )

    except (OAuthTokenError, OAuthUserInfoError) as e:
        logger.error(f"OAuth verification failed during confirm-link: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth verification failed. Please restart the OAuth flow.",
        )

    # 6. Verify email matches
    if user_info.email != user.email:
        logger.warning(f"OAuth confirm-link email mismatch: {user_info.email} vs {user.email}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth email does not match account email",
        )

    # 7. Check if OAuth is already linked to another provider
    if user.oauth_provider and user.oauth_provider != provider:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Account is already linked to {user.oauth_provider}. Unlink first to use {provider}.",
        )

    # 8. Link OAuth account
    user.oauth_provider = provider
    user.oauth_provider_id = user_info.provider_id
    user.oauth_linked_at = datetime.now(timezone.utc)
    user.email_verified = True  # OAuth providers verify email

    db.commit()

    # 9. Generate JWT tokens
    token_payload = create_token_payload(
        user_id=user.id,
        email=user.email,
        roles=[role.name for role in user.roles],
        companies=[company.id for company in user.companies],
        is_superuser=user.is_superuser,
    )

    access_token = create_access_token(token_payload)
    refresh_token = create_refresh_token({"sub": user.id})

    logger.info(f"OAuth account linked after password confirmation: {user.email} to {provider}")

    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


# ============================================================================
# Run Application
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
