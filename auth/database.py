"""
KGRAG Auth Service - Database Models and Setup
PostgreSQL + SQLAlchemy for user authentication and authorization.
"""

import json
import os
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Boolean,
    DateTime,
    Table,
    ForeignKey,
    Integer,
    text,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.types import TypeDecorator, Text as TextType


class StringArrayType(TypeDecorator):
    """
    Cross-database string array type.

    Uses PostgreSQL ARRAY(String) on PostgreSQL, JSON on SQLite.
    SQLite doesn't support ARRAY, so we serialize to JSON text.

    Usage: Column(StringArrayType(), nullable=True)
    """

    impl = TextType
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(ARRAY(String))
        else:
            # SQLite and others: use Text with JSON serialization
            return dialect.type_descriptor(TextType())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            # PostgreSQL handles list natively
            return value
        else:
            # SQLite: serialize to JSON
            return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            # PostgreSQL returns list directly
            return value
        else:
            # SQLite: deserialize from JSON
            return json.loads(value)


class JSONType(TypeDecorator):
    """
    Cross-database JSON type.

    Uses PostgreSQL JSONB on PostgreSQL, JSON-serialized Text on SQLite.
    SQLite doesn't support JSONB, so we serialize to JSON text.

    Usage: Column(JSONType(), nullable=True)
    """

    impl = TextType
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB)
        else:
            # SQLite and others: use Text with JSON serialization
            return dialect.type_descriptor(TextType())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            # PostgreSQL handles dict/list natively
            return value
        else:
            # SQLite: serialize to JSON
            return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            # PostgreSQL returns dict/list directly
            return value
        else:
            # SQLite: deserialize from JSON
            return json.loads(value)


# Database URL from environment
DATABASE_URL = os.getenv("POSTGRES_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "POSTGRES_URL environment variable not set. "
        "Please configure PostgreSQL connection in .env file."
    )

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ============================================================================
# Association Tables (Many-to-Many)
# ============================================================================

user_roles = Table(
    "user_roles",
    Base.metadata,
    Column("user_id", String, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
)

role_permissions = Table(
    "role_permissions",
    Base.metadata,
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
    Column(
        "permission_id", Integer, ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True
    ),
)

company_users = Table(
    "company_users",
    Base.metadata,
    Column("company_id", String, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("user_id", String, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("role", String, default="member"),  # 'owner', 'member'
    Column("joined_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
)


# ============================================================================
# Database Models
# ============================================================================


class User(Base):
    """User account with authentication credentials."""

    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    username = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)
    email_verified = Column(Boolean, default=False, nullable=False)
    verification_code = Column(String(128), nullable=True)  # Bcrypt hash
    verification_code_expires_at = Column(DateTime(timezone=True), nullable=True)
    verification_attempts = Column(Integer, default=0, nullable=False)
    last_verification_request_at = Column(DateTime(timezone=True), nullable=True)
    # TOTP 2FA fields
    totp_secret = Column(Text, nullable=True)  # Fernet-encrypted
    totp_enabled = Column(Boolean, default=False, nullable=False)
    totp_backup_codes = Column(
        StringArrayType(), nullable=True
    )  # Cross-database compatible (ARRAY on PG, JSON on SQLite)
    totp_enabled_at = Column(DateTime(timezone=True), nullable=True)
    # OAuth2 fields
    oauth_provider = Column(String(20), nullable=True)  # google, github
    oauth_provider_id = Column(String(255), nullable=True)  # Provider's user ID
    oauth_linked_at = Column(DateTime(timezone=True), nullable=True)
    oauth_access_token = Column(String, nullable=True)  # Fernet-encrypted
    oauth_refresh_token = Column(String, nullable=True)  # Fernet-encrypted
    oauth_token_expires_at = Column(DateTime(timezone=True), nullable=True)
    # Profile fields
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    display_name = Column(String(200), nullable=True)
    avatar_url = Column(Text, nullable=True)
    bio = Column(Text, nullable=True)
    phone_number = Column(String(20), nullable=True)
    timezone = Column(String(50), nullable=True)
    locale = Column(String(10), nullable=True)
    profile_completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    roles = relationship("Role", secondary=user_roles, back_populates="users")
    companies = relationship("Company", secondary=company_users, back_populates="users")
    api_tokens = relationship("ApiToken", back_populates="user", cascade="all, delete-orphan")

    def has_permission(self, resource: str, action: str) -> bool:
        """Check if user has specific permission."""
        if self.is_superuser:
            return True

        for role in self.roles:
            for perm in role.permissions:
                if perm.resource == resource and perm.action == action:
                    return True
        return False

    def has_role(self, role_name: str) -> bool:
        """Check if user has specific role."""
        return any(role.name == role_name for role in self.roles)


class Role(Base):
    """Role with associated permissions."""

    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    description = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationships
    users = relationship("User", secondary=user_roles, back_populates="roles")
    permissions = relationship("Permission", secondary=role_permissions, back_populates="roles")


class Permission(Base):
    """Permission for specific resource and action."""

    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True, index=True)
    resource = Column(String, nullable=False, index=True)  # e.g., "memories", "users"
    action = Column(String, nullable=False, index=True)  # e.g., "read", "write", "delete"
    description = Column(String)

    # Relationships
    roles = relationship("Role", secondary=role_permissions, back_populates="permissions")

    __table_args__ = (
        # Unique constraint on resource + action
        {"sqlite_autoincrement": True},
    )


class Company(Base):
    """Company (multi-tenant organization)."""

    __tablename__ = "companies"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    is_active = Column(Boolean, default=True, nullable=False)
    cognee_enabled = Column(Boolean, nullable=False, default=False)

    # Relationships
    users = relationship("User", secondary=company_users, back_populates="companies")
    projects = relationship("Project", back_populates="company", cascade="all, delete-orphan")


class Project(Base):
    """Project under a company."""

    __tablename__ = "projects"

    id = Column(String, primary_key=True, index=True)
    company_id = Column(
        String, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String, nullable=False, index=True)
    description = Column(String)
    # GitHub integration — written by api/repositories/project_repository.py.
    # github_token is the per-project Personal Access Token used by code_preprocessor
    # to clone private repos. github_webhook_secret is the HMAC-SHA256 secret used by
    # /webhooks/github/{company}/{project} to validate incoming pushes. webhook_url
    # is the path-only suffix shown to the user when configuring GitHub.
    github_token = Column(String, nullable=True)
    github_webhook_secret = Column(String, nullable=True)
    webhook_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    company = relationship("Company", back_populates="projects")
    repositories = relationship(
        "Repository", back_populates="project", cascade="all, delete-orphan"
    )


class Repository(Base):
    """Repository under a project."""

    __tablename__ = "repositories"

    id = Column(String, primary_key=True, index=True)
    project_id = Column(
        String, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String, nullable=False, index=True)
    url = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    project = relationship("Project", back_populates="repositories")
    branches = relationship("Branch", back_populates="repository", cascade="all, delete-orphan")


class Branch(Base):
    """Branch under a repository."""

    __tablename__ = "branches"

    id = Column(String, primary_key=True, index=True)
    repository_id = Column(
        String, ForeignKey("repositories.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String, nullable=False, index=True)
    commit_sha = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    repository = relationship("Repository", back_populates="branches")


class DeletionTask(Base):
    """
    Persistent deletion task tracking.

    Enables resumable, phased user account deletion with:
    - Session invalidation (Phase 0)
    - External storage cleanup - Qdrant/Neo4j (Phase 1)
    - PostgreSQL cascade delete (Phase 2)

    State machine: pending → session_invalidation → external_cleanup → postgres_deletion → completed|failed
    """

    __tablename__ = "deletion_tasks"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False)

    # State machine
    status = Column(String(20), nullable=False, default="pending")
    current_phase = Column(String(50), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Progress tracking (JSONB)
    progress = Column(JSONType(), nullable=True)
    external_cleanup_state = Column(JSONType(), nullable=True)

    # Error handling
    error_message = Column(Text, nullable=True)
    error_phase = Column(String(50), nullable=True)
    retry_count = Column(Integer, default=0)
    last_retry_at = Column(DateTime(timezone=True), nullable=True)

    # Audit data preservation (JSONB)
    owned_company_ids = Column(JSONType(), nullable=True)
    affected_member_ids = Column(JSONType(), nullable=True)
    deletion_summary = Column(JSONType(), nullable=True)


class ApiToken(Base):
    """
    User API token for programmatic access.

    Token format: lgn_{base64_random_32_bytes} (~47 chars)
    Storage: SHA-256 hash only (plaintext NEVER stored)

    Security:
    - Tokens deleted when user is deleted (CASCADE)
    - Soft revocation via revoked_at timestamp
    - Scope-based permissions (JSONB array)

    Note: "Revoke all tokens on password change" is deferred to v2.
    """

    __tablename__ = "api_tokens"

    id = Column(String(36), primary_key=True)
    user_id = Column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Token identification
    name = Column(String(100), nullable=False)
    token_prefix = Column(String(12), nullable=False)  # "lgn_xK7m" for fast lookup
    token_hash = Column(String(64), nullable=False, unique=True)  # SHA-256
    token_hint = Column(String(8), nullable=False)  # "...AbCd" for display

    # Permissions
    scopes = Column(JSONType(), nullable=False, default=list)

    # Lifecycle
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=True)  # NULL = never expires
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True)  # Soft delete

    # Audit
    created_from_ip = Column(String(45), nullable=True)
    last_used_ip = Column(String(45), nullable=True)

    # Relationships
    user = relationship("User", back_populates="api_tokens")


# ============================================================================
# Database Utilities
# ============================================================================


def get_db():
    """Dependency for FastAPI endpoints to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database with tables and default data."""
    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Create default roles and permissions
    db = SessionLocal()
    try:
        # Check if defaults already exist
        if db.query(Role).first():
            return

        # Create permissions
        perms = [
            Permission(resource="memories", action="read", description="Read memories"),
            Permission(resource="memories", action="write", description="Create/update memories"),
            Permission(resource="memories", action="delete", description="Delete memories"),
            Permission(resource="users", action="read", description="Read users"),
            Permission(resource="users", action="write", description="Create/update users"),
            Permission(resource="users", action="delete", description="Delete users"),
        ]
        db.add_all(perms)
        db.commit()

        # Create roles
        admin_role = Role(name="admin", description="Full system access")
        admin_role.permissions = perms  # All permissions

        user_role = Role(name="user", description="Standard user access")
        user_role.permissions = [
            p for p in perms if p.resource == "memories"
        ]  # Only memory permissions

        readonly_role = Role(name="readonly", description="Read-only access")
        readonly_role.permissions = [
            p for p in perms if p.action == "read"
        ]  # Only read permissions

        db.add_all([admin_role, user_role, readonly_role])
        db.commit()

        print("✓ Database initialized with default roles and permissions")

    finally:
        db.close()


def health_check() -> bool:
    """Check database connectivity."""
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        return True
    except Exception:
        return False
