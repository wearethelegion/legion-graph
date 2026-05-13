"""
KGRAG Knowledge Domain — SQLAlchemy Models
PostgreSQL table definitions for Knowledge, Expertise, and Lessons Learned.

These models mirror the tables created in migration 060.
Runtime operations use asyncpg directly; these models exist for:
  - Alembic metadata registration (autogenerate support)
  - Type-safe column reference in future ORM queries
  - Schema documentation

Tables:
  - knowledge              Core knowledge documents
  - knowledge_chunks       Hierarchical chunks of knowledge
  - expertise              Structured expertise documents
  - expertise_chunks       Hierarchical chunks of expertise
  - lessons_learned        Resolved issues with structured fields
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    String,
    Text,
    Boolean,
    Integer,
    DateTime,
    ForeignKey,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from auth.database import Base


# ============================================================================
# Knowledge
# ============================================================================


class Knowledge(Base):
    """Knowledge document with text content and metadata."""

    __tablename__ = "knowledge"

    id = Column(PG_UUID, primary_key=True, server_default=func.gen_random_uuid())

    company_id = Column(String(36), nullable=False, index=True)
    project_id = Column(String(36), nullable=False)

    title = Column(Text, nullable=False)
    text_content = Column(Text, nullable=False)
    when_to_use = Column(Text, nullable=True)
    content_hash = Column(String(64), nullable=True)

    metadata_ = Column("metadata", JSONB, nullable=False, server_default="{}")

    created_by_user_id = Column(String(36), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    chunks = relationship(
        "KnowledgeChunk",
        back_populates="knowledge",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class KnowledgeChunk(Base):
    """Hierarchical chunk within a knowledge document."""

    __tablename__ = "knowledge_chunks"

    id = Column(PG_UUID, primary_key=True, server_default=func.gen_random_uuid())
    knowledge_id = Column(
        PG_UUID,
        ForeignKey("knowledge.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    content = Column(Text, nullable=False)
    summary = Column(Text, nullable=True)

    position = Column(Integer, nullable=False, server_default="0")
    level = Column(Integer, nullable=False, server_default="0")
    parent_chunk_id = Column(
        PG_UUID,
        ForeignKey("knowledge_chunks.id", ondelete="SET NULL"),
        nullable=True,
    )

    chunk_type = Column(String(20), nullable=True)
    section_title = Column(Text, nullable=True)
    has_code = Column(Boolean, nullable=False, server_default="false")
    keywords = Column(JSONB, nullable=False, server_default="[]")

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    knowledge = relationship("Knowledge", back_populates="chunks")

    __table_args__ = (
        CheckConstraint(
            "chunk_type IN ('prose', 'code', 'heading', 'mixed')",
            name="ck_knowledge_chunks_chunk_type",
        ),
    )


# ============================================================================
# Expertise
# ============================================================================


class Expertise(Base):
    """Structured expertise document with sections."""

    __tablename__ = "expertise"

    id = Column(PG_UUID, primary_key=True, server_default=func.gen_random_uuid())

    company_id = Column(String(36), nullable=False, index=True)
    project_id = Column(String(36), nullable=True)

    title = Column(Text, nullable=False)
    content = Column(Text, nullable=False)
    summary = Column(Text, nullable=True)
    when_to_use = Column(Text, nullable=True)
    is_company_level = Column(Boolean, nullable=False, server_default="false")
    content_hash = Column(String(64), nullable=True)

    metadata_ = Column("metadata", JSONB, nullable=False, server_default="{}")

    created_by_user_id = Column(String(36), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    chunks = relationship(
        "ExpertiseChunk",
        back_populates="expertise",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ExpertiseChunk(Base):
    """Hierarchical chunk within an expertise document."""

    __tablename__ = "expertise_chunks"

    id = Column(PG_UUID, primary_key=True, server_default=func.gen_random_uuid())
    expertise_id = Column(
        PG_UUID,
        ForeignKey("expertise.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    content = Column(Text, nullable=False)
    summary = Column(Text, nullable=True)

    position = Column(Integer, nullable=False, server_default="0")
    level = Column(Integer, nullable=False, server_default="0")
    parent_chunk_id = Column(
        PG_UUID,
        ForeignKey("expertise_chunks.id", ondelete="SET NULL"),
        nullable=True,
    )
    chunk_path = Column(String(100), nullable=True)

    chunk_type = Column(String(20), nullable=True)
    section_title = Column(Text, nullable=True)
    has_code = Column(Boolean, nullable=False, server_default="false")
    keywords = Column(JSONB, nullable=False, server_default="[]")

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    expertise = relationship("Expertise", back_populates="chunks")

    __table_args__ = (
        CheckConstraint(
            "chunk_type IN ('prose', 'code', 'heading', 'mixed')",
            name="ck_expertise_chunks_chunk_type",
        ),
    )


# ============================================================================
# Lessons Learned
# ============================================================================


class LessonLearned(Base):
    """Resolved issue with structured symptom/root-cause/solution fields."""

    __tablename__ = "lessons_learned"

    id = Column(PG_UUID, primary_key=True, server_default=func.gen_random_uuid())

    company_id = Column(String(36), nullable=False, index=True)
    project_id = Column(String(36), nullable=False)

    title = Column(Text, nullable=False)
    category = Column(String(200), nullable=False)

    symptom = Column(Text, nullable=False)
    root_cause = Column(Text, nullable=False)
    solution = Column(Text, nullable=False)
    prevention = Column(Text, nullable=False)

    severity = Column(String(20), nullable=False, server_default="medium")

    tags = Column(JSONB, nullable=False, server_default="[]")
    files_changed = Column(JSONB, nullable=False, server_default="[]")

    content = Column(Text, nullable=True)
    content_hash = Column(String(64), nullable=True)
    metadata_ = Column("metadata", JSONB, nullable=False, server_default="{}")

    created_by_user_id = Column(String(36), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')",
            name="ck_lessons_learned_severity",
        ),
    )
