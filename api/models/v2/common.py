"""
Brain v2 — Common Pydantic models.

Provides generic pagination wrapper and query-param models
used across all v2 endpoints.
"""

from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


# =============================================================================
# Pagination
# =============================================================================


class PaginationMeta(BaseModel):
    """Pagination metadata embedded in every list response."""

    total: int = Field(..., description="Total number of matching records")
    limit: int = Field(..., description="Page size used for this request")
    offset: int = Field(..., description="Zero-based offset into the result set")

    model_config = ConfigDict(from_attributes=True)


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated envelope.

    Usage::

        PaginatedResponse[KnowledgeResponse](
            status="success",
            data=[...],
            pagination=PaginationMeta(total=42, limit=20, offset=0),
        )
    """

    status: str = "success"
    data: List[T] = Field(default_factory=list, description="Page of results")
    pagination: PaginationMeta

    model_config = ConfigDict(from_attributes=True)


class PaginationParams(BaseModel):
    """Query parameters accepted by paginated list endpoints."""

    limit: int = Field(default=20, ge=1, le=100, description="Max items per page")
    offset: int = Field(default=0, ge=0, description="Number of items to skip")

    model_config = ConfigDict(from_attributes=True)
