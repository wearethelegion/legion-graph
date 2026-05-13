"""
Brain v2 Pydantic models.

All request/response schemas for the v2 REST API layer.
Postgres column names from migration 060.
"""

from api.models.v2.common import (
    PaginatedResponse,
    PaginationMeta,
    PaginationParams,
)
from api.models.v2.knowledge import (
    KnowledgeChunkResponse,
    KnowledgeCreateRequest,
    KnowledgeResponse,
    KnowledgeUpdateRequest,
)
from api.models.v2.expertise import (
    ExpertiseChunkCreateRequest,
    ExpertiseChunkResponse,
    ExpertiseCreateRequest,
    ExpertiseResponse,
    ExpertiseUpdateRequest,
)
from api.models.v2.lessons import (
    LessonCreateRequest,
    LessonResponse,
    LessonUpdateRequest,
)
from api.models.v2.search import (
    UnifiedSearchRequest,
    UnifiedSearchResponse,
    UnifiedSearchResultItem,
)

__all__ = [
    # Common
    "PaginatedResponse",
    "PaginationMeta",
    "PaginationParams",
    # Knowledge
    "KnowledgeCreateRequest",
    "KnowledgeUpdateRequest",
    "KnowledgeResponse",
    "KnowledgeChunkResponse",
    # Expertise
    "ExpertiseCreateRequest",
    "ExpertiseUpdateRequest",
    "ExpertiseChunkCreateRequest",
    "ExpertiseResponse",
    "ExpertiseChunkResponse",
    # Lessons
    "LessonCreateRequest",
    "LessonUpdateRequest",
    "LessonResponse",
    # Search
    "UnifiedSearchRequest",
    "UnifiedSearchResponse",
    "UnifiedSearchResultItem",
]
