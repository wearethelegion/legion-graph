"""
Agent Pydantic Models
Request/response schemas for agent and skill chunk operations.

Access Control Fields (added in migration 022):
- project_id: Scopes agent to a project (NULL = company-wide)
- user_id: Owner of the agent (NULL = system agent, admin-only modifiable)
- sealed: If TRUE, agent cannot be modified
- public: If TRUE, visible to others in same company/project
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


class AgentCreate(BaseModel):
    """Request to create a new agent."""
    name: str = Field(..., min_length=1, max_length=255, description="Agent name")
    personality: str = Field(..., min_length=1, description="Agent personality description")
    main_responsibilities: str = Field(..., min_length=1, description="Main responsibilities")
    system_prompt: str = Field(..., min_length=1, description="System prompt for agent")
    metadata: Optional[dict] = Field(default_factory=dict, description="Additional metadata")
    when_to_use: Optional[str] = Field(None, description="When to delegate to this agent")


class AgentUpdate(BaseModel):
    """Request to update an existing agent."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Agent name")
    personality: Optional[str] = Field(None, min_length=1, description="Agent personality description")
    main_responsibilities: Optional[str] = Field(None, min_length=1, description="Main responsibilities")
    system_prompt: Optional[str] = Field(None, min_length=1, description="System prompt for agent")
    metadata: Optional[dict] = Field(None, description="Additional metadata")
    when_to_use: Optional[str] = Field(None, description="When to delegate to this agent")


class AgentCreateRequest(BaseModel):
    """Request to create a new agent (JSON body matching gRPC createAgent).

    Note: user_id is NOT accepted in request - it's set from current_user.user_id
    to prevent ownership spoofing.
    """
    name: str = Field(..., min_length=1, max_length=255, description="Agent name")
    role: str = Field(..., min_length=1, max_length=100, description="Agent role (e.g., developer, researcher)")
    personality: str = Field(..., min_length=1, description="Agent personality description")
    main_responsibilities: str = Field(..., min_length=1, description="Main responsibilities")
    system_prompt: str = Field(..., min_length=1, description="System prompt for agent")
    capabilities: Optional[List[str]] = Field(default=None, description="List of agent capabilities")
    specialization: Optional[str] = Field(default=None, max_length=100, description="Agent specialization area")
    when_to_use: Optional[str] = Field(None, description="When to delegate to this agent")

    # Access control fields
    project_id: Optional[str] = Field(
        default=None,
        description="Scope agent to project. NULL = company-wide"
    )
    sealed: bool = Field(
        default=False,
        description="If true, agent cannot be modified after creation"
    )
    public: bool = Field(
        default=False,
        description="If true, visible to others in same company/project"
    )
    # Note: user_id is set from current_user.user_id, NOT from request body


class AgentUpdateRequest(BaseModel):
    """Request to update an existing agent (all fields optional).

    Note: sealed cannot be changed to true via update (use separate seal endpoint).
    Note: user_id cannot be changed (ownership transfer is a separate operation).
    """
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Agent name")
    role: Optional[str] = Field(None, min_length=1, max_length=100, description="Agent role")
    personality: Optional[str] = Field(None, min_length=1, description="Agent personality description")
    main_responsibilities: Optional[str] = Field(None, min_length=1, description="Main responsibilities")
    system_prompt: Optional[str] = Field(None, min_length=1, description="System prompt for agent")
    capabilities: Optional[List[str]] = Field(None, description="List of agent capabilities")
    specialization: Optional[str] = Field(None, max_length=100, description="Agent specialization area")
    when_to_use: Optional[str] = Field(None, description="When to delegate to this agent")

    # Updatable access control fields
    project_id: Optional[str] = Field(
        default=None,
        description="Change project scope. Set to empty string to make company-wide"
    )
    public: Optional[bool] = Field(
        default=None,
        description="Change visibility"
    )
    # Note: sealed cannot be changed via this endpoint
    # Note: user_id cannot be changed (ownership transfer is separate)


class AgentResponse(BaseModel):
    """Agent information response."""
    id: str
    company_id: str
    name: str
    personality: str
    main_responsibilities: str
    system_prompt: str
    metadata: dict
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    when_to_use: Optional[str] = Field(None, description="When to delegate to this agent")

    # Access control fields
    project_id: Optional[str] = Field(
        default=None,
        description="Project scope. NULL = company-wide"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="Owner. NULL = system agent"
    )
    sealed: bool = Field(
        default=False,
        description="If true, agent cannot be modified"
    )
    public: bool = Field(
        default=False,
        description="If true, visible to others"
    )

    # Optional aggregated stats
    chunk_count: Optional[int] = None
    file_count: Optional[int] = None

    # Learning skill auto-link status (for create)
    learning_skill_linked: Optional[bool] = None

    model_config = ConfigDict(from_attributes=True)


class AgentListResponse(BaseModel):
    """List of agents response."""
    agents: List[AgentResponse]
    total: int


class SkillChunkResponse(BaseModel):
    """Skill chunk information response."""
    id: str
    agent_id: str
    file_path: str
    file_type: Optional[str] = None
    chunk_index: int
    section_title: Optional[str] = None
    chunk_type: Optional[str] = None
    summary: Optional[str] = None
    content: str
    token_count: Optional[int] = None
    key_concepts: List[str] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    file_references: List[str] = Field(default_factory=list)
    qdrant_point_id: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class SkillChunkListResponse(BaseModel):
    """List of skill chunks response."""
    chunks: List[SkillChunkResponse]
    total: int


class AgentSearchQuery(BaseModel):
    """Request to search agent skills."""
    query: str = Field(..., min_length=1, description="Search query")
    limit: int = Field(default=10, ge=1, le=50, description="Maximum number of results")
    file_type_filter: Optional[str] = Field(None, description="Filter by file type")
    chunk_type_filter: Optional[str] = Field(None, description="Filter by chunk type")


class AgentSearchResult(BaseModel):
    """Single search result with relevance score."""
    agent_id: str
    agent_name: str
    chunk: SkillChunkResponse
    relevance_score: float = Field(..., ge=0.0, le=1.0, description="Relevance score (0-1)")


class AgentSearchResponse(BaseModel):
    """Search results response."""
    results: List[AgentSearchResult]
    total: int
    query: str
