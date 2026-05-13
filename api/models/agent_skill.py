"""
Agent Skill Pydantic Models
Request/response schemas for agent skill management operations.
Separate from agent CRUD - handles skill linking/unlinking only.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List


# =============================================================================
# Request Models
# =============================================================================

class LinkSkillRequest(BaseModel):
    """Request to link a skill (expertise) to an agent."""
    expertise_id: str = Field(..., description="Expertise UUID to link as a skill")


# =============================================================================
# Response Models
# =============================================================================

class SkillSectionResponse(BaseModel):
    """Individual skill section for navigation."""
    chunk_id: str = Field(..., description="Chunk UUID for content retrieval")
    title: str = Field(..., description="Section title")
    summary: str = Field(default="", description="Section summary")
    has_code: bool = Field(default=False, description="Contains code examples")
    level: int = Field(default=0, description="Hierarchy depth")
    position: int = Field(default=0, description="Order within parent")

    model_config = ConfigDict(from_attributes=True)


class SkillOverviewResponse(BaseModel):
    """Lightweight skill overview for LLM navigation."""
    expertise_id: str = Field(..., description="Expertise UUID")
    title: str = Field(..., description="Skill title")
    summary: str = Field(default="", description="Skill summary")
    sections_count: int = Field(default=0, description="Number of sections")

    model_config = ConfigDict(from_attributes=True)


class AgentSkillsResponse(BaseModel):
    """Response for GET /agents/{agent_id}/skills."""
    agent_id: str = Field(..., description="Agent UUID")
    agent_name: str = Field(default="", description="Agent name")
    skills_count: int = Field(default=0, description="Total skills linked")
    skills: List[SkillOverviewResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class LinkSkillResponse(BaseModel):
    """Response for POST /agents/{agent_id}/skills."""
    status: str = Field(..., description="Result status: linked, already_linked, error")
    agent_id: str = Field(..., description="Agent UUID")
    expertise_id: str = Field(..., description="Expertise UUID")
    expertise_title: str = Field(default="", description="Title of linked expertise")

    model_config = ConfigDict(from_attributes=True)


class UnlinkSkillResponse(BaseModel):
    """Response for DELETE /agents/{agent_id}/skills/{expertise_id}."""
    status: str = Field(..., description="Result status: unlinked, not_found, error")
    agent_id: str = Field(..., description="Agent UUID")
    expertise_id: str = Field(..., description="Expertise UUID")

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Agent Context Response (WhoAmI equivalent)
# =============================================================================

class CompanyInstructionsContext(BaseModel):
    """Company instructions in context response."""
    id: str = Field(default="", description="Instructions UUID")
    company_id: str = Field(default="", description="Company UUID")
    ground_rules: str = Field(default="", description="Ground rules")
    coding_standards: Optional[str] = Field(default=None, description="Coding standards")
    communication_style: Optional[str] = Field(default=None, description="Communication style")
    forbidden_actions: Optional[str] = Field(default=None, description="Forbidden actions")
    custom_instructions: Optional[str] = Field(default=None, description="Custom instructions")

    model_config = ConfigDict(from_attributes=True)


class ProjectInstructionsContext(BaseModel):
    """Project instructions in context response."""
    id: str = Field(default="", description="Instructions UUID")
    project_id: str = Field(default="", description="Project UUID")
    description: str = Field(default="", description="Project description")
    languages: List[str] = Field(default_factory=list, description="Programming languages")
    frameworks: List[str] = Field(default_factory=list, description="Frameworks used")
    tools: List[str] = Field(default_factory=list, description="Tools and infrastructure")
    architecture_notes: Optional[str] = Field(default=None, description="Architecture notes")
    conventions: Optional[str] = Field(default=None, description="Conventions")
    custom_instructions: Optional[str] = Field(default=None, description="Custom instructions")

    model_config = ConfigDict(from_attributes=True)


class AvailableAgentResponse(BaseModel):
    """Agent available for delegation."""
    agent_id: str = Field(..., description="Agent UUID")
    name: str = Field(..., description="Agent name")
    role: str = Field(default="specialist", description="Agent role")
    specialization: str = Field(default="", description="Agent specialization")
    description: str = Field(default="", description="Short description (truncated)")

    model_config = ConfigDict(from_attributes=True)


class AgentContextResponse(BaseModel):
    """
    Full agent context response (WhoAmI equivalent).
    Combines agent identity, skills, and hierarchical instructions.
    """
    status: str = Field(default="success", description="Response status")
    agent_id: str = Field(..., description="Agent UUID")
    name: str = Field(..., description="Agent name")
    role: str = Field(default="specialist", description="Agent role")
    personality: str = Field(default="", description="Agent personality")
    main_responsibilities: str = Field(default="", description="Main responsibilities")
    system_prompt: str = Field(default="", description="Agent's raw system prompt")
    combined_system_prompt: str = Field(default="", description="Hierarchical prompt (company + project + agent)")

    # Capabilities and skills
    capabilities: List[str] = Field(default_factory=list, description="Agent capabilities")
    skills_count: int = Field(default=0, description="Number of linked skills")
    skills_overview: List[SkillOverviewResponse] = Field(default_factory=list, description="Skill summaries")

    # Available agents for delegation
    available_agents_count: int = Field(default=0, description="Agents available for delegation")
    available_agents: List[AvailableAgentResponse] = Field(default_factory=list)

    # Instructions (optional, included if project_id provided)
    company_instructions: Optional[CompanyInstructionsContext] = Field(default=None)
    project_instructions: Optional[ProjectInstructionsContext] = Field(default=None)

    model_config = ConfigDict(from_attributes=True)
