"""
Instructions Pydantic Models
Request/response schemas for company and project instructions.
These serve as database-backed CLAUDE.md equivalents.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


# =============================================================================
# Company Instructions
# =============================================================================

class CompanyInstructionsCreate(BaseModel):
    """Request to create/update company instructions."""
    ground_rules: Optional[str] = Field(None, description="General principles and values")
    coding_standards: Optional[str] = Field(None, description="Code style: SOLID, DRY, KISS, etc.")
    communication_style: Optional[str] = Field(None, description="How agents should communicate")
    forbidden_actions: Optional[str] = Field(None, description="Things agents should never do")
    custom_instructions: Optional[str] = Field(None, description="Any other company-wide instructions")


class CompanyInstructionsResponse(BaseModel):
    """Company instructions response."""
    id: str
    company_id: str
    ground_rules: Optional[str] = None
    coding_standards: Optional[str] = None
    communication_style: Optional[str] = None
    forbidden_actions: Optional[str] = None
    custom_instructions: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Project Instructions
# =============================================================================

class ProjectInstructionsCreate(BaseModel):
    """Request to create/update project instructions."""
    description: Optional[str] = Field(None, description="What the project is")
    languages: Optional[list[str]] = Field(None, description="Programming languages used")
    frameworks: Optional[list[str]] = Field(None, description="Frameworks used")
    tools: Optional[list[str]] = Field(None, description="Tools and infrastructure")
    architecture_notes: Optional[str] = Field(None, description="Key architectural decisions")
    conventions: Optional[str] = Field(None, description="Project-specific patterns and conventions")
    custom_instructions: Optional[str] = Field(None, description="Any other project-specific instructions")


class ProjectInstructionsResponse(BaseModel):
    """Project instructions response."""
    id: str
    project_id: str
    description: Optional[str] = None
    languages: Optional[list[str]] = None
    frameworks: Optional[list[str]] = None
    tools: Optional[list[str]] = None
    architecture_notes: Optional[str] = None
    conventions: Optional[str] = None
    custom_instructions: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Combined Context (for agent delegation)
# =============================================================================

class ContextInstructionsResponse(BaseModel):
    """Combined company + project instructions for agent context."""
    company: Optional[CompanyInstructionsResponse] = None
    project: Optional[ProjectInstructionsResponse] = None

    def to_system_prompt_section(self) -> str:
        """Format instructions as a system prompt section."""
        sections = []

        if self.company:
            company_parts = []
            if self.company.ground_rules:
                company_parts.append(f"## Ground Rules\n{self.company.ground_rules}")
            if self.company.coding_standards:
                company_parts.append(f"## Coding Standards\n{self.company.coding_standards}")
            if self.company.communication_style:
                company_parts.append(f"## Communication Style\n{self.company.communication_style}")
            if self.company.forbidden_actions:
                company_parts.append(f"## Forbidden Actions\n{self.company.forbidden_actions}")
            if self.company.custom_instructions:
                company_parts.append(f"## Additional Company Guidelines\n{self.company.custom_instructions}")

            if company_parts:
                sections.append("# Company Instructions\n\n" + "\n\n".join(company_parts))

        if self.project:
            project_parts = []
            if self.project.description:
                project_parts.append(f"## Project Description\n{self.project.description}")
            if self.project.languages:
                project_parts.append(f"## Languages\n{', '.join(self.project.languages)}")
            if self.project.frameworks:
                project_parts.append(f"## Frameworks\n{', '.join(self.project.frameworks)}")
            if self.project.tools:
                project_parts.append(f"## Tools & Infrastructure\n{', '.join(self.project.tools)}")
            if self.project.architecture_notes:
                project_parts.append(f"## Architecture Notes\n{self.project.architecture_notes}")
            if self.project.conventions:
                project_parts.append(f"## Project Conventions\n{self.project.conventions}")
            if self.project.custom_instructions:
                project_parts.append(f"## Additional Project Guidelines\n{self.project.custom_instructions}")

            if project_parts:
                sections.append("# Project Instructions\n\n" + "\n\n".join(project_parts))

        return "\n\n---\n\n".join(sections) if sections else ""
