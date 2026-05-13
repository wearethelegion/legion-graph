"""
Company Statistics Pydantic Models
Simple flat response schema for company statistics endpoint.
"""

from pydantic import BaseModel, Field


class CompanyStatsResponse(BaseModel):
    """Response for company-level statistics (simple flat record counts)."""
    company_id: str = Field(..., description="Company UUID")
    # PostgreSQL tables
    projects_count: int = Field(default=0, description="Number of projects")
    agents_count: int = Field(default=0, description="Number of agents")
    engagements_count: int = Field(default=0, description="Number of engagements")
    tasks_count: int = Field(default=0, description="Number of tasks")
    delegations_count: int = Field(default=0, description="Number of delegations")
    # Neo4j nodes
    knowledge_count: int = Field(default=0, description="Number of Knowledge nodes (from Neo4j)")
    code_count: int = Field(default=0, description="Number of Code/File nodes (from Neo4j)")
    expertise_count: int = Field(default=0, description="Number of Expertise nodes (from Neo4j)")
