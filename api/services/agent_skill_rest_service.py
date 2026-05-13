"""
Agent Skill REST Service
Thin wrapper around AgentSkillService for REST endpoints.
Handles authentication, authorization, and Pydantic conversion.
"""

import json
from typing import Optional
from fastapi import HTTPException, status
from loguru import logger

from api.auth import CurrentUser, validate_company_access
from api.services.agent_skill_service import AgentSkillService
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.repositories.agent_repository import AgentRepository
from api.repositories.instructions_repository import InstructionsRepository
from api.repositories.project_repository import ProjectRepository
from api.models.agent_skill import (
    AgentSkillsResponse,
    SkillOverviewResponse,
    LinkSkillResponse,
    UnlinkSkillResponse,
    AgentContextResponse,
    AvailableAgentResponse,
    CompanyInstructionsContext,
    ProjectInstructionsContext,
)
from api.models.agent import AgentResponse
import asyncpg


class AgentSkillRESTService:
    """
    REST service for agent skill operations.
    Wraps AgentSkillService with auth/company validation.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        neo4j_repo: Optional[Neo4jRepository] = None,
        qdrant_repo: Optional[QdrantRepository] = None,
    ):
        self._pool = pool
        self._neo4j_repo = neo4j_repo or Neo4jRepository()
        self._qdrant_repo = qdrant_repo or QdrantRepository()
        self._agent_skill_service = AgentSkillService(
            neo4j_repo=self._neo4j_repo, qdrant_repo=self._qdrant_repo
        )
        self._agent_repo = AgentRepository(pool)
        self._instructions_repo = InstructionsRepository(pool)
        self._project_repo = ProjectRepository(pool)

    async def _get_agent_company_id(self, agent_id: str) -> str:
        """Get company_id for an agent, raise 404 if not found."""
        agent = await self._agent_repo.get_agent(agent_id)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Agent not found: {agent_id}"
            )
        return agent["company_id"]

    async def get_agent(self, agent_id: str, current_user: CurrentUser) -> AgentResponse:
        """
        Get agent details by ID.

        Args:
            agent_id: Agent UUID
            current_user: Current authenticated user

        Returns:
            AgentResponse with agent details

        Raises:
            HTTPException: On auth errors or agent not found
        """
        # Get agent
        agent = await self._agent_repo.get_agent(agent_id)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Agent not found: {agent_id}"
            )

        # Validate company access
        company_id = agent["company_id"]
        validate_company_access(current_user, company_id)

        try:
            # Parse metadata
            metadata = agent.get("metadata", {})
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            return AgentResponse(
                id=str(agent["id"]),
                company_id=str(agent["company_id"]),
                name=agent["name"],
                personality=agent.get("personality", ""),
                main_responsibilities=agent.get("main_responsibilities", ""),
                system_prompt=agent.get("system_prompt", ""),
                metadata=metadata,
                created_at=agent["created_at"],
                updated_at=agent["updated_at"],
                created_by=str(agent.get("created_by")) if agent.get("created_by") else None,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get agent: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get agent: {str(e)}",
            )

    async def get_agent_skills(
        self, agent_id: str, current_user: CurrentUser
    ) -> AgentSkillsResponse:
        """
        Get agent's skills for LLM navigation.

        Args:
            agent_id: Agent UUID
            current_user: Current authenticated user

        Returns:
            AgentSkillsResponse with skill overview

        Raises:
            HTTPException: On auth errors or agent not found
        """
        # Get company_id and validate access
        company_id = await self._get_agent_company_id(agent_id)
        validate_company_access(current_user, company_id)

        try:
            result = await self._agent_skill_service.get_skills(agent_id)

            if "error" in result:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=result["error"])

            # Convert to Pydantic response
            skills = [
                SkillOverviewResponse(
                    expertise_id=skill.get("expertise_id", ""),
                    title=skill.get("title", ""),
                    summary=skill.get("summary", ""),
                    sections_count=len(skill.get("sections", [])),
                )
                for skill in result.get("skills", [])
            ]

            return AgentSkillsResponse(
                agent_id=result.get("agent_id", agent_id),
                agent_name=result.get("agent_name", ""),
                skills_count=result.get("skills_count", len(skills)),
                skills=skills,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get agent skills: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get agent skills: {str(e)}",
            )

    async def link_skill(
        self, agent_id: str, expertise_id: str, current_user: CurrentUser
    ) -> LinkSkillResponse:
        """
        Link an expertise document to an agent as a skill.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID to link
            current_user: Current authenticated user

        Returns:
            LinkSkillResponse with status

        Raises:
            HTTPException: On auth errors or not found
        """
        # Get company_id and validate access
        company_id = await self._get_agent_company_id(agent_id)
        validate_company_access(current_user, company_id)

        try:
            logger.info(f"Linking skill {expertise_id} to agent {agent_id} by {current_user.email}")

            result = await self._agent_skill_service.link_skill(
                agent_id=agent_id, expertise_id=expertise_id
            )

            return LinkSkillResponse(
                status=result.get("status", "linked"),
                agent_id=result.get("agent_id", agent_id),
                expertise_id=result.get("expertise_id", expertise_id),
                expertise_title=result.get("expertise_title", ""),
            )

        except ValueError as e:
            # Agent or expertise not found
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to link skill: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to link skill: {str(e)}",
            )

    async def unlink_skill(
        self, agent_id: str, expertise_id: str, current_user: CurrentUser
    ) -> UnlinkSkillResponse:
        """
        Remove a skill link from an agent.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID to unlink
            current_user: Current authenticated user

        Returns:
            UnlinkSkillResponse with status

        Raises:
            HTTPException: On auth errors
        """
        # Get company_id and validate access
        company_id = await self._get_agent_company_id(agent_id)
        validate_company_access(current_user, company_id)

        try:
            logger.info(
                f"Unlinking skill {expertise_id} from agent {agent_id} by {current_user.email}"
            )

            result = await self._agent_skill_service.unlink_skill(
                agent_id=agent_id, expertise_id=expertise_id
            )

            return UnlinkSkillResponse(
                status=result.get("status", "unlinked"),
                agent_id=result.get("agent_id", agent_id),
                expertise_id=result.get("expertise_id", expertise_id),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to unlink skill: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to unlink skill: {str(e)}",
            )

    async def get_agent_context(
        self, agent_id: str, current_user: CurrentUser, project_id: Optional[str] = None
    ) -> AgentContextResponse:
        """
        Get full agent context (WhoAmI equivalent).
        Combines agent identity, skills, and hierarchical instructions.

        Args:
            agent_id: Agent UUID
            current_user: Current authenticated user
            project_id: Optional project UUID for hierarchical instructions

        Returns:
            AgentContextResponse with full agent context

        Raises:
            HTTPException: On auth errors or agent not found
        """
        # Get agent first
        agent = await self._agent_repo.get_agent(agent_id)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Agent not found: {agent_id}"
            )

        company_id = agent["company_id"]
        validate_company_access(current_user, company_id)

        try:
            # Parse metadata
            metadata = agent.get("metadata", {})
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            capabilities = metadata.get("capabilities", [])
            role = metadata.get("role", "specialist")

            # Get skills overview
            skills_result = await self._agent_skill_service.get_skills(agent_id)
            skills = skills_result.get("skills", [])

            # Get available agents for delegation
            available_agents = await self._agent_repo.get_available_agents(
                company_id=company_id, exclude_agent_id=agent_id
            )

            # Build hierarchical system prompt
            prompt_sections = []
            company_instructions = None
            project_instructions = None

            # 1. Fetch company instructions
            company_instr_data = await self._instructions_repo.get_company_instructions(company_id)
            if company_instr_data:
                company_instructions = CompanyInstructionsContext(
                    id=company_instr_data.get("id", ""),
                    company_id=company_instr_data.get("company_id", ""),
                    ground_rules=company_instr_data.get("ground_rules") or "",
                    coding_standards=company_instr_data.get("coding_standards"),
                    communication_style=company_instr_data.get("communication_style"),
                    forbidden_actions=company_instr_data.get("forbidden_actions"),
                    custom_instructions=company_instr_data.get("custom_instructions"),
                )

                # Add to combined prompt
                company_parts = []
                if company_instr_data.get("ground_rules"):
                    company_parts.append(f"## Ground Rules\n{company_instr_data['ground_rules']}")
                if company_instr_data.get("coding_standards"):
                    company_parts.append(
                        f"## Coding Standards\n{company_instr_data['coding_standards']}"
                    )
                if company_instr_data.get("communication_style"):
                    company_parts.append(
                        f"## Communication Style\n{company_instr_data['communication_style']}"
                    )
                if company_instr_data.get("forbidden_actions"):
                    company_parts.append(
                        f"## Forbidden Actions\n{company_instr_data['forbidden_actions']}"
                    )
                if company_instr_data.get("custom_instructions"):
                    company_parts.append(
                        f"## Additional Company Guidelines\n{company_instr_data['custom_instructions']}"
                    )

                if company_parts:
                    prompt_sections.append(
                        "# Company Instructions\n\n" + "\n\n".join(company_parts)
                    )

            # 2. Fetch project instructions (if project_id provided)
            if project_id:
                project_instr_data = await self._instructions_repo.get_project_instructions(
                    project_id
                )
                if project_instr_data:
                    project_instructions = ProjectInstructionsContext(
                        id=project_instr_data.get("id", ""),
                        project_id=project_instr_data.get("project_id", ""),
                        description=project_instr_data.get("description") or "",
                        languages=project_instr_data.get("languages") or [],
                        frameworks=project_instr_data.get("frameworks") or [],
                        tools=project_instr_data.get("tools") or [],
                        architecture_notes=project_instr_data.get("architecture_notes"),
                        conventions=project_instr_data.get("conventions"),
                        custom_instructions=project_instr_data.get("custom_instructions"),
                    )

                    # Add to combined prompt
                    project_parts = []
                    if project_instr_data.get("description"):
                        project_parts.append(
                            f"## Project Description\n{project_instr_data['description']}"
                        )
                    if project_instr_data.get("languages"):
                        langs = project_instr_data["languages"]
                        if isinstance(langs, list):
                            project_parts.append(f"## Languages\n{', '.join(langs)}")
                    if project_instr_data.get("frameworks"):
                        frameworks = project_instr_data["frameworks"]
                        if isinstance(frameworks, list):
                            project_parts.append(f"## Frameworks\n{', '.join(frameworks)}")
                    if project_instr_data.get("tools"):
                        tools = project_instr_data["tools"]
                        if isinstance(tools, list):
                            project_parts.append(f"## Tools & Infrastructure\n{', '.join(tools)}")
                    if project_instr_data.get("architecture_notes"):
                        project_parts.append(
                            f"## Architecture Notes\n{project_instr_data['architecture_notes']}"
                        )
                    if project_instr_data.get("conventions"):
                        project_parts.append(
                            f"## Project Conventions\n{project_instr_data['conventions']}"
                        )
                    if project_instr_data.get("custom_instructions"):
                        project_parts.append(
                            f"## Additional Project Guidelines\n{project_instr_data['custom_instructions']}"
                        )

                    if project_parts:
                        prompt_sections.append(
                            "# Project Instructions\n\n" + "\n\n".join(project_parts)
                        )

            # 3. Agent system prompt
            agent_system_prompt = agent.get("system_prompt", "")
            if agent_system_prompt:
                prompt_sections.append(f"# Agent Instructions\n\n{agent_system_prompt}")

            # Combine all sections
            combined_system_prompt = "\n\n---\n\n".join(prompt_sections) if prompt_sections else ""

            # Build skills overview
            skills_overview = [
                SkillOverviewResponse(
                    expertise_id=skill.get("expertise_id", ""),
                    title=skill.get("title", ""),
                    summary=skill.get("summary", ""),
                    sections_count=len(skill.get("sections", [])),
                )
                for skill in skills
            ]

            # Build available agents list
            available_agents_list = []
            for avail_agent in available_agents:
                avail_metadata = avail_agent.get("metadata", {})
                if isinstance(avail_metadata, str):
                    avail_metadata = json.loads(avail_metadata)

                available_agents_list.append(
                    AvailableAgentResponse(
                        agent_id=avail_agent["id"],
                        name=avail_agent["name"],
                        role=avail_metadata.get("role", "specialist"),
                        specialization=avail_metadata.get("specialization", ""),
                        description=avail_agent.get("main_responsibilities", "")[:200],
                    )
                )

            logger.info(
                f"Agent context retrieved for {agent['name']} ({role}): "
                f"{len(skills)} skills, {len(available_agents)} available agents"
            )

            return AgentContextResponse(
                status="success",
                agent_id=agent["id"],
                name=agent["name"],
                role=role,
                personality=agent.get("personality", ""),
                main_responsibilities=agent.get("main_responsibilities", ""),
                system_prompt=agent_system_prompt,
                combined_system_prompt=combined_system_prompt,
                capabilities=capabilities,
                skills_count=len(skills),
                skills_overview=skills_overview,
                available_agents_count=len(available_agents),
                available_agents=available_agents_list,
                company_instructions=company_instructions,
                project_instructions=project_instructions,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get agent context: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get agent context: {str(e)}",
            )
