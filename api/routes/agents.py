"""
KGRAG API - Agent Routes
RESTful endpoints for agent management.

Access Control (added in migration 022):
- GET /agents: Returns only visible agents (owned OR public)
- GET /agents/{id}: Returns 404 if not visible (security: hide existence)
- PUT /agents/{id}: Returns 404 if not visible, 403 if sealed/not owner
- DELETE /agents/{id}: Returns 404 if not visible, 403 if sealed/not owner
- POST /agents: Creates agent with current user as owner
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from uuid import UUID
from typing import List, Optional
from loguru import logger

from api.services.agent_service import AgentService
from api.models.agent import (
    AgentResponse,
    AgentSearchResult,
    SkillChunkResponse,
    AgentCreateRequest,
    AgentUpdateRequest,
)
from api.auth import CurrentUser, get_current_user, validate_company_access
from api.database import get_db_pool
from api.repositories.agent_repository import AgentRepository
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.repositories.project_repository import ProjectRepository

router = APIRouter(prefix="/api/v1/companies/{company_id}/agents", tags=["agents"])


async def get_agent_service() -> AgentService:
    """Dependency to provide AgentService instance with project repo for access control."""
    pool = await get_db_pool()
    agent_repo = AgentRepository(pool)
    neo4j_repo = Neo4jRepository()
    qdrant_repo = QdrantRepository()
    project_repo = ProjectRepository(pool)

    return AgentService(agent_repo, neo4j_repo, qdrant_repo, project_repo)


@router.post("", response_model=AgentResponse, status_code=status.HTTP_201_CREATED)
async def create_agent(
    company_id: str,
    data: AgentCreateRequest,
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    Create a new agent with JSON body.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body**:
    - name: Agent name (required)
    - role: Agent role (required, e.g., developer, researcher, orchestrator)
    - personality: Agent personality description (required)
    - main_responsibilities: Agent main responsibilities (required)
    - system_prompt: System prompt for agent (required)
    - capabilities: List of agent capabilities (optional)
    - specialization: Agent specialization area (optional)
    - project_id: Scope to project (optional, NULL = company-wide)
    - sealed: Lock agent from modification (optional, default: false)
    - public: Make visible to others (optional, default: false)

    **Response**: 201 Created with AgentResponse

    **Access Control**:
    - user_id is automatically set to current user (cannot be spoofed)
    - Owner can modify their agents unless sealed

    **Error Handling**:
    - 400: Duplicate agent name or invalid input
    - 403: Access denied to company
    - 500: Processing or sync failure (full rollback)
    """
    logger.info(f"Creating agent '{data.name}' with role '{data.role}' for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        # Convert company_id to UUID
        company_uuid = UUID(company_id)

        # Create agent through service with owner
        agent = await agent_service.create_agent_with_owner(
            company_id=company_uuid,
            name=data.name,
            role=data.role,
            personality=data.personality,
            main_responsibilities=data.main_responsibilities,
            system_prompt=data.system_prompt,
            current_user=current_user,
            capabilities=data.capabilities,
            specialization=data.specialization,
            project_id=data.project_id,
            sealed=data.sealed,
            public=data.public,
            when_to_use=data.when_to_use,
        )

        logger.info(f"Successfully created agent {agent['id']} ({data.name}) owned by {current_user.user_id}")

        return AgentResponse(
            id=str(agent["id"]),
            company_id=str(agent["company_id"]),
            name=agent["name"],
            personality=agent["personality"],
            main_responsibilities=agent["main_responsibilities"],
            system_prompt=agent["system_prompt"],
            metadata=agent.get("metadata", {}),
            created_at=agent["created_at"],
            updated_at=agent["updated_at"],
            created_by=str(agent.get("created_by")) if agent.get("created_by") else None,
            when_to_use=agent.get("when_to_use"),
            project_id=agent.get("project_id"),
            user_id=agent.get("user_id"),
            sealed=agent.get("sealed", False),
            public=agent.get("public", False),
            chunk_count=None,
            file_count=None,
            learning_skill_linked=agent.get("learning_skill_linked")
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for agent creation: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to create agent: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create agent: {str(e)}"
        )


@router.put("/{agent_id}", response_model=AgentResponse)
async def update_agent(
    company_id: str,
    agent_id: str,
    data: AgentUpdateRequest,
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    Update an existing agent.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body** (all fields optional):
    - name: New agent name
    - role: New agent role
    - personality: New personality description
    - main_responsibilities: New main responsibilities
    - system_prompt: New system prompt
    - capabilities: New list of capabilities
    - specialization: New specialization area
    - project_id: New project scope (empty string to make company-wide)
    - public: Change visibility

    **Access Control**:
    - Returns 404 if agent not found OR not visible (security: hide existence)
    - Returns 403 if agent is sealed
    - Returns 403 if user is not owner (non-admin) or system agent (non-admin)

    **Note**: sealed and user_id cannot be changed via this endpoint.

    **Response**: 200 OK with updated AgentResponse
    """
    logger.info(f"Updating agent {agent_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        # Detect if project_id was explicitly provided in request
        # This distinguishes "not sent" (None, keep current) from "sent as null" (clear to NULL)
        project_id_provided = "project_id" in data.model_fields_set
        when_to_use_provided = "when_to_use" in data.model_fields_set

        # Update agent with access check
        agent = await agent_service.update_agent_with_access_check(
            agent_id=agent_id,
            current_user=current_user,
            company_id=company_id,
            name=data.name,
            role=data.role,
            personality=data.personality,
            main_responsibilities=data.main_responsibilities,
            system_prompt=data.system_prompt,
            capabilities=data.capabilities,
            specialization=data.specialization,
            project_id=data.project_id,
            public=data.public,
            project_id_provided=project_id_provided,
            when_to_use=data.when_to_use,
            when_to_use_provided=when_to_use_provided
        )

        logger.info(f"Successfully updated agent {agent_id}")

        return AgentResponse(
            id=str(agent["id"]),
            company_id=str(agent["company_id"]),
            name=agent["name"],
            personality=agent["personality"],
            main_responsibilities=agent["main_responsibilities"],
            system_prompt=agent["system_prompt"],
            metadata=agent.get("metadata", {}),
            created_at=agent["created_at"],
            updated_at=agent["updated_at"],
            created_by=str(agent.get("created_by")) if agent.get("created_by") else None,
            when_to_use=agent.get("when_to_use"),
            project_id=agent.get("project_id"),
            user_id=agent.get("user_id"),
            sealed=agent.get("sealed", False),
            public=agent.get("public", False),
            chunk_count=None,
            file_count=None
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for agent update: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to update agent: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update agent: {str(e)}"
        )


@router.get("", response_model=dict)
async def list_agents(
    company_id: str,
    limit: int = Query(50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    List all agents for company with pagination.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Query Parameters**:
    - limit: Maximum number of results (default: 50, max: 100)
    - offset: Number of results to skip (default: 0)

    **Response**: 200 OK with {"agents": [AgentResponse], "total": int}

    **Error Handling**:
    - 403: Access denied to company
    - 500: Database query failure
    """
    logger.info(f"Listing agents for company {company_id} (limit={limit}, offset={offset})")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        company_uuid = UUID(company_id)

        # Get visible agents (owned by user OR public)
        result = await agent_service.list_agents_visible(
            company_id=str(company_uuid),
            current_user=current_user,
            limit=limit,
            offset=offset
        )

        # Convert to response format
        agent_responses = [
            AgentResponse(
                id=str(agent["id"]),
                company_id=str(agent["company_id"]),
                name=agent["name"],
                personality=agent["personality"],
                main_responsibilities=agent["main_responsibilities"],
                system_prompt=agent["system_prompt"],
                metadata=agent.get("metadata", {}),
                created_at=agent["created_at"],
                updated_at=agent["updated_at"],
                created_by=str(agent.get("created_by")) if agent.get("created_by") else None,
                when_to_use=agent.get("when_to_use"),
                project_id=agent.get("project_id"),
                user_id=agent.get("user_id"),
                sealed=agent.get("sealed", False),
                public=agent.get("public", False),
                chunk_count=agent.get("chunk_count"),
                file_count=agent.get("file_count")
            )
            for agent in result["agents"]
        ]

        logger.info(f"Found {len(agent_responses)} visible agents for company {company_id}")

        return {
            "agents": agent_responses,
            "total": result["total_count"]
        }

    except ValueError as e:
        logger.error("Invalid company_id format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid company_id format"
        )
    except Exception as e:
        logger.error("Failed to list agents: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list agents: {str(e)}"
        )


@router.get("/search", response_model=List[AgentSearchResult])
async def search_agents(
    company_id: str,
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of results"),
    file_type_filter: Optional[str] = Query(None, description="Filter by file type"),
    chunk_type_filter: Optional[str] = Query(None, description="Filter by chunk type"),
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    Hybrid search agents by query (vector + graph).

    **Authentication Required**: User must be authenticated and have access to the company.

    **Query Parameters**:
    - query: Search text (required)
    - limit: Maximum number of results (default: 10, max: 50)
    - file_type_filter: Optional file type filter (e.g., "python", "javascript")
    - chunk_type_filter: Optional chunk type filter (e.g., "function", "class")

    **Response**: 200 OK with [AgentSearchResult]

    **Search Process**:
    1. Embed query with same embedder used for indexing
    2. Search Qdrant vector store for relevant chunks
    3. Apply optional filters (file_type, chunk_type)
    4. Retrieve agent metadata from PostgreSQL
    5. Return results with relevance scores

    **Error Handling**:
    - 400: Invalid query or parameters
    - 403: Access denied to company
    - 500: Search failure
    """
    logger.info(f"Searching agents for company {company_id} with query: '{query}'")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        company_uuid = UUID(company_id)

        # Search agents
        results = await agent_service.search_agents(
            company_id=company_uuid,
            query=query,
            limit=limit,
            file_type_filter=file_type_filter,
            chunk_type_filter=chunk_type_filter
        )

        # Convert to response format
        search_results = []
        for result in results:
            chunk_response = SkillChunkResponse(
                id=result["chunk_id"],
                agent_id=result["agent_id"],
                file_path=result["file_path"],
                file_type=result.get("file_type"),
                chunk_index=0,  # Not included in search result
                section_title=result.get("section_title"),
                chunk_type=result.get("chunk_type"),
                summary=result.get("summary"),
                content="",  # Not included in search result for performance
                key_concepts=result.get("key_concepts", []),
                dependencies=[],
                file_references=[],
                created_at=None  # Not included in search result
            )

            search_results.append(
                AgentSearchResult(
                    agent_id=result["agent_id"],
                    agent_name=result["agent_name"],
                    chunk=chunk_response,
                    relevance_score=result["relevance_score"]
                )
            )

        logger.info(f"Found {len(search_results)} results for query '{query}'")

        return search_results

    except ValueError as e:
        logger.error("Invalid input: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Search failed: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    company_id: str,
    agent_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    Get agent details with chunk counts.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Response**: 200 OK with AgentResponse

    **Error Handling**:
    - 403: Access denied to company
    - 404: Agent not found or doesn't belong to company
    - 500: Database query failure
    """
    logger.info(f"Getting agent {agent_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        agent_uuid = UUID(agent_id)

        # Get agent with visibility check (returns None if not visible)
        agent = await agent_service.get_agent_if_visible(
            str(agent_uuid), current_user, company_id
        )
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Agent {agent_id} not found"
            )

        logger.info(f"Found agent {agent_id} with {agent.get('chunk_count', 0)} chunks")

        return AgentResponse(
            id=str(agent["id"]),
            company_id=str(agent["company_id"]),
            name=agent["name"],
            personality=agent["personality"],
            main_responsibilities=agent["main_responsibilities"],
            system_prompt=agent["system_prompt"],
            metadata=agent.get("metadata", {}),
            created_at=agent["created_at"],
            updated_at=agent["updated_at"],
            created_by=str(agent.get("created_by")) if agent.get("created_by") else None,
            when_to_use=agent.get("when_to_use"),
            project_id=agent.get("project_id"),
            user_id=agent.get("user_id"),
            sealed=agent.get("sealed", False),
            public=agent.get("public", False),
            chunk_count=agent.get("chunk_count"),
            file_count=agent.get("file_count")
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid agent_id or company_id format"
        )
    except Exception as e:
        logger.error("Failed to get agent: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get agent: {str(e)}"
        )


@router.delete("/{agent_id}", response_model=dict)
async def delete_agent(
    company_id: str,
    agent_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    agent_service: AgentService = Depends(get_agent_service)
):
    """
    Delete agent (cascades to chunks, Neo4j, Qdrant).

    **Authentication Required**: User must be authenticated and have access to the company.

    **Response**: 200 OK with {"message": "Agent deleted successfully"}

    **Process**:
    1. Validate company access
    2. Verify agent exists and belongs to company
    3. Delete from Qdrant (vector store)
    4. Delete from Neo4j (knowledge graph)
    5. Delete from PostgreSQL (cascades to skill_chunks)

    **Error Handling**:
    - 403: Access denied to company
    - 404: Agent not found or doesn't belong to company
    - 500: Cascade delete failure
    """
    logger.info(f"Deleting agent {agent_id} from company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        agent_uuid = UUID(agent_id)

        # Delete with visibility + modification access checks
        # Raises 404 if not visible, 403 if sealed/not owner
        await agent_service.delete_agent_with_access_check(
            str(agent_uuid), current_user, company_id
        )

        logger.info(f"Successfully deleted agent {agent_id}")

        return {"message": "Agent deleted successfully"}

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid agent_id or company_id format"
        )
    except Exception as e:
        logger.error("Failed to delete agent: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete agent: {str(e)}"
        )


