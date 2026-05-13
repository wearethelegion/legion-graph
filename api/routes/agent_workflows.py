"""
KGRAG API - Agent Workflow Routes
RESTful endpoints for agent behavioral workflow management.

Access Control:
- GET /agent-workflows: Returns only visible workflows (owned OR public)
- GET /agent-workflows/{id}: Returns 404 if not visible (security: hide existence)
- PUT /agent-workflows/{id}: Returns 404 if not visible, 403 if not owner
- DELETE /agent-workflows/{id}: Returns 404 if not visible, 403 if not owner
- POST /agent-workflows: Creates workflow with current user as owner
- GET /agent-workflows/applicable: Get workflows applicable to agent (hierarchical)
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from uuid import UUID
from typing import Optional
from loguru import logger

from api.services.agent_workflow_service import AgentWorkflowService
from api.models.agent_workflow import (
    AgentWorkflowCreate,
    AgentWorkflowUpdate,
    AgentWorkflowResponse,
    AgentWorkflowListResponse,
)
from api.auth import CurrentUser, get_current_user, validate_company_access
from api.database import get_db_pool
from api.repositories.agent_workflow_repository import AgentWorkflowRepository
from api.repositories.project_repository import ProjectRepository
from api.repositories.agent_repository import AgentRepository

router = APIRouter(
    prefix="/api/v1/companies/{company_id}/agent-workflows", tags=["agent-workflows"]
)


def _normalize_description(workflow_id: str, description) -> Optional[str]:
    """
    Normalize workflow description to a string.

    Handles data corruption where description was stored as a list
    due to _parse_json_fields in base_repository parsing text fields.

    Args:
        workflow_id: Workflow ID for logging
        description: Raw description value from database

    Returns:
        Normalized string description or None
    """
    if description is None:
        return None
    if isinstance(description, str):
        return description
    # Handle list (data corruption case)
    logger.warning(
        f"Workflow {workflow_id} has invalid description type: {type(description)}. "
        "Converting to string."
    )
    if isinstance(description, list):
        return description[0] if description else None
    return str(description)


async def get_workflow_service() -> AgentWorkflowService:
    """Dependency to provide AgentWorkflowService instance."""
    pool = await get_db_pool()
    workflow_repo = AgentWorkflowRepository(pool)
    project_repo = ProjectRepository(pool)
    agent_repo = AgentRepository(pool)

    return AgentWorkflowService(workflow_repo, project_repo, agent_repo)


@router.post("", response_model=AgentWorkflowResponse, status_code=status.HTTP_201_CREATED)
async def create_workflow(
    company_id: str,
    data: AgentWorkflowCreate,
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    Create a new agent workflow with JSON body.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body**:
    - name: Workflow name (required)
    - content: Markdown workflow content (required)
    - description: Optional description
    - project_id: Scope to project (optional)
    - agent_id: Scope to specific agent (optional)
    - role: Scope to agent role (optional)
    - public: Make visible to others (optional, default: false)
    - metadata: Optional flexible metadata

    **Response**: 201 Created with AgentWorkflowResponse

    **Access Control**:
    - user_id is automatically set to current user (cannot be spoofed)

    **Error Handling**:
    - 400: Duplicate workflow name in scope
    - 403: Access denied to company
    - 500: Processing failure
    """
    logger.info(f"Creating workflow '{data.name}' for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        workflow = await workflow_service.create_workflow(
            company_id=company_id,
            name=data.name,
            content=data.content,
            current_user=current_user,
            project_id=data.project_id,
            agent_id=data.agent_id,
            role=data.role,
            public=data.public,
            description=data.description,
            metadata=data.metadata,
            signals=data.signals,
            when_to_use=data.when_to_use,
        )

        logger.info(
            f"Successfully created workflow {workflow['id']} ({data.name}) "
            f"owned by {current_user.user_id}"
        )

        return AgentWorkflowResponse(
            id=str(workflow["id"]),
            company_id=str(workflow["company_id"]),
            project_id=workflow.get("project_id"),
            agent_id=workflow.get("agent_id"),
            role=workflow.get("role"),
            user_id=workflow.get("user_id"),
            public=workflow.get("public", False),
            name=workflow["name"],
            content=workflow["content"],
            description=_normalize_description(workflow["id"], workflow.get("description")),
            metadata=workflow.get("metadata"),
            signals=workflow.get("signals") or [],
            when_to_use=workflow.get("when_to_use"),
            version=workflow.get("version", 1),
            created_at=workflow["created_at"],
            updated_at=workflow["updated_at"],
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for workflow creation: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to create workflow: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create workflow: {str(e)}",
        )


@router.get("", response_model=dict)
async def list_workflows(
    company_id: str,
    project_id: Optional[str] = Query(None, description="Filter by project"),
    agent_id: Optional[str] = Query(None, description="Filter by agent"),
    role: Optional[str] = Query(None, description="Filter by role"),
    include_public: bool = Query(True, description="Include public workflows"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    List all visible workflows for company with pagination.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Query Parameters**:
    - project_id: Filter by project UUID
    - agent_id: Filter by agent UUID
    - role: Filter by role
    - include_public: Include public workflows (default: true)
    - limit: Maximum number of results (default: 50, max: 100)
    - offset: Number of results to skip (default: 0)

    **Response**: 200 OK with {"workflows": [AgentWorkflowResponse], "total": int}

    **Error Handling**:
    - 403: Access denied to company
    - 500: Database query failure
    """
    logger.info(f"Listing workflows for company {company_id} (limit={limit}, offset={offset})")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        result = await workflow_service.list_workflows_visible(
            company_id=company_id,
            current_user=current_user,
            project_id=project_id,
            agent_id=agent_id,
            role=role,
            include_public=include_public,
            limit=limit,
            offset=offset,
        )

        # Convert to response format
        workflow_responses = [
            AgentWorkflowResponse(
                id=str(w["id"]),
                company_id=str(w["company_id"]),
                project_id=w.get("project_id"),
                agent_id=w.get("agent_id"),
                role=w.get("role"),
                user_id=w.get("user_id"),
                public=w.get("public", False),
                name=w["name"],
                content=w["content"],
                description=_normalize_description(w["id"], w.get("description")),
                metadata=w.get("metadata"),
                signals=w.get("signals") or [],
                when_to_use=w.get("when_to_use"),
                version=w.get("version", 1),
                created_at=w["created_at"],
                updated_at=w["updated_at"],
            )
            for w in result["workflows"]
        ]

        logger.info(f"Found {len(workflow_responses)} visible workflows for company {company_id}")

        return {"workflows": workflow_responses, "total": result["total_count"]}

    except ValueError as e:
        error_msg = str(e)
        if "validation error" in error_msg.lower():
            logger.error("Data validation error in workflows: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Workflow data validation error - please contact support",
            )
        logger.error("Invalid company_id format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid company_id format"
        )
    except Exception as e:
        logger.error("Failed to list workflows: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list workflows: {str(e)}",
        )


@router.get("/export", response_model=dict)
async def export_workflows(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """Export workflows for the requested company."""
    validate_company_access(current_user, company_id)

    try:
        return await workflow_service.export_company_workflows(
            company_id=company_id,
            current_user=current_user,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to export workflows for company {}: {}", company_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export workflows: {str(e)}",
        )


@router.get("/applicable", response_model=dict)
async def get_applicable_workflows(
    company_id: str,
    agent_id: Optional[str] = Query(None, description="Agent UUID"),
    role: Optional[str] = Query(None, description="Agent role"),
    project_id: Optional[str] = Query(None, description="Project UUID"),
    include_public: bool = Query(True, description="Include public workflows"),
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    Get applicable workflows for an agent, ordered by specificity.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Query Parameters**:
    - agent_id: Agent UUID for agent-specific workflows
    - role: Agent role for role-specific workflows
    - project_id: Project UUID for project-specific workflows
    - include_public: Include public workflows (default: true)

    **Resolution Order** (most to least specific):
    1. agent_id match
    2. role match
    3. project_id match
    4. company-level

    **Response**: 200 OK with {"workflows": [AgentWorkflowResponse]}

    **Error Handling**:
    - 403: Access denied to company
    - 500: Database query failure
    """
    logger.info(
        f"Getting applicable workflows for company {company_id} "
        f"(agent_id={agent_id}, role={role}, project_id={project_id})"
    )

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        workflows = await workflow_service.get_applicable_workflows(
            company_id=company_id,
            current_user=current_user,
            agent_id=agent_id,
            role=role,
            project_id=project_id,
            include_public=include_public,
        )

        # Convert to response format
        workflow_responses = [
            AgentWorkflowResponse(
                id=str(w["id"]),
                company_id=str(w["company_id"]),
                project_id=w.get("project_id"),
                agent_id=w.get("agent_id"),
                role=w.get("role"),
                user_id=w.get("user_id"),
                public=w.get("public", False),
                name=w["name"],
                content=w["content"],
                description=_normalize_description(w["id"], w.get("description")),
                metadata=w.get("metadata"),
                signals=w.get("signals") or [],
                when_to_use=w.get("when_to_use"),
                version=w.get("version", 1),
                created_at=w["created_at"],
                updated_at=w["updated_at"],
            )
            for w in workflows
        ]

        logger.info(f"Found {len(workflow_responses)} applicable workflows")

        return {"workflows": workflow_responses}

    except Exception as e:
        logger.error("Failed to get applicable workflows: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get applicable workflows: {str(e)}",
        )


@router.get("/{workflow_id}", response_model=AgentWorkflowResponse)
async def get_workflow(
    company_id: str,
    workflow_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    Get workflow details.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Response**: 200 OK with AgentWorkflowResponse

    **Error Handling**:
    - 403: Access denied to company
    - 404: Workflow not found or not visible
    - 500: Database query failure
    """
    logger.info(f"Getting workflow {workflow_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        UUID(workflow_id)  # Validate UUID format

        workflow = await workflow_service.get_workflow_if_visible(
            workflow_id, current_user, company_id
        )
        if not workflow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Workflow {workflow_id} not found"
            )

        logger.info(f"Found workflow {workflow_id}")

        return AgentWorkflowResponse(
            id=str(workflow["id"]),
            company_id=str(workflow["company_id"]),
            project_id=workflow.get("project_id"),
            agent_id=workflow.get("agent_id"),
            role=workflow.get("role"),
            user_id=workflow.get("user_id"),
            public=workflow.get("public", False),
            name=workflow["name"],
            content=workflow["content"],
            description=_normalize_description(workflow["id"], workflow.get("description")),
            metadata=workflow.get("metadata"),
            signals=workflow.get("signals") or [],
            when_to_use=workflow.get("when_to_use"),
            version=workflow.get("version", 1),
            created_at=workflow["created_at"],
            updated_at=workflow["updated_at"],
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workflow_id or company_id format",
        )
    except Exception as e:
        logger.error("Failed to get workflow: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get workflow: {str(e)}",
        )


@router.put("/{workflow_id}", response_model=AgentWorkflowResponse)
async def update_workflow(
    company_id: str,
    workflow_id: str,
    data: AgentWorkflowUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    Update an existing workflow.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body** (all fields optional):
    - name: New workflow name
    - content: New markdown content
    - description: New description
    - project_id: Change project scope (null to clear, omit to keep)
    - agent_id: Change agent scope (null to clear, omit to keep)
    - role: Change role scope (null to clear, omit to keep)
    - public: Change visibility
    - metadata: New metadata
    - signals: New trigger hints

    **Scope Change Semantics**:
    - Omitted field = no change to current value
    - Field set to null = clear scope (move towards company-scope)
    - Field set to value = set new scope (validated against company)

    **Access Control**:
    - Returns 404 if workflow not found OR not visible (security: hide existence)
    - Returns 403 if user is not owner (non-admin) or system workflow (non-admin)

    **Note**: user_id cannot be changed (ownership transfer is not supported).

    **Response**: 200 OK with updated AgentWorkflowResponse
    """
    logger.info(f"Updating workflow {workflow_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        # Use exclude_unset to distinguish "not sent" from "sent as null"
        update_kwargs = data.model_dump(exclude_unset=True)

        workflow = await workflow_service.update_workflow_with_access_check(
            workflow_id=workflow_id,
            current_user=current_user,
            company_id=company_id,
            **update_kwargs,
        )

        logger.info(f"Successfully updated workflow {workflow_id}")

        return AgentWorkflowResponse(
            id=str(workflow["id"]),
            company_id=str(workflow["company_id"]),
            project_id=workflow.get("project_id"),
            agent_id=workflow.get("agent_id"),
            role=workflow.get("role"),
            user_id=workflow.get("user_id"),
            public=workflow.get("public", False),
            name=workflow["name"],
            content=workflow["content"],
            description=_normalize_description(workflow["id"], workflow.get("description")),
            metadata=workflow.get("metadata"),
            signals=workflow.get("signals") or [],
            when_to_use=workflow.get("when_to_use"),
            version=workflow.get("version", 1),
            created_at=workflow["created_at"],
            updated_at=workflow["updated_at"],
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for workflow update: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to update workflow: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update workflow: {str(e)}",
        )


@router.delete("/{workflow_id}", response_model=dict)
async def delete_workflow(
    company_id: str,
    workflow_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    workflow_service: AgentWorkflowService = Depends(get_workflow_service),
):
    """
    Delete a workflow.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Response**: 200 OK with {"message": "Workflow deleted successfully"}

    **Access Control**:
    - Returns 404 if workflow not found OR not visible (security: hide existence)
    - Returns 403 if user is not owner (non-admin) or system workflow (non-admin)

    **Error Handling**:
    - 403: Access denied to company or not owner
    - 404: Workflow not found or not visible
    - 500: Delete failure
    """
    logger.info(f"Deleting workflow {workflow_id} from company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        UUID(workflow_id)  # Validate UUID format

        await workflow_service.delete_workflow_with_access_check(
            workflow_id, current_user, company_id
        )

        logger.info(f"Successfully deleted workflow {workflow_id}")

        return {"message": "Workflow deleted successfully"}

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workflow_id or company_id format",
        )
    except Exception as e:
        logger.error("Failed to delete workflow: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete workflow: {str(e)}",
        )
