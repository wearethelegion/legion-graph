# """
# Workflow Definition Routes (Company-Scoped)
# Thin HTTP controllers that delegate to service layer.
# Handles workflow definition and step CRUD endpoints.

# Based on ADR-002: Workflow Configuration API Architecture
# Revision: Company-scoped endpoints (f73613ec)
# """

# from typing import Optional
# from fastapi import APIRouter, Depends, HTTPException, Query, status
# from api.auth import get_current_user, CurrentUser, validate_company_access
# from api.database import get_db_pool
# from api.services.workflow_definition_service import WorkflowDefinitionService
# from api.models.workflow_definition import (
#     WorkflowDefinitionCreate,
#     WorkflowDefinitionUpdate,
#     WorkflowDefinitionResponse,
#     WorkflowDefinitionWithStepsResponse,
#     WorkflowDefinitionListResponse,
#     WorkflowStepCreate,
#     WorkflowStepUpdate,
#     WorkflowStepResponse,
#     WorkflowValidationResult,
# )
# from loguru import logger
# import asyncpg

# router = APIRouter(prefix="/api/v1/companies", tags=["workflow-definitions"])


# # =============================================================================
# # Service Dependencies
# # =============================================================================

# async def get_workflow_definition_service(
#     pool: asyncpg.Pool = Depends(get_db_pool)
# ) -> WorkflowDefinitionService:
#     """Dependency to get workflow definition service."""
#     return WorkflowDefinitionService(pool=pool)


# # =============================================================================
# # Helper Functions
# # =============================================================================

# def _definition_to_response(definition: dict, include_steps: bool = False) -> WorkflowDefinitionResponse:
#     """Convert definition dict to response model.

#     Args:
#         definition: Dict from repository
#         include_steps: If True, returns WorkflowDefinitionWithStepsResponse with steps
#     """
#     steps = []
#     if include_steps and definition.get("steps"):
#         steps = [
#             WorkflowStepResponse(
#                 id=s["id"],
#                 workflow_definition_id=s["workflow_definition_id"],
#                 name=s["name"],
#                 display_name=s.get("display_name"),
#                 step_order=s["step_order"],
#                 step_type=s["step_type"],
#                 agent_id=s.get("agent_id"),
#                 agent_role=s.get("agent_role"),
#                 task_template=s["task_template"],
#                 timeout_seconds=s["timeout_seconds"],
#                 max_retries=s["max_retries"],
#                 on_success=s["on_success"],
#                 on_failure=s["on_failure"],
#                 requires_approval=s["requires_approval"],
#                 created_at=s["created_at"],
#                 updated_at=s["updated_at"],
#             )
#             for s in definition["steps"]
#         ]

#     if include_steps:
#         return WorkflowDefinitionWithStepsResponse(
#             id=definition["id"],
#             company_id=definition["company_id"],
#             project_id=definition.get("project_id"),
#             name=definition["name"],
#             description=definition.get("description"),
#             is_active=definition["is_active"],
#             is_builtin=definition["is_builtin"],
#             version=definition["version"],
#             created_by_agent_id=definition.get("created_by_agent_id"),
#             created_at=definition["created_at"],
#             updated_at=definition["updated_at"],
#             steps=steps,
#         )
#     else:
#         return WorkflowDefinitionResponse(
#             id=definition["id"],
#             company_id=definition["company_id"],
#             project_id=definition.get("project_id"),
#             name=definition["name"],
#             description=definition.get("description"),
#             is_active=definition["is_active"],
#             is_builtin=definition["is_builtin"],
#             version=definition["version"],
#             created_by_agent_id=definition.get("created_by_agent_id"),
#             created_at=definition["created_at"],
#             updated_at=definition["updated_at"],
#         )


# def _step_to_response(step: dict) -> WorkflowStepResponse:
#     """Convert step dict to WorkflowStepResponse model."""
#     return WorkflowStepResponse(
#         id=step["id"],
#         workflow_definition_id=step["workflow_definition_id"],
#         name=step["name"],
#         display_name=step.get("display_name"),
#         step_order=step["step_order"],
#         step_type=step["step_type"],
#         agent_id=step.get("agent_id"),
#         agent_role=step.get("agent_role"),
#         task_template=step["task_template"],
#         timeout_seconds=step["timeout_seconds"],
#         max_retries=step["max_retries"],
#         on_success=step["on_success"],
#         on_failure=step["on_failure"],
#         requires_approval=step["requires_approval"],
#         created_at=step["created_at"],
#         updated_at=step["updated_at"],
#     )


# # =============================================================================
# # Definition Endpoints (Company-Scoped)
# # =============================================================================

# @router.get(
#     "/{company_id}/workflow-definitions",
#     response_model=WorkflowDefinitionListResponse,
#     summary="List workflow definitions for a company"
# )
# async def list_workflow_definitions(
#     company_id: str,
#     project_id: Optional[str] = Query(None, description="Filter by project (null = company-wide)"),
#     include_inactive: bool = Query(False, description="Include inactive definitions"),
#     limit: int = Query(50, ge=1, le=100, description="Maximum results"),
#     offset: int = Query(0, ge=0, description="Offset for pagination"),
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowDefinitionListResponse:
#     """
#     List workflow definitions for a company.

#     - **company_id**: Company UUID (path parameter)
#     - **project_id**: Optional filter by project (null = company-wide definitions)
#     - **include_inactive**: Include inactive definitions (default: false)
#     - **limit**: Maximum results (default: 50, max: 100)
#     - **offset**: Pagination offset (default: 0)

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         definitions = await service.list_definitions(
#             company_id=company_id,
#             project_id=project_id,
#             include_inactive=include_inactive,
#             limit=limit,
#             offset=offset,
#         )

#         return WorkflowDefinitionListResponse(
#             total_count=len(definitions),
#             definitions=[_definition_to_response(d) for d in definitions],
#         )

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to list workflow definitions: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to list workflow definitions: {str(e)}"
#         )


# @router.post(
#     "/{company_id}/workflow-definitions",
#     response_model=WorkflowDefinitionWithStepsResponse,
#     status_code=status.HTTP_201_CREATED,
#     summary="Create a new workflow definition"
# )
# async def create_workflow_definition(
#     company_id: str,
#     data: WorkflowDefinitionCreate,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowDefinitionWithStepsResponse:
#     """
#     Create a new workflow definition with optional initial steps.

#     - **company_id**: Company UUID (path parameter)
#     - **name**: Unique workflow name (snake_case)
#     - **description**: Optional description
#     - **project_id**: Optional project UUID (null = company-wide)
#     - **steps**: Optional list of initial step definitions
#     - **created_by_agent_id**: Optional agent who created this

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # Convert steps to dict format for service
#         steps_data = None
#         if data.steps:
#             steps_data = [
#                 {
#                     "name": s.name,
#                     "step_order": s.step_order,
#                     "task_template": s.task_template,
#                     "agent_id": s.agent_id,
#                     "agent_role": s.agent_role,
#                     "display_name": s.display_name,
#                     "timeout_seconds": s.timeout_seconds,
#                     "max_retries": s.max_retries,
#                     "on_success": s.on_success,
#                     "on_failure": s.on_failure,
#                     "requires_approval": s.requires_approval,
#                     "step_type": s.step_type,
#                 }
#                 for s in data.steps
#             ]

#         definition = await service.create_definition(
#             company_id=company_id,
#             name=data.name,
#             description=data.description,
#             project_id=data.project_id,
#             steps=steps_data,
#             created_by_agent_id=data.created_by_agent_id,
#         )

#         return _definition_to_response(definition, include_steps=True)

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to create workflow definition: {e}", exc_info=True)
#         # Check for unique constraint violation
#         if "unique" in str(e).lower() or "duplicate" in str(e).lower():
#             raise HTTPException(
#                 status_code=status.HTTP_409_CONFLICT,
#                 detail=f"Workflow definition with name '{data.name}' already exists"
#             )
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to create workflow definition: {str(e)}"
#         )


# @router.get(
#     "/{company_id}/workflow-definitions/{definition_id}",
#     response_model=WorkflowDefinitionWithStepsResponse,
#     summary="Get workflow definition details"
# )
# async def get_workflow_definition(
#     company_id: str,
#     definition_id: str,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowDefinitionWithStepsResponse:
#     """
#     Get workflow definition details including all steps.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         definition = await service.get_definition_with_steps(definition_id)

#         if not definition:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Verify definition belongs to this company
#         if definition["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         return _definition_to_response(definition, include_steps=True)

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to get workflow definition: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to get workflow definition: {str(e)}"
#         )


# @router.put(
#     "/{company_id}/workflow-definitions/{definition_id}",
#     response_model=WorkflowDefinitionWithStepsResponse,
#     summary="Update workflow definition"
# )
# async def update_workflow_definition(
#     company_id: str,
#     definition_id: str,
#     data: WorkflowDefinitionUpdate,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowDefinitionWithStepsResponse:
#     """
#     Update workflow definition fields.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID
#     - **name**: Optional new name (snake_case)
#     - **description**: Optional new description
#     - **is_active**: Optional active status

#     Note: Builtin workflows cannot be modified.

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Check builtin protection
#         if existing.get("is_builtin", False):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="Cannot modify builtin workflow definitions"
#             )

#         definition = await service.update_definition(
#             definition_id=definition_id,
#             name=data.name,
#             description=data.description,
#             is_active=data.is_active,
#         )

#         if not definition:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Get full definition with steps for response
#         full_definition = await service.get_definition_with_steps(definition_id)
#         return _definition_to_response(full_definition, include_steps=True)

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to update workflow definition: {e}", exc_info=True)
#         if "unique" in str(e).lower() or "duplicate" in str(e).lower():
#             raise HTTPException(
#                 status_code=status.HTTP_409_CONFLICT,
#                 detail=f"Workflow definition with name '{data.name}' already exists"
#             )
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to update workflow definition: {str(e)}"
#         )


# @router.delete(
#     "/{company_id}/workflow-definitions/{definition_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
#     summary="Delete workflow definition"
# )
# async def delete_workflow_definition(
#     company_id: str,
#     definition_id: str,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> None:
#     """
#     Delete a workflow definition and all its steps.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID

#     Note: Builtin workflows cannot be deleted.

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Check builtin protection
#         if existing.get("is_builtin", False):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="Cannot delete builtin workflow definitions"
#             )

#         deleted = await service.delete_definition(definition_id)
#         if not deleted:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to delete workflow definition: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to delete workflow definition: {str(e)}"
#         )


# # =============================================================================
# # Validation Endpoint
# # =============================================================================

# @router.post(
#     "/{company_id}/workflow-definitions/{definition_id}/validate",
#     response_model=WorkflowValidationResult,
#     summary="Validate workflow definition"
# )
# async def validate_workflow_definition(
#     company_id: str,
#     definition_id: str,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowValidationResult:
#     """
#     Validate workflow definition structure.

#     Checks for:
#     - At least one step
#     - Connected graph (all step references valid)
#     - No orphan steps
#     - Valid agent assignments
#     - Proper flow control

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         result = await service.validate_definition(definition_id)
#         return result

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to validate workflow definition: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to validate workflow definition: {str(e)}"
#         )


# # =============================================================================
# # Step Endpoints
# # =============================================================================

# @router.post(
#     "/{company_id}/workflow-definitions/{definition_id}/steps",
#     response_model=WorkflowStepResponse,
#     status_code=status.HTTP_201_CREATED,
#     summary="Add step to workflow definition"
# )
# async def add_workflow_step(
#     company_id: str,
#     definition_id: str,
#     data: WorkflowStepCreate,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowStepResponse:
#     """
#     Add a step to a workflow definition.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID
#     - **name**: Step name (snake_case, unique within definition)
#     - **step_order**: Execution order (1-based)
#     - **task_template**: Task template with {{variable}} placeholders
#     - **agent_id**: Optional specific agent UUID
#     - **agent_role**: Optional agent role (researcher, architect, developer, protector)
#     - **timeout_seconds**: Max execution time (default: 600)
#     - **max_retries**: Retry count on failure (default: 0)
#     - **on_success**: Next step name or "END" (default: "END")
#     - **on_failure**: Next step name, "error", or "retry" (default: "error")
#     - **requires_approval**: Human approval required before executing (default: false)
#     - **step_type**: Step type (agent, approval, conditional) (default: "agent")

#     Note: Builtin workflows cannot be modified.

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Check builtin protection
#         if existing.get("is_builtin", False):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="Cannot modify builtin workflow definitions"
#             )

#         step = await service.add_step(
#             workflow_definition_id=definition_id,
#             name=data.name,
#             step_order=data.step_order,
#             task_template=data.task_template,
#             agent_id=data.agent_id,
#             agent_role=data.agent_role,
#             display_name=data.display_name,
#             timeout_seconds=data.timeout_seconds,
#             max_retries=data.max_retries,
#             on_success=data.on_success,
#             on_failure=data.on_failure,
#             requires_approval=data.requires_approval,
#             step_type=data.step_type,
#         )

#         return _step_to_response(step)

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to add workflow step: {e}", exc_info=True)
#         if "unique" in str(e).lower() or "duplicate" in str(e).lower():
#             raise HTTPException(
#                 status_code=status.HTTP_409_CONFLICT,
#                 detail=f"Step with name '{data.name}' already exists in this workflow"
#             )
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to add workflow step: {str(e)}"
#         )


# @router.put(
#     "/{company_id}/workflow-definitions/{definition_id}/steps/{step_id}",
#     response_model=WorkflowStepResponse,
#     summary="Update workflow step"
# )
# async def update_workflow_step(
#     company_id: str,
#     definition_id: str,
#     step_id: str,
#     data: WorkflowStepUpdate,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> WorkflowStepResponse:
#     """
#     Update a workflow step.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID
#     - **step_id**: Step UUID
#     - All step fields are optional for partial updates

#     Note: Builtin workflows cannot be modified.

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Check builtin protection
#         if existing.get("is_builtin", False):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="Cannot modify builtin workflow definitions"
#             )

#         # Verify step belongs to this definition
#         existing_step = await service.get_step(step_id)
#         if not existing_step:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found"
#             )

#         if existing_step["workflow_definition_id"] != definition_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found in definition {definition_id}"
#             )

#         # Build update kwargs from non-None fields
#         update_kwargs = {}
#         for field in ["name", "display_name", "step_order", "step_type",
#                       "agent_id", "agent_role", "task_template", "timeout_seconds",
#                       "max_retries", "on_success", "on_failure", "requires_approval"]:
#             value = getattr(data, field, None)
#             if value is not None:
#                 update_kwargs[field] = value

#         step = await service.update_step(step_id=step_id, **update_kwargs)

#         if not step:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found"
#             )

#         return _step_to_response(step)

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to update workflow step: {e}", exc_info=True)
#         if "unique" in str(e).lower() or "duplicate" in str(e).lower():
#             raise HTTPException(
#                 status_code=status.HTTP_409_CONFLICT,
#                 detail=f"Step with name '{data.name}' already exists in this workflow"
#             )
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to update workflow step: {str(e)}"
#         )


# @router.delete(
#     "/{company_id}/workflow-definitions/{definition_id}/steps/{step_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
#     summary="Delete workflow step"
# )
# async def delete_workflow_step(
#     company_id: str,
#     definition_id: str,
#     step_id: str,
#     current_user: CurrentUser = Depends(get_current_user),
#     service: WorkflowDefinitionService = Depends(get_workflow_definition_service),
# ) -> None:
#     """
#     Delete a workflow step.

#     - **company_id**: Company UUID (path parameter)
#     - **definition_id**: Definition UUID
#     - **step_id**: Step UUID

#     Note: Builtin workflows cannot be modified.

#     **Authentication Required**
#     """
#     # Direct company access validation (per ADR-002 revision)
#     validate_company_access(current_user, company_id)

#     try:
#         # First verify definition exists and belongs to company
#         existing = await service.get_definition(definition_id)
#         if not existing:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         if existing["company_id"] != company_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Workflow definition {definition_id} not found"
#             )

#         # Check builtin protection
#         if existing.get("is_builtin", False):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="Cannot modify builtin workflow definitions"
#             )

#         # Verify step belongs to this definition
#         existing_step = await service.get_step(step_id)
#         if not existing_step:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found"
#             )

#         if existing_step["workflow_definition_id"] != definition_id:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found in definition {definition_id}"
#             )

#         deleted = await service.delete_step(step_id)
#         if not deleted:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Step {step_id} not found"
#             )

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to delete workflow step: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to delete workflow step: {str(e)}"
#         )
