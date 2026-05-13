# """
# Workflow Definition Service
# Business logic for workflow definition management.

# Based on ADR-002: Workflow Configuration API Architecture
# """

# from typing import Dict, Any, List, Optional
# import asyncpg
# from loguru import logger

# from api.repositories.workflow_definition_repository import WorkflowDefinitionRepository
# from api.models.workflow_definition import WorkflowValidationResult


# class WorkflowDefinitionService:
#     """Service for workflow definition business logic."""

#     def __init__(self, pool: asyncpg.Pool):
#         self.pool = pool
#         self.repo = WorkflowDefinitionRepository(pool)

#     # =========================================================================
#     # Definition Operations
#     # =========================================================================

#     async def create_definition(
#         self,
#         company_id: str,
#         name: str,
#         description: Optional[str] = None,
#         project_id: Optional[str] = None,
#         steps: Optional[List[Dict[str, Any]]] = None,
#         created_by_agent_id: Optional[str] = None
#     ) -> Dict[str, Any]:
#         """
#         Create a workflow definition with optional initial steps.

#         Args:
#             company_id: Company UUID
#             name: Workflow name (unique per company)
#             description: Optional description
#             project_id: Optional project UUID (null = company-wide)
#             steps: Optional list of step definitions
#             created_by_agent_id: Optional agent UUID who created this

#         Returns:
#             Created definition with steps
#         """
#         # Create definition
#         definition = await self.repo.create_definition(
#             company_id=company_id,
#             name=name,
#             description=description,
#             project_id=project_id,
#             created_by_agent_id=created_by_agent_id
#         )

#         definition_id = definition["id"]

#         # Add initial steps if provided
#         if steps:
#             for step_data in steps:
#                 await self.repo.add_step(
#                     workflow_definition_id=definition_id,
#                     name=step_data["name"],
#                     step_order=step_data["step_order"],
#                     task_template=step_data["task_template"],
#                     agent_id=step_data.get("agent_id"),
#                     agent_role=step_data.get("agent_role"),
#                     display_name=step_data.get("display_name"),
#                     timeout_seconds=step_data.get("timeout_seconds", 600),
#                     max_retries=step_data.get("max_retries", 0),
#                     on_success=step_data.get("on_success", "END"),
#                     on_failure=step_data.get("on_failure", "error"),
#                     requires_approval=step_data.get("requires_approval", False),
#                     step_type=step_data.get("step_type", "agent")
#                 )

#         # Return definition with steps
#         return await self.repo.get_definition_with_steps(definition_id)

#     async def get_definition(self, definition_id: str) -> Optional[Dict[str, Any]]:
#         """Get workflow definition by ID (without steps)."""
#         return await self.repo.get_definition(definition_id)

#     async def get_definition_with_steps(self, definition_id: str) -> Optional[Dict[str, Any]]:
#         """Get workflow definition with all steps."""
#         return await self.repo.get_definition_with_steps(definition_id)

#     async def get_definition_by_name(
#         self,
#         company_id: str,
#         name: str,
#         project_id: Optional[str] = None
#     ) -> Optional[Dict[str, Any]]:
#         """Get workflow definition by name."""
#         return await self.repo.get_definition_by_name(
#             company_id=company_id,
#             name=name,
#             project_id=project_id
#         )

#     async def list_definitions(
#         self,
#         company_id: str,
#         project_id: Optional[str] = None,
#         include_inactive: bool = False,
#         limit: int = 50,
#         offset: int = 0
#     ) -> List[Dict[str, Any]]:
#         """List workflow definitions for a company."""
#         return await self.repo.list_definitions(
#             company_id=company_id,
#             project_id=project_id,
#             include_inactive=include_inactive,
#             limit=limit,
#             offset=offset
#         )

#     async def update_definition(
#         self,
#         definition_id: str,
#         name: Optional[str] = None,
#         description: Optional[str] = None,
#         is_active: Optional[bool] = None
#     ) -> Optional[Dict[str, Any]]:
#         """Update workflow definition fields."""
#         return await self.repo.update_definition(
#             definition_id=definition_id,
#             name=name,
#             description=description,
#             is_active=is_active
#         )

#     async def delete_definition(self, definition_id: str) -> bool:
#         """Delete workflow definition."""
#         return await self.repo.delete_definition(definition_id)

#     # =========================================================================
#     # Step Operations
#     # =========================================================================

#     async def add_step(
#         self,
#         workflow_definition_id: str,
#         name: str,
#         step_order: int,
#         task_template: str,
#         agent_id: Optional[str] = None,
#         agent_role: Optional[str] = None,
#         display_name: Optional[str] = None,
#         timeout_seconds: int = 600,
#         max_retries: int = 0,
#         on_success: str = "END",
#         on_failure: str = "error",
#         requires_approval: bool = False,
#         step_type: str = "agent"
#     ) -> Dict[str, Any]:
#         """Add a step to a workflow definition."""
#         return await self.repo.add_step(
#             workflow_definition_id=workflow_definition_id,
#             name=name,
#             step_order=step_order,
#             task_template=task_template,
#             agent_id=agent_id,
#             agent_role=agent_role,
#             display_name=display_name,
#             timeout_seconds=timeout_seconds,
#             max_retries=max_retries,
#             on_success=on_success,
#             on_failure=on_failure,
#             requires_approval=requires_approval,
#             step_type=step_type
#         )

#     async def get_step(self, step_id: str) -> Optional[Dict[str, Any]]:
#         """Get workflow step by ID."""
#         return await self.repo.get_step(step_id)

#     async def update_step(
#         self,
#         step_id: str,
#         **kwargs
#     ) -> Optional[Dict[str, Any]]:
#         """Update workflow step fields."""
#         return await self.repo.update_step(step_id=step_id, **kwargs)

#     async def delete_step(self, step_id: str) -> bool:
#         """Delete a workflow step."""
#         return await self.repo.delete_step(step_id)

#     # =========================================================================
#     # Validation
#     # =========================================================================

#     async def validate_definition(self, definition_id: str) -> WorkflowValidationResult:
#         """
#         Validate workflow definition structure.

#         Checks for:
#         - At least one step
#         - Connected graph (all step references valid)
#         - No orphan steps
#         - Valid agent assignments
#         - Proper flow control

#         Args:
#             definition_id: Workflow definition UUID

#         Returns:
#             Validation result with errors and warnings
#         """
#         errors: List[str] = []
#         warnings: List[str] = []

#         # Get definition with steps
#         definition = await self.repo.get_definition_with_steps(definition_id)
#         if not definition:
#             return WorkflowValidationResult(
#                 is_valid=False,
#                 errors=["Definition not found"],
#                 warnings=[]
#             )

#         steps = definition.get("steps", [])

#         # Check: At least one step
#         if not steps:
#             errors.append("Workflow must have at least one step")
#             return WorkflowValidationResult(
#                 is_valid=False,
#                 errors=errors,
#                 warnings=warnings
#             )

#         # Build step name set
#         step_names = {step["name"] for step in steps}
#         step_names.add("END")
#         step_names.add("error")

#         # Check each step
#         for step in steps:
#             step_name = step["name"]
#             step_type = step.get("step_type", "agent")

#             # Check: Agent assignment for agent steps
#             if step_type == "agent":
#                 if not step.get("agent_id") and not step.get("agent_role"):
#                     errors.append(f"Step '{step_name}': agent_id or agent_role required for agent steps")
#                 elif step.get("agent_id") and step.get("agent_role"):
#                     warnings.append(f"Step '{step_name}': both agent_id and agent_role set (agent_id takes precedence)")

#             # Check: Valid on_success reference
#             on_success = step.get("on_success", "END")
#             if on_success not in step_names:
#                 errors.append(f"Step '{step_name}': on_success references unknown step '{on_success}'")

#             # Check: Valid on_failure reference
#             on_failure = step.get("on_failure", "error")
#             if on_failure not in step_names and on_failure != "retry":
#                 errors.append(f"Step '{step_name}': on_failure references unknown step '{on_failure}'")

#             # Check: Task template not empty
#             if not step.get("task_template", "").strip():
#                 errors.append(f"Step '{step_name}': task_template cannot be empty")

#         # Check: Reachability (simplified - check if all steps can be reached from step_order 1)
#         first_step = next((s for s in steps if s["step_order"] == 1), None)
#         if not first_step:
#             errors.append("No step with step_order=1 (entry point)")
#         else:
#             reachable = {first_step["name"]}
#             to_check = [first_step]

#             while to_check:
#                 current = to_check.pop()
#                 on_success = current.get("on_success", "END")
#                 on_failure = current.get("on_failure", "error")

#                 for next_name in [on_success, on_failure]:
#                     if next_name not in reachable and next_name not in ("END", "error", "retry"):
#                         next_step = next((s for s in steps if s["name"] == next_name), None)
#                         if next_step:
#                             reachable.add(next_name)
#                             to_check.append(next_step)

#             # Find unreachable steps
#             unreachable = step_names - reachable - {"END", "error"}
#             if unreachable:
#                 warnings.append(f"Unreachable steps: {', '.join(unreachable)}")

#         # Check: Step order gaps
#         orders = sorted(step["step_order"] for step in steps)
#         expected_orders = list(range(1, len(steps) + 1))
#         if orders != expected_orders:
#             warnings.append(f"Step order has gaps or duplicates: {orders}")

#         return WorkflowValidationResult(
#             is_valid=len(errors) == 0,
#             errors=errors,
#             warnings=warnings
#         )
