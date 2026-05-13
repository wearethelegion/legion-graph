"""
Agent Workflow Service
Business logic for agent behavioral workflow operations.

Access Control:
- is_workflow_visible: Checks if user can see a workflow
- can_modify_workflow: Checks if user can modify a workflow
- get_workflow_if_visible: Returns workflow only if visible to user
- list_workflows_visible: Returns filtered list based on visibility
- update_workflow_with_access_check: Validates visibility + modification rights + scope changes
- delete_workflow_with_access_check: Validates visibility + modification rights
"""

from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from loguru import logger
from datetime import datetime, timezone

from api.repositories.agent_workflow_repository import AgentWorkflowRepository
from api.repositories.project_repository import ProjectRepository
from api.repositories.agent_repository import AgentRepository
from api.services.workflow_access import is_workflow_visible, can_modify_workflow
from api.auth import CurrentUser


class AgentWorkflowService:
    """Service for agent workflow management operations."""

    def __init__(
        self,
        workflow_repo: AgentWorkflowRepository,
        project_repo: Optional[ProjectRepository] = None,
        agent_repo: Optional[AgentRepository] = None,
    ):
        self.workflow_repo = workflow_repo
        self.project_repo = project_repo
        self.agent_repo = agent_repo

    async def get_user_accessible_projects(
        self, current_user: CurrentUser, company_id: str
    ) -> List[str]:
        """
        Get list of project IDs user has access to within a company.

        For now, users can access all projects in companies they belong to.
        Future: Implement project-level access control if needed.

        Args:
            current_user: Authenticated user
            company_id: Company to get projects for

        Returns:
            List of project UUID strings
        """
        if not self.project_repo:
            return []

        # Get all projects for the company
        projects = await self.project_repo.get_by_company(company_id)
        return [p["id"] for p in projects]

    async def create_workflow(
        self,
        company_id: str,
        name: str,
        content: str,
        current_user: CurrentUser,
        project_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        public: bool = False,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        signals: Optional[List[str]] = None,
        when_to_use: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new workflow with owner set to current user.

        Note: user_id is automatically set to current_user.user_id.
        This ensures ownership cannot be spoofed via request body.

        Args:
            company_id: Company UUID
            name: Workflow name
            content: Markdown content
            current_user: Authenticated user (becomes owner)
            project_id: Optional project scope
            agent_id: Optional agent scope
            role: Optional role scope
            public: If True, visible to others
            description: Optional description
            metadata: Optional metadata
            signals: Optional list of trigger hints
            when_to_use: When to activate this workflow

        Returns:
            Created workflow dict

        Raises:
            HTTPException 400: Duplicate name in scope
        """
        try:
            # Check for duplicate name within scope
            name_exists = await self.workflow_repo.check_name_exists(
                company_id=company_id,
                name=name,
                project_id=project_id,
                agent_id=agent_id,
                role=role,
            )

            if name_exists:
                raise HTTPException(
                    status_code=400,
                    detail=f"Workflow with name '{name}' already exists in this scope",
                )

            # Create workflow with owner
            workflow = await self.workflow_repo.create(
                company_id=company_id,
                name=name,
                content=content,
                user_id=current_user.user_id,  # Owner = creator
                project_id=project_id,
                agent_id=agent_id,
                role=role,
                public=public,
                description=description,
                metadata=metadata,
                signals=signals,
                when_to_use=when_to_use,
            )

            logger.info(
                f"Created workflow {workflow['id']} ({name}) owned by {current_user.user_id}"
            )
            return workflow

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create workflow: {}", e)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_workflow_if_visible(
        self, workflow_id: str, current_user: CurrentUser, company_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get workflow only if visible to user.

        Returns None (not 403) if workflow isn't visible - security best practice
        to avoid revealing existence of hidden workflows.

        Args:
            workflow_id: Workflow UUID
            current_user: Authenticated user
            company_id: Optional company context for project lookup

        Returns:
            Workflow dict if visible, None otherwise
        """
        workflow = await self.workflow_repo.get_by_id(workflow_id)
        if not workflow:
            return None

        # Get accessible projects for visibility check
        accessible_projects = []
        if company_id and self.project_repo:
            accessible_projects = await self.get_user_accessible_projects(current_user, company_id)

        if not is_workflow_visible(workflow, current_user, accessible_projects):
            return None  # Return None, not 403 (security: hide existence)

        return workflow

    async def list_workflows_visible(
        self,
        company_id: str,
        current_user: CurrentUser,
        project_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        include_public: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List workflows with visibility filtering.

        Args:
            company_id: Company UUID (already validated for access)
            current_user: Authenticated user
            project_id: Optional filter by specific project
            agent_id: Optional filter by specific agent
            role: Optional filter by role
            include_public: Include public workflows (default: True)
            limit: Maximum results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'workflows' list
        """
        # Get user's accessible projects
        accessible_projects = await self.get_user_accessible_projects(current_user, company_id)

        return await self.workflow_repo.list_visible(
            company_id=company_id,
            current_user_id=current_user.user_id,
            accessible_project_ids=accessible_projects,
            project_id_filter=project_id,
            agent_id_filter=agent_id,
            role_filter=role,
            include_public=include_public,
            limit=limit,
            offset=offset,
        )

    async def get_applicable_workflows(
        self,
        company_id: str,
        current_user: CurrentUser,
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        project_id: Optional[str] = None,
        include_public: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get applicable workflows for an agent, ordered by specificity.

        Resolution order (most to least specific):
        1. agent_id match
        2. role match
        3. project_id match
        4. company-level

        Args:
            company_id: Company UUID
            current_user: Authenticated user
            agent_id: Optional agent UUID
            role: Optional role
            project_id: Optional project UUID
            include_public: Include public workflows (default: True)

        Returns:
            List of applicable workflows ordered by specificity
        """
        # Get user's accessible projects
        accessible_projects = await self.get_user_accessible_projects(current_user, company_id)

        return await self.workflow_repo.get_applicable(
            company_id=company_id,
            current_user_id=current_user.user_id,
            accessible_project_ids=accessible_projects,
            agent_id=agent_id,
            role=role,
            project_id=project_id,
            include_public=include_public,
        )

    async def update_workflow_with_access_check(
        self, workflow_id: str, current_user: CurrentUser, company_id: str, **kwargs
    ) -> Dict[str, Any]:
        """
        Update workflow with visibility, modification checks, and scope change support.

        Accepts **kwargs from route using model_dump(exclude_unset=True) pattern.
        This allows distinguishing "field not sent" from "field sent as null".

        Security checks (in order):
        1. Get workflow and check visibility
        2. Check modification permission (ownership)
        3. Validate scope changes (FK validation, role validation)
        4. Check name uniqueness in target scope
        5. Proceed with update (increments version)

        Args:
            workflow_id: Workflow UUID
            current_user: Authenticated user
            company_id: Company UUID (for context)
            **kwargs: Fields from AgentWorkflowUpdate.model_dump(exclude_unset=True)
                - name, content, description, public, metadata, signals
                - project_id, agent_id, role (scope fields)

        Returns:
            Updated workflow dict

        Raises:
            HTTPException 404: Workflow not found or not visible
            HTTPException 403: Not owner
            HTTPException 400: Invalid FK, invalid role, or duplicate name
        """
        # 1. Get and check visibility
        workflow = await self.get_workflow_if_visible(workflow_id, current_user, company_id)
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

        # 2. Check modification permission
        allowed, error = can_modify_workflow(workflow, current_user)
        if not allowed:
            raise HTTPException(status_code=403, detail=error)

        # 3. Verify workflow belongs to company
        if str(workflow["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404, detail=f"Workflow {workflow_id} not found in company {company_id}"
            )

        # 4. Detect what scope fields were provided (sentinel pattern)
        project_id_provided = "project_id" in kwargs
        agent_id_provided = "agent_id" in kwargs
        role_provided = "role" in kwargs

        # 5. Calculate new scope values (or current if not provided)
        new_project_id = (
            kwargs.get("project_id") if project_id_provided else workflow.get("project_id")
        )
        new_agent_id = kwargs.get("agent_id") if agent_id_provided else workflow.get("agent_id")
        new_role = kwargs.get("role") if role_provided else workflow.get("role")

        # 6. FK Validation: project_id
        if project_id_provided and kwargs.get("project_id"):
            if not self.project_repo:
                raise HTTPException(status_code=500, detail="Project validation not available")
            project = await self.project_repo.get_by_id(kwargs["project_id"])
            if not project or str(project["company_id"]) != str(company_id):
                raise HTTPException(status_code=400, detail="Project not found in this company")

        # 7. FK Validation: agent_id
        if agent_id_provided and kwargs.get("agent_id"):
            if not self.agent_repo:
                raise HTTPException(status_code=500, detail="Agent validation not available")
            agent = await self.agent_repo.get_agent(kwargs["agent_id"])
            if not agent or str(agent["company_id"]) != str(company_id):
                raise HTTPException(status_code=400, detail="Agent not found in this company")

        # 8. Role Validation - REMOVED: roles are now dynamic per company
        # Future: validate against company_roles table when available

        # 9. Name Uniqueness in Target Scope
        new_name = kwargs.get("name") if "name" in kwargs else workflow["name"]

        # Check if scope is changing
        scope_changing = (
            (
                project_id_provided
                and str(new_project_id or "") != str(workflow.get("project_id") or "")
            )
            or (
                agent_id_provided and str(new_agent_id or "") != str(workflow.get("agent_id") or "")
            )
            or (role_provided and (new_role or "") != (workflow.get("role") or ""))
        )

        if "name" in kwargs or scope_changing:
            name_exists = await self.workflow_repo.check_name_exists(
                company_id=company_id,
                name=new_name,
                project_id=new_project_id,
                agent_id=new_agent_id,
                role=new_role,
                exclude_id=workflow_id,
            )

            if name_exists:
                raise HTTPException(
                    status_code=400, detail=f"Workflow '{new_name}' already exists in target scope"
                )

        # 10. Update workflow (increments version)
        updated_workflow = await self.workflow_repo.update(
            workflow_id=workflow_id,
            name=kwargs.get("name"),
            content=kwargs.get("content"),
            description=kwargs.get("description"),
            public=kwargs.get("public"),
            metadata=kwargs.get("metadata"),
            signals=kwargs.get("signals"),
            project_id_provided=project_id_provided,
            project_id=kwargs.get("project_id") if project_id_provided else None,
            agent_id_provided=agent_id_provided,
            agent_id=kwargs.get("agent_id") if agent_id_provided else None,
            role_provided=role_provided,
            role=kwargs.get("role") if role_provided else None,
            when_to_use=kwargs.get("when_to_use"),
        )

        if not updated_workflow:
            raise HTTPException(status_code=500, detail="Failed to update workflow")

        logger.info(f"Updated workflow {workflow_id} with access check")
        return updated_workflow

    async def delete_workflow_with_access_check(
        self, workflow_id: str, current_user: CurrentUser, company_id: str
    ) -> bool:
        """
        Delete workflow with visibility and modification checks.

        Security checks (in order):
        1. Get workflow and check visibility
        2. Check modification permission (ownership)
        3. Delete workflow

        Args:
            workflow_id: Workflow UUID
            current_user: Authenticated user
            company_id: Company UUID

        Returns:
            True if deleted

        Raises:
            HTTPException 404: Workflow not found or not visible
            HTTPException 403: Not owner
            HTTPException 500: Delete failure
        """
        # 1. Get and check visibility
        workflow = await self.get_workflow_if_visible(workflow_id, current_user, company_id)
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

        # 2. Check modification permission
        allowed, error = can_modify_workflow(workflow, current_user)
        if not allowed:
            raise HTTPException(status_code=403, detail=error)

        # 3. Verify workflow belongs to company
        if str(workflow["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404, detail=f"Workflow {workflow_id} not found in company {company_id}"
            )

        # 4. Delete workflow
        try:
            deleted = await self.workflow_repo.delete(workflow_id)
            if not deleted:
                raise HTTPException(status_code=500, detail="Failed to delete workflow")
            logger.info(f"Deleted workflow {workflow_id} with access check")
            return True
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to delete workflow {}: {}", workflow_id, e)
            raise HTTPException(status_code=500, detail="Failed to delete workflow")

    async def export_company_workflows(
        self,
        company_id: str,
        current_user: CurrentUser,
    ) -> Dict[str, Any]:
        """Export all workflows visible to the current user for a company."""
        workflows: List[Dict[str, Any]] = []
        offset = 0
        limit = 100

        while True:
            page = await self.list_workflows_visible(
                company_id=company_id,
                current_user=current_user,
                include_public=True,
                limit=limit,
                offset=offset,
            )
            page_items = page.get("workflows", [])
            if not page_items:
                break

            workflows.extend(page_items)
            offset += len(page_items)
            if offset >= page.get("total_count", 0):
                break

        workflows.sort(
            key=lambda w: (
                str(w.get("project_id", "")),
                str(w.get("name", "")),
                str(w.get("id", "")),
            )
        )

        return {
            "company_id": company_id,
            "exported_entity_type": "workflows",
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "total_items": len(workflows),
            "items": workflows,
        }
