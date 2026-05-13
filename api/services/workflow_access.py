"""
Workflow Access Control Module

Provides reusable functions for workflow visibility and modification checks.
Centralized logic for determining who can see and modify agent workflows.

Visibility Rules:
1. User owns the workflow (workflow.user_id == current_user.user_id)
2. Workflow is public AND user has access to workflow's company
3. Workflow is public AND user has access to workflow's project

Modification Rules:
1. System workflows (user_id=NULL) require admin/superuser
2. Only the owner can modify their workflows
"""

from typing import Optional, List, Dict, Any, Tuple
from api.auth import CurrentUser


def is_workflow_visible(
    workflow: Dict[str, Any],
    current_user: CurrentUser,
    accessible_project_ids: Optional[List[str]] = None
) -> bool:
    """
    Check if a workflow is visible to the current user.

    A workflow is visible if ANY of these conditions are met:
    1. User owns the workflow (workflow.user_id == current_user.user_id)
    2. Workflow is public AND user has access to workflow's company
    3. Workflow is public AND user has access to workflow's project

    Args:
        workflow: Workflow dict with user_id, public, company_id, project_id
        current_user: Authenticated user from JWT
        accessible_project_ids: User's accessible projects (optional)

    Returns:
        True if workflow is visible to user, False otherwise
    """
    # Superusers see everything
    if current_user.is_superuser:
        return True

    workflow_user_id = workflow.get("user_id")
    workflow_company_id = str(workflow.get("company_id", ""))
    user_companies = [str(c) for c in current_user.companies]

    # Rule 1: Ownership — str coercion guards against UUID/string type mismatches
    if workflow_user_id is not None and str(workflow_user_id) == str(current_user.user_id):
        return True

    # Rule 2: System workflow (user_id=NULL) — visible to all company members
    if workflow_user_id is None and workflow_company_id in user_companies:
        return True

    if not workflow.get("public", False):
        return False

    # Rule 3: Public + Same Company
    if workflow_company_id and workflow_company_id in user_companies:
        return True

    # Rule 4: Public + Same Project
    workflow_project_id = workflow.get("project_id")
    if workflow_project_id and accessible_project_ids:
        if str(workflow_project_id) in [str(p) for p in accessible_project_ids]:
            return True

    return False


def can_modify_workflow(
    workflow: Dict[str, Any],
    current_user: CurrentUser
) -> Tuple[bool, Optional[str]]:
    """
    Check if the current user can modify a workflow.

    Modification rules:
    1. If workflow has no owner (user_id=NULL), only admins can modify
    2. Otherwise, only the owner can modify

    Args:
        workflow: Workflow dict with user_id
        current_user: Authenticated user from JWT

    Returns:
        Tuple of (can_modify: bool, error_reason: Optional[str])
    """
    # Superusers can modify anything
    if current_user.is_superuser:
        return True, None

    # Rule 1: System workflows (user_id=NULL) require admin
    workflow_user_id = workflow.get("user_id")
    if workflow_user_id is None:
        return False, "System workflows can only be modified by administrators"

    # Rule 2: Only owner can modify — str coercion guards UUID/string mismatches
    if str(workflow_user_id) != str(current_user.user_id):
        return False, "Only the workflow owner can modify this workflow"

    return True, None


def get_user_company_ids(current_user: CurrentUser) -> List[str]:
    """
    Extract company IDs from current user.

    Args:
        current_user: Authenticated user from JWT

    Returns:
        List of company UUID strings the user has access to
    """
    return current_user.companies or []
