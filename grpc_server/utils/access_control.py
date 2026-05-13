"""
gRPC Access Control Wrapper
Adapts api/services/agent_access.py functions for gRPC servicers.

This module wraps the core access control functions (is_agent_visible, can_modify_agent)
for gRPC context usage, handling project list retrieval for visibility checks.
"""

from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
from api.auth import CurrentUser
from api.services.agent_access import is_agent_visible, can_modify_agent


async def get_accessible_projects_for_grpc(
    project_repo,
    current_user: CurrentUser,
    company_id: str
) -> List[str]:
    """
    Get accessible projects for gRPC context.

    For now, returns all projects in the company. This matches the REST API pattern
    where company membership implies project access.

    Args:
        project_repo: ProjectRepository instance
        current_user: Authenticated user from gRPC context
        company_id: Company UUID

    Returns:
        List of project UUIDs user can access
    """
    if not project_repo or not company_id:
        return []

    try:
        # Get all projects for the company
        projects = await project_repo.get_by_company(company_id)
        return [p["id"] for p in projects]
    except Exception as e:
        logger.warning(f"Failed to get projects for company {company_id}: {e}")
        return []


def check_agent_visibility(
    agent: Dict[str, Any],
    current_user: CurrentUser,
    accessible_project_ids: List[str]
) -> Tuple[bool, Optional[str]]:
    """
    Check if agent is visible to user.

    Wraps is_agent_visible() for gRPC servicer use.

    Args:
        agent: Agent dict
        current_user: Authenticated user
        accessible_project_ids: User's accessible projects

    Returns:
        (is_visible, error_code) - error_code is "NOT_FOUND" if not visible
    """
    if is_agent_visible(agent, current_user, accessible_project_ids):
        return True, None
    return False, "NOT_FOUND"


def check_agent_modification(
    agent: Dict[str, Any],
    current_user: CurrentUser
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Check if user can modify agent.

    Wraps can_modify_agent() for gRPC servicer use.

    Args:
        agent: Agent dict
        current_user: Authenticated user

    Returns:
        (can_modify, error_message, error_code)
    """
    can_modify, reason = can_modify_agent(agent, current_user)
    if can_modify:
        return True, None, None
    return False, reason, "PERMISSION_DENIED"
