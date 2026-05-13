"""
Agent Access Control Module

Provides reusable functions for agent visibility and modification checks.
Centralized logic for determining who can see and modify agents.

Visibility Rules:
1. User owns the agent (agent.user_id == current_user.user_id)
2. Agent is public AND user has access to agent's company
3. Agent is public AND user has access to agent's project

Modification Rules:
1. Sealed agents cannot be modified by anyone (except unsealing via admin)
2. System agents (user_id=NULL) require admin/superuser
3. Only the owner can modify their agents
"""

from typing import Optional, List, Dict, Any, Tuple
from api.auth import CurrentUser


def is_agent_visible(
    agent: Dict[str, Any],
    current_user: CurrentUser,
    accessible_project_ids: Optional[List[str]] = None
) -> bool:
    """
    Check if an agent is visible to the current user.

    An agent is visible if ANY of these conditions are met:
    1. User owns the agent (agent.user_id == current_user.user_id)
    2. Agent is public AND user has access to agent's company
    3. Agent is public AND user has access to agent's project

    Args:
        agent: Agent dict with user_id, public, company_id, project_id
        current_user: Authenticated user from JWT
        accessible_project_ids: User's accessible projects (optional)

    Returns:
        True if agent is visible to user, False otherwise
    """
    # Superusers see everything
    if current_user.is_superuser:
        return True

    # Rule 1: Ownership - owner always sees their agents
    agent_user_id = agent.get("user_id")
    if agent_user_id is not None and agent_user_id == current_user.user_id:
        return True

    # For public visibility, check company access
    if not agent.get("public", False):
        return False

    # Rule 2: Public + Same Company
    agent_company_id = agent.get("company_id")
    if agent_company_id and agent_company_id in current_user.companies:
        return True

    # Rule 3: Public + Same Project
    agent_project_id = agent.get("project_id")
    if agent_project_id and accessible_project_ids:
        if agent_project_id in accessible_project_ids:
            return True

    return False


def can_modify_agent(
    agent: Dict[str, Any],
    current_user: CurrentUser
) -> Tuple[bool, Optional[str]]:
    """
    Check if the current user can modify an agent.

    Modification rules:
    1. If agent is sealed, no one can modify (except unsealing via admin)
    2. If agent has no owner (user_id=NULL), only admins can modify
    3. Otherwise, only the owner can modify

    Args:
        agent: Agent dict with user_id, sealed
        current_user: Authenticated user from JWT

    Returns:
        Tuple of (can_modify: bool, error_reason: Optional[str])
    """
    # Rule 1: Sealed agents cannot be modified
    if agent.get("sealed", False):
        return False, "Agent is sealed and cannot be modified"

    # Superusers can modify anything (except sealed)
    if current_user.is_superuser:
        return True, None

    # Rule 2: System agents (user_id=NULL) require admin
    agent_user_id = agent.get("user_id")
    if agent_user_id is None:
        return False, "System agents can only be modified by administrators"

    # Rule 3: Only owner can modify
    if agent_user_id != current_user.user_id:
        return False, "Only the agent owner can modify this agent"

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
