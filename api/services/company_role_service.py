"""
Company Role Service
Business logic for company role management.

Per architecture plan (entry 733d587e-3a7b-48a0-88e0-d48f9298cfd3):
- ensure_defaults_exist for new companies
- validate_role_exists for agent assignment
- System roles cannot be deleted
- Roles with assigned agents cannot be deleted
"""

from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from loguru import logger

from api.repositories.company_role_repository import CompanyRoleRepository


class CompanyRoleService:
    """Service for company role management operations."""

    def __init__(self, role_repo: CompanyRoleRepository):
        self.role_repo = role_repo

    async def list_roles(
        self,
        company_id: str,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List roles for a company with pagination.

        Args:
            company_id: Company UUID
            active_only: If True, only return active roles
            limit: Maximum results
            offset: Skip first N results

        Returns:
            Dict with 'roles' and 'total'
        """
        roles = await self.role_repo.list_by_company(
            company_id=company_id,
            active_only=active_only,
            limit=limit,
            offset=offset
        )
        total = await self.role_repo.count_by_company(
            company_id=company_id,
            active_only=active_only
        )

        return {
            "roles": roles,
            "total": total
        }

    async def get_role(self, role_id: str) -> Optional[Dict[str, Any]]:
        """
        Get role by ID.

        Args:
            role_id: Role UUID

        Returns:
            Role dict or None
        """
        return await self.role_repo.get_by_id(role_id)

    async def get_role_by_name(
        self,
        company_id: str,
        name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get role by company and name.

        Args:
            company_id: Company UUID
            name: Role name

        Returns:
            Role dict or None
        """
        return await self.role_repo.get_by_name(company_id, name)

    async def create_role(
        self,
        company_id: str,
        name: str,
        display_name: str,
        description: Optional[str] = None,
        color: Optional[str] = None,
        icon: Optional[str] = None,
        sort_order: int = 0
    ) -> Dict[str, Any]:
        """
        Create a new company role.

        Args:
            company_id: Company UUID
            name: Role identifier
            display_name: Human-readable name
            description: Optional description
            color: Optional hex color
            icon: Optional icon identifier
            sort_order: UI ordering

        Returns:
            Created role dict

        Raises:
            HTTPException 400: Duplicate role name
        """
        # Check for duplicate name
        existing = await self.role_repo.get_by_name(company_id, name)
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Role with name '{name}' already exists in this company"
            )

        role = await self.role_repo.create(
            company_id=company_id,
            name=name,
            display_name=display_name,
            description=description,
            color=color,
            icon=icon,
            is_system=False,  # User-created roles are never system roles
            sort_order=sort_order
        )

        logger.info(f"Created role '{name}' for company {company_id}")
        return role

    async def update_role(
        self,
        role_id: str,
        company_id: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        color: Optional[str] = None,
        icon: Optional[str] = None,
        is_active: Optional[bool] = None,
        sort_order: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Update a company role.

        Note: name and is_system are immutable.

        Args:
            role_id: Role UUID
            company_id: Company UUID (for validation)
            display_name: Optional new display name
            description: Optional new description
            color: Optional new color
            icon: Optional new icon
            is_active: Optional new active status
            sort_order: Optional new sort order

        Returns:
            Updated role dict

        Raises:
            HTTPException 404: Role not found
            HTTPException 403: Role belongs to different company
        """
        # Get and validate role
        role = await self.role_repo.get_by_id(role_id)
        if not role:
            raise HTTPException(
                status_code=404,
                detail=f"Role {role_id} not found"
            )

        if str(role["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404,
                detail=f"Role {role_id} not found in company {company_id}"
            )

        # Update role
        updated = await self.role_repo.update(
            role_id=role_id,
            display_name=display_name,
            description=description,
            color=color,
            icon=icon,
            is_active=is_active,
            sort_order=sort_order
        )

        if not updated:
            raise HTTPException(
                status_code=500,
                detail="Failed to update role"
            )

        logger.info(f"Updated role {role_id}")
        return updated

    async def delete_role(self, role_id: str, company_id: str) -> bool:
        """
        Delete a company role.

        Validates:
        - Role exists and belongs to company
        - Role is not a system role
        - Role has no assigned agents

        Args:
            role_id: Role UUID
            company_id: Company UUID

        Returns:
            True if deleted

        Raises:
            HTTPException 404: Role not found
            HTTPException 403: System role or has assigned agents
        """
        # Get and validate role
        role = await self.role_repo.get_by_id(role_id)
        if not role:
            raise HTTPException(
                status_code=404,
                detail=f"Role {role_id} not found"
            )

        if str(role["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404,
                detail=f"Role {role_id} not found in company {company_id}"
            )

        # Check if system role
        if role.get("is_system", False):
            raise HTTPException(
                status_code=403,
                detail="System roles cannot be deleted. Deactivate instead."
            )

        # Check for assigned agents
        agents_count = await self.role_repo.get_agents_count_by_role(role_id)
        if agents_count > 0:
            raise HTTPException(
                status_code=403,
                detail=f"Cannot delete role with {agents_count} assigned agent(s). Reassign or deactivate instead."
            )

        # Delete role
        deleted = await self.role_repo.delete(role_id)
        if not deleted:
            raise HTTPException(
                status_code=500,
                detail="Failed to delete role"
            )

        logger.info(f"Deleted role {role_id}")
        return True

    async def ensure_defaults_exist(self, company_id: str) -> List[Dict[str, Any]]:
        """
        Ensure default roles exist for a company.

        Called when:
        - New company created
        - Admin requests re-seed

        Idempotent - skips existing roles.

        Args:
            company_id: Company UUID

        Returns:
            List of newly created roles (excludes already existing)
        """
        created = await self.role_repo.seed_defaults(company_id)
        logger.info(f"Ensured defaults for company {company_id}: {len(created)} created")
        return created

    async def validate_role_exists(
        self,
        company_id: str,
        role_id: str
    ) -> bool:
        """
        Validate a role exists and is active for agent assignment.

        Args:
            company_id: Company UUID
            role_id: Role UUID to validate

        Returns:
            True if role exists, is active, and belongs to company

        Raises:
            HTTPException 400: Role not found, inactive, or wrong company
        """
        role = await self.role_repo.get_by_id(role_id)

        if not role:
            raise HTTPException(
                status_code=400,
                detail=f"Role {role_id} not found"
            )

        if str(role["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=400,
                detail=f"Role {role_id} does not belong to company {company_id}"
            )

        if not role.get("is_active", True):
            raise HTTPException(
                status_code=400,
                detail=f"Role '{role['name']}' is inactive"
            )

        return True
