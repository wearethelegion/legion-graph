"""
Company Role Repository
Database operations for company roles.

Per architecture plan (entry 733d587e-3a7b-48a0-88e0-d48f9298cfd3):
- CRUD operations for company_roles table
- seed_defaults for new companies
- System roles cannot be deleted
"""

from typing import List, Optional, Dict, Any
from uuid import uuid4
from api.repositories.base_repository import BaseRepository
from loguru import logger


# Default roles seeded for every new company (from architecture plan)
DEFAULT_ROLES = [
    {"name": "orchestrator", "display_name": "Orchestrator", "color": "#6366F1", "is_system": True, "sort_order": 0},
    {"name": "researcher", "display_name": "Researcher", "color": "#8B5CF6", "is_system": True, "sort_order": 1},
    {"name": "architect", "display_name": "Architect", "color": "#EC4899", "is_system": True, "sort_order": 2},
    {"name": "developer", "display_name": "Developer", "color": "#10B981", "is_system": True, "sort_order": 3},
    {"name": "ui-developer", "display_name": "UI Developer", "color": "#F59E0B", "is_system": True, "sort_order": 4},
    {"name": "protector", "display_name": "Protector", "color": "#EF4444", "is_system": True, "sort_order": 5},
    {"name": "test-automation-engineer", "display_name": "Test Automation Engineer", "color": "#06B6D4", "is_system": True, "sort_order": 6},
    {"name": "prompt-engineer", "display_name": "Prompt Engineer", "color": "#84CC16", "is_system": True, "sort_order": 7},
    {"name": "systems-architect", "display_name": "Systems Architect", "color": "#F97316", "is_system": True, "sort_order": 8},
    {"name": "presenter", "display_name": "Presenter", "color": "#A855F7", "is_system": True, "sort_order": 9},
    {"name": "technical-writer", "display_name": "Technical Writer", "color": "#14B8A6", "is_system": False, "sort_order": 10},
]


class CompanyRoleRepository(BaseRepository):
    """Repository for company role operations."""

    # Column list for SELECT statements
    _ROLE_COLUMNS = """
        id, company_id, name, display_name, description,
        color, icon, is_system, is_active, sort_order,
        created_at, updated_at
    """

    async def list_by_company(
        self,
        company_id: str,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List roles for a company.

        Args:
            company_id: Company UUID
            active_only: If True, only return active roles
            limit: Maximum number of records
            offset: Number of records to skip

        Returns:
            List of role records
        """
        if active_only:
            query = f"""
                SELECT {self._ROLE_COLUMNS}
                FROM company_roles
                WHERE company_id = $1 AND is_active = true
                ORDER BY sort_order, name
                LIMIT $2 OFFSET $3
            """
        else:
            query = f"""
                SELECT {self._ROLE_COLUMNS}
                FROM company_roles
                WHERE company_id = $1
                ORDER BY sort_order, name
                LIMIT $2 OFFSET $3
            """
        return await self.fetch_all(query, company_id, limit, offset)

    async def count_by_company(
        self,
        company_id: str,
        active_only: bool = True
    ) -> int:
        """
        Count roles for a company.

        Args:
            company_id: Company UUID
            active_only: If True, only count active roles

        Returns:
            Number of roles
        """
        if active_only:
            query = "SELECT COUNT(*) FROM company_roles WHERE company_id = $1 AND is_active = true"
        else:
            query = "SELECT COUNT(*) FROM company_roles WHERE company_id = $1"
        return await self.fetch_val(query, company_id)

    async def get_by_id(self, role_id: str) -> Optional[Dict[str, Any]]:
        """
        Get role by ID.

        Args:
            role_id: Role UUID

        Returns:
            Role record or None if not found
        """
        query = f"""
            SELECT {self._ROLE_COLUMNS}
            FROM company_roles
            WHERE id = $1
        """
        return await self.fetch_one(query, role_id)

    async def get_by_name(
        self,
        company_id: str,
        name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get role by company and name.

        Args:
            company_id: Company UUID
            name: Role name (e.g., 'developer')

        Returns:
            Role record or None if not found
        """
        query = f"""
            SELECT {self._ROLE_COLUMNS}
            FROM company_roles
            WHERE company_id = $1 AND name = $2
        """
        return await self.fetch_one(query, company_id, name)

    async def create(
        self,
        company_id: str,
        name: str,
        display_name: str,
        description: Optional[str] = None,
        color: Optional[str] = None,
        icon: Optional[str] = None,
        is_system: bool = False,
        sort_order: int = 0
    ) -> Dict[str, Any]:
        """
        Create a new company role.

        Args:
            company_id: Company UUID
            name: Role identifier (lowercase, hyphen-separated)
            display_name: Human-readable name
            description: Optional description
            color: Optional hex color
            icon: Optional icon identifier
            is_system: If True, role cannot be deleted
            sort_order: UI ordering

        Returns:
            Created role record
        """
        role_id = str(uuid4())

        query = """
            INSERT INTO company_roles (
                id, company_id, name, display_name, description,
                color, icon, is_system, is_active, sort_order,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, $9, NOW(), NOW())
            RETURNING id, company_id, name, display_name, description,
                      color, icon, is_system, is_active, sort_order,
                      created_at, updated_at
        """

        row = await self.fetch_one(
            query,
            role_id,
            company_id,
            name,
            display_name,
            description,
            color,
            icon,
            is_system,
            sort_order
        )

        if not row:
            raise RuntimeError("Failed to create company role")

        logger.info(f"Created company role: {role_id} ({name}) for company {company_id}")
        return row

    async def update(
        self,
        role_id: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        color: Optional[str] = None,
        icon: Optional[str] = None,
        is_active: Optional[bool] = None,
        sort_order: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update role with COALESCE semantics.

        Note: name and is_system are immutable.

        Args:
            role_id: Role UUID
            display_name: Optional new display name
            description: Optional new description
            color: Optional new color
            icon: Optional new icon
            is_active: Optional new active status
            sort_order: Optional new sort order

        Returns:
            Updated role record or None if not found
        """
        query = f"""
            UPDATE company_roles
            SET display_name = COALESCE($2, display_name),
                description = COALESCE($3, description),
                color = COALESCE($4, color),
                icon = COALESCE($5, icon),
                is_active = COALESCE($6, is_active),
                sort_order = COALESCE($7, sort_order),
                updated_at = NOW()
            WHERE id = $1
            RETURNING {self._ROLE_COLUMNS}
        """

        row = await self.fetch_one(
            query,
            role_id,
            display_name,
            description,
            color,
            icon,
            is_active,
            sort_order
        )

        if row:
            logger.info(f"Updated company role: {role_id}")

        return row

    async def delete(self, role_id: str) -> bool:
        """
        Delete a role.

        Note: Will fail if is_system=true (handled at service layer).

        Args:
            role_id: Role UUID

        Returns:
            True if deleted, False otherwise
        """
        query = "DELETE FROM company_roles WHERE id = $1"
        result = await self.execute(query, role_id)
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted company role: {role_id}")

        return deleted

    async def seed_defaults(self, company_id: str) -> List[Dict[str, Any]]:
        """
        Seed default roles for a company.

        Skips roles that already exist (idempotent).

        Args:
            company_id: Company UUID

        Returns:
            List of created role records (excludes already existing)
        """
        created_roles = []

        for role_data in DEFAULT_ROLES:
            # Check if role already exists
            existing = await self.get_by_name(company_id, role_data["name"])
            if existing:
                logger.debug(f"Role {role_data['name']} already exists for company {company_id}")
                continue

            # Create role
            role = await self.create(
                company_id=company_id,
                name=role_data["name"],
                display_name=role_data["display_name"],
                color=role_data.get("color"),
                is_system=role_data.get("is_system", False),
                sort_order=role_data.get("sort_order", 0)
            )
            created_roles.append(role)

        logger.info(f"Seeded {len(created_roles)} default roles for company {company_id}")
        return created_roles

    async def get_agents_count_by_role(self, role_id: str) -> int:
        """
        Count agents assigned to a role.

        Used to check if role can be deleted (shouldn't delete with assigned agents).

        Note: This queries agents table via role_id FK (added in migration).

        Args:
            role_id: Role UUID

        Returns:
            Number of agents with this role
        """
        query = "SELECT COUNT(*) FROM agents WHERE role_id = $1"
        return await self.fetch_val(query, role_id)
