"""
Company Repository
Database operations for companies.
"""

from typing import Optional, List, Dict, Any
from uuid import uuid4, UUID
from api.repositories.base_repository import BaseRepository
from loguru import logger


class CompanyRepository(BaseRepository):
    """Repository for company database operations."""

    async def create(self, name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new company.

        Args:
            name: Company name
            description: Company description

        Returns:
            Created company record
        """
        company_id = str(uuid4())

        query = """
            INSERT INTO companies (id, name, description, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id::text, name, description, is_active,
                      created_at, updated_at, cognee_enabled
        """

        row = await self.fetch_one(query, company_id, name, description, True)

        if not row:
            raise RuntimeError("Failed to create company")

        logger.info(f"Created company: {company_id} ({name})")
        return row

    async def add_user_to_company(self, company_id: str, user_id: str, role: str = "owner") -> None:
        """Add user to company."""
        query = """
          INSERT INTO company_users (company_id, user_id, role, joined_at)
          VALUES ($1, $2, $3, NOW())
      """
        await self.execute(query, company_id, user_id, role)

    async def get_by_id(self, company_id: str) -> Optional[Dict[str, Any]]:
        """
        Get company by ID.

        Args:
            company_id: Company UUID

        Returns:
            Company record or None if not found
        """
        query = """
            SELECT id::text, name, description, is_active,
                   created_at, updated_at, cognee_enabled
            FROM companies
            WHERE id = $1
        """

        return await self.fetch_one(query, company_id)

    async def is_cognee_enabled(self, company_id: str) -> bool:
        """
        Check whether cognee is enabled for a company.

        Args:
            company_id: Company UUID

        Returns:
            True if cognee_enabled is set, False otherwise
        """
        row = await self.fetch_one(
            "SELECT cognee_enabled FROM companies WHERE id = $1",
            company_id,
        )
        return bool(row["cognee_enabled"]) if row else False

    async def get_all(self) -> List[Dict[str, Any]]:
        """
        Get all companies.

        Returns:
            List of company records
        """
        query = """
            SELECT id::text, name, description, is_active,
                   created_at, updated_at, cognee_enabled
            FROM companies
            ORDER BY created_at DESC
        """

        return await self.fetch_all(query)

    async def get_by_user(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get companies that a user belongs to.

        Args:
            user_id: User UUID

        Returns:
            List of company records
        """
        query = """
            SELECT c.id::text, c.name, c.description, c.is_active,
                   c.created_at, c.updated_at, c.cognee_enabled
            FROM companies c
            INNER JOIN company_users cu ON c.id = cu.company_id
            WHERE cu.user_id = $1
            ORDER BY c.created_at DESC
        """

        return await self.fetch_all(query, user_id)

    async def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get company by name (case-insensitive).

        Args:
            name: Company name

        Returns:
            Company record or None if not found
        """
        query = """
            SELECT id::text, name, description, is_active,
                   created_at, updated_at
            FROM companies
            WHERE LOWER(name) = LOWER($1)
            LIMIT 1
        """

        return await self.fetch_one(query, name)

    async def exists(self, company_id: str) -> bool:
        """
        Check if company exists.

        Args:
            company_id: Company UUID

        Returns:
            True if company exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM companies WHERE id = $1)"
        return await self.fetch_val(query, company_id)

    async def user_has_access(self, user_id: str, company_id: str) -> bool:
        """
        Check if user has access to company.

        Args:
            user_id: User UUID
            company_id: Company UUID

        Returns:
            True if user has access, False otherwise
        """
        query = """
            SELECT EXISTS(
                SELECT 1 FROM company_users
                WHERE company_id = $1 AND user_id = $2
            )
        """

        return await self.fetch_val(query, company_id, user_id)

    async def count_by_user(self, user_id: str) -> int:
        """
        Count companies a user belongs to.

        Args:
            user_id: User UUID

        Returns:
            Number of companies the user is a member of
        """
        query = """
            SELECT COUNT(*)
            FROM company_users
            WHERE user_id = $1
        """
        result = await self.fetch_val(query, user_id)
        return result or 0

    async def delete(self, company_id: str) -> None:
        """
        Delete a company (for rollback operations).

        Args:
            company_id: Company UUID
        """
        query = "DELETE FROM companies WHERE id = $1"
        await self.execute(query, company_id)
        logger.info(f"Deleted company: {company_id}")
