"""
Branch Repository
Database operations for branches.
"""

from typing import Optional, List, Dict, Any
from uuid import uuid4, UUID
from api.repositories.base_repository import BaseRepository
from loguru import logger


class BranchRepository(BaseRepository):
    """Repository for branch database operations."""

    async def create(
        self,
        repository_id: str,
        name: str,
        commit_sha: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new branch under a repository.

        Args:
            repository_id: Repository UUID
            name: Branch name
            commit_sha: Commit SHA

        Returns:
            Created branch record
        """
        branch_id = str(uuid4())

        query = """
            INSERT INTO branches (id, repository_id, name, commit_sha, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id::text, repository_id::text, name, commit_sha,
                      created_at, updated_at
        """

        row = await self.fetch_one(
            query,
            UUID(branch_id),
            self.parse_uuid(repository_id),
            name,
            commit_sha
        )

        if not row:
            raise RuntimeError("Failed to create branch")

        logger.info(f"Created branch: {branch_id} ({name}) under repository {repository_id}")
        return row

    async def get_by_id(self, branch_id: str) -> Optional[Dict[str, Any]]:
        """
        Get branch by ID.

        Args:
            branch_id: Branch UUID

        Returns:
            Branch record or None if not found
        """
        query = """
            SELECT id::text, repository_id::text, name, commit_sha,
                   created_at, updated_at
            FROM branches
            WHERE id = $1
        """

        return await self.fetch_one(query, self.parse_uuid(branch_id))

    async def get_by_repository(self, repository_id: str) -> List[Dict[str, Any]]:
        """
        Get all branches for a repository.

        Args:
            repository_id: Repository UUID

        Returns:
            List of branch records
        """
        query = """
            SELECT id::text, repository_id::text, name, commit_sha,
                   created_at, updated_at
            FROM branches
            WHERE repository_id = $1
            ORDER BY created_at DESC
        """

        return await self.fetch_all(query, self.parse_uuid(repository_id))

    async def exists(self, branch_id: str) -> bool:
        """
        Check if branch exists.

        Args:
            branch_id: Branch UUID

        Returns:
            True if branch exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM branches WHERE id = $1)"
        return await self.fetch_val(query, self.parse_uuid(branch_id))

    async def get_repository_id(self, branch_id: str) -> Optional[str]:
        """
        Get repository_id for a branch.

        Args:
            branch_id: Branch UUID

        Returns:
            Repository UUID or None if branch not found
        """
        query = "SELECT repository_id::text FROM branches WHERE id = $1"
        return await self.fetch_val(query, self.parse_uuid(branch_id))

    async def delete(self, branch_id: str) -> None:
        """
        Delete a branch (for rollback purposes).

        Args:
            branch_id: Branch UUID
        """
        query = "DELETE FROM branches WHERE id = $1"
        await self.execute(query, self.parse_uuid(branch_id))
        logger.info(f"Deleted branch: {branch_id}")
