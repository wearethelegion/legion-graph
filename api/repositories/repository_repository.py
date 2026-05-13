"""
Repository Repository
Database operations for repositories.
"""

from typing import Optional, List, Dict, Any
from uuid import uuid4, UUID
from api.repositories.base_repository import BaseRepository
from loguru import logger


class RepositoryRepository(BaseRepository):
    """Repository for repository database operations."""

    async def create(
        self,
        project_id: str,
        name: str,
        url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new repository under a project.

        Args:
            project_id: Project UUID
            name: Repository name
            url: Repository URL

        Returns:
            Created repository record
        """
        repository_id = str(uuid4())

        query = """
            INSERT INTO repositories (id, project_id, name, url, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id::text, project_id::text, name, url,
                      created_at, updated_at
        """

        row = await self.fetch_one(
            query,
            UUID(repository_id),
            self.parse_uuid(project_id),
            name,
            url
        )

        if not row:
            raise RuntimeError("Failed to create repository")

        logger.info(f"Created repository: {repository_id} ({name}) under project {project_id}")
        return row

    async def get_by_id(self, repository_id: str) -> Optional[Dict[str, Any]]:
        """
        Get repository by ID.

        Args:
            repository_id: Repository UUID

        Returns:
            Repository record or None if not found
        """
        query = """
            SELECT id::text, project_id::text, name, url,
                   created_at, updated_at
            FROM repositories
            WHERE id = $1
        """

        return await self.fetch_one(query, self.parse_uuid(repository_id))

    async def get_by_project(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get all repositories for a project.

        Args:
            project_id: Project UUID

        Returns:
            List of repository records
        """
        query = """
            SELECT id::text, project_id::text, name, url,
                   created_at, updated_at
            FROM repositories
            WHERE project_id = $1
            ORDER BY created_at DESC
        """

        return await self.fetch_all(query, self.parse_uuid(project_id))

    async def exists(self, repository_id: str) -> bool:
        """
        Check if repository exists.

        Args:
            repository_id: Repository UUID

        Returns:
            True if repository exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM repositories WHERE id = $1)"
        return await self.fetch_val(query, self.parse_uuid(repository_id))

    async def get_project_id(self, repository_id: str) -> Optional[str]:
        """
        Get project_id for a repository.

        Args:
            repository_id: Repository UUID

        Returns:
            Project UUID or None if repository not found
        """
        query = "SELECT project_id::text FROM repositories WHERE id = $1"
        return await self.fetch_val(query, self.parse_uuid(repository_id))

    async def delete(self, repository_id: str) -> None:
        """
        Delete a repository (for rollback purposes).

        Args:
            repository_id: Repository UUID
        """
        query = "DELETE FROM repositories WHERE id = $1"
        await self.execute(query, self.parse_uuid(repository_id))
        logger.info(f"Deleted repository: {repository_id}")
