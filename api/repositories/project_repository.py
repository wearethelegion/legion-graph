"""
Project Repository
Database operations for projects.
"""

from typing import Optional, List, Dict, Any
from uuid import uuid4
import secrets
import re
from api.repositories.base_repository import BaseRepository
from loguru import logger


class ProjectRepository(BaseRepository):
    """Repository for project database operations."""

    @staticmethod
    def _slugify(text: str) -> str:
        """
        Convert text to URL-safe slug.

        Examples:
            "ACME Corp!" → "acme-corp"
            "My Project 2.0" → "my-project-2-0"
        """
        text = text.lower()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s_]+", "-", text)
        text = re.sub(r"-+", "-", text)
        return text.strip("-")

    async def create(
        self,
        company_id: str,
        name: str,
        description: Optional[str] = None,
        github_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new project under a company with auto-generated webhook config.

        Args:
            company_id: Company UUID
            name: Project name
            description: Project description
            github_token: Optional GitHub personal access token

        Returns:
            Created project record with webhook fields
        """
        project_id = str(uuid4())

        # Auto-generate webhook secret
        github_webhook_secret = secrets.token_urlsafe(32)

        # Get company name for webhook URL generation
        company_query = "SELECT name FROM companies WHERE id = $1"
        company_row = await self.fetch_one(company_query, company_id)
        if not company_row:
            raise RuntimeError(f"Company not found: {company_id}")

        # Generate webhook URL
        company_slug = self._slugify(company_row["name"])
        project_slug = self._slugify(name)
        webhook_url = f"/api/v1/webhooks/github/{company_slug}/{project_slug}"

        query = """
            INSERT INTO projects (
                id, company_id, name, description,
                github_webhook_secret, github_token, webhook_url,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
            RETURNING id::text, company_id::text, name, description,
                      github_webhook_secret, github_token, webhook_url,
                      created_at, updated_at
        """

        row = await self.fetch_one(
            query,
            project_id,
            company_id,
            name,
            description,
            github_webhook_secret,
            github_token,
            webhook_url,
        )

        if not row:
            raise RuntimeError("Failed to create project")

        logger.info(f"Created project: {project_id} ({name}) under company {company_id}")
        logger.info(f"Generated webhook URL: {webhook_url}")
        return row

    async def get_by_id(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get project by ID.

        Args:
            project_id: Project UUID

        Returns:
            Project record or None if not found
        """
        query = """
            SELECT id::text, company_id::text, name, description,
                   github_webhook_secret, github_token, webhook_url,
                   created_at, updated_at
            FROM projects
            WHERE id = $1
        """

        return await self.fetch_one(query, project_id)

    async def get_by_name_and_company(self, name: str, company_id: str) -> Optional[Dict[str, Any]]:
        """
        Get project by name and company (case-insensitive).

        Args:
            name: Project name
            company_id: Company UUID

        Returns:
            Project record with webhook fields or None
        """
        query = """
            SELECT id::text, company_id::text, name, description,
                   github_webhook_secret, github_token, webhook_url,
                   created_at, updated_at
            FROM projects
            WHERE LOWER(name) = LOWER($1)
              AND company_id = $2
            LIMIT 1
        """

        return await self.fetch_one(query, name, company_id)

    async def get_by_company(self, company_id: str) -> List[Dict[str, Any]]:
        """
        Get all projects for a company.

        Args:
            company_id: Company UUID

        Returns:
            List of project records with company_name
        """
        query = """
            SELECT p.id::text, p.company_id::text, p.name, p.description,
                   p.github_webhook_secret, p.github_token, p.webhook_url,
                   p.created_at, p.updated_at,
                   c.name AS company_name, c.cognee_enabled
            FROM projects p
            JOIN companies c ON p.company_id = c.id
            WHERE p.company_id = $1
            ORDER BY p.created_at DESC
        """

        return await self.fetch_all(query, company_id)

    async def get_all(self) -> List[Dict[str, Any]]:
        """
        Get all projects across all companies (superuser access).

        Returns:
            List of all project records with company_name
        """
        query = """
            SELECT p.id::text, p.company_id::text, p.name, p.description,
                   p.github_webhook_secret, p.github_token, p.webhook_url,
                   p.created_at, p.updated_at,
                   c.name AS company_name, c.cognee_enabled
            FROM projects p
            JOIN companies c ON p.company_id = c.id
            ORDER BY p.created_at DESC
        """

        return await self.fetch_all(query)

    async def count_by_user(self, user_id: str) -> int:
        """
        Count total projects accessible to a user across all their companies.

        Args:
            user_id: User UUID

        Returns:
            Total project count across all companies the user belongs to
        """
        query = """
            SELECT COUNT(DISTINCT p.id)
            FROM projects p
            INNER JOIN company_users cu ON p.company_id = cu.company_id
            WHERE cu.user_id = $1
        """
        result = await self.fetch_val(query, user_id)
        return result or 0

    async def count_by_company(self, company_id: str) -> int:
        """
        Count projects in a company.

        Args:
            company_id: Company UUID

        Returns:
            Number of projects in the company
        """
        query = "SELECT COUNT(*) FROM projects WHERE company_id = $1"
        result = await self.fetch_val(query, company_id)
        return result or 0

    async def exists(self, project_id: str) -> bool:
        """
        Check if project exists.

        Args:
            project_id: Project UUID

        Returns:
            True if project exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM projects WHERE id = $1)"
        return await self.fetch_val(query, project_id)

    async def get_company_id(self, project_id: str) -> Optional[str]:
        """
        Get company_id for a project.

        Args:
            project_id: Project UUID

        Returns:
            Company UUID or None if project not found
        """
        query = "SELECT company_id::text FROM projects WHERE id = $1"
        return await self.fetch_val(query, project_id)

    async def delete(self, project_id: str) -> None:
        """
        Delete a project (for rollback purposes).

        Args:
            project_id: Project UUID
        """
        query = "DELETE FROM projects WHERE id = $1"
        await self.execute(query, project_id)
        logger.info(f"Deleted project: {project_id}")

    async def transfer_company_scope(
        self, project_id: str, source_company_id: str, target_company_id: str
    ) -> Dict[str, Any]:
        """
        Transfer project ownership and project-scoped company_id fields.

        This updates:
        - projects.company_id + projects.webhook_url
        - Any table in public schema that has BOTH project_id and company_id

        Args:
            project_id: Project UUID
            source_company_id: Current company UUID expected for the project
            target_company_id: Destination company UUID

        Returns:
            Migration stats by table

        Raises:
            RuntimeError: If project doesn't exist or source mismatch
        """
        if source_company_id == target_company_id:
            return {
                "project_updated": False,
                "project_company_id": source_company_id,
                "updated_tables": {},
            }

        # noqa: KGRAG-RAW-CONN — transfer_company() uses raw conn within a transaction.
        # All parameters are UUIDs, schema-introspected table names, and slugified
        # company/project names derived from DB reads — no user-supplied free-text strings.
        # sanitize_text() is intentionally omitted here; no UTF-8 corruption risk.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Lock project row for transfer safety
                project_row = await conn.fetchrow(
                    """
                    SELECT p.id::text AS id,
                           p.company_id::text AS company_id,
                           p.name,
                           c.name AS source_company_name
                    FROM projects p
                    JOIN companies c ON c.id = p.company_id
                    WHERE p.id = $1
                    FOR UPDATE
                    """,
                    project_id,
                )

                if not project_row:
                    raise RuntimeError(f"Project {project_id} not found")

                actual_source_company_id = project_row["company_id"]
                if actual_source_company_id != source_company_id:
                    raise RuntimeError(
                        "Project source company mismatch: "
                        f"expected {source_company_id}, found {actual_source_company_id}"
                    )

                target_company_name = await conn.fetchval(
                    "SELECT name FROM companies WHERE id = $1",
                    target_company_id,
                )
                if not target_company_name:
                    raise RuntimeError(f"Target company {target_company_id} not found")

                target_company_slug = self._slugify(target_company_name)
                project_slug = self._slugify(project_row["name"])
                new_webhook_url = f"/api/v1/webhooks/github/{target_company_slug}/{project_slug}"

                # Update projects row first inside transaction
                await conn.execute(
                    """
                    UPDATE projects
                    SET company_id = $1,
                        webhook_url = $2,
                        updated_at = NOW()
                    WHERE id = $3
                      AND company_id = $4
                    """,
                    target_company_id,
                    new_webhook_url,
                    project_id,
                    source_company_id,
                )

                # Discover all project-scoped tables that also carry company_id
                scoped_tables = await conn.fetch(
                    """
                    SELECT c1.table_name
                    FROM information_schema.columns c1
                    JOIN information_schema.columns c2
                      ON c1.table_schema = c2.table_schema
                     AND c1.table_name = c2.table_name
                    WHERE c1.table_schema = 'public'
                      AND c1.column_name = 'project_id'
                      AND c2.column_name = 'company_id'
                    ORDER BY c1.table_name
                    """
                )

                updated_tables: Dict[str, int] = {}

                for row in scoped_tables:
                    table_name = row["table_name"]
                    if table_name == "projects":
                        continue

                    has_updated_at = await conn.fetchval(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = $1
                              AND column_name = 'updated_at'
                        )
                        """,
                        table_name,
                    )

                    if has_updated_at:
                        query = (
                            f'UPDATE "{table_name}" '
                            "SET company_id = $1, updated_at = NOW() "
                            "WHERE project_id = $2 AND company_id = $3"
                        )
                    else:
                        query = (
                            f'UPDATE "{table_name}" '
                            "SET company_id = $1 "
                            "WHERE project_id = $2 AND company_id = $3"
                        )

                    result = await conn.execute(
                        query,
                        target_company_id,
                        project_id,
                        source_company_id,
                    )
                    # asyncpg returns: UPDATE <count>
                    updated_count = int(result.split(" ")[-1])
                    if updated_count > 0:
                        updated_tables[table_name] = updated_count

                logger.info(
                    f"Transferred project {project_id} "
                    f"from company {source_company_id} to {target_company_id}"
                )

                return {
                    "project_updated": True,
                    "project_company_id": target_company_id,
                    "webhook_url": new_webhook_url,
                    "updated_tables": updated_tables,
                }

    async def update(
        self,
        project_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        github_token: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update project fields using COALESCE semantics (only update provided fields).

        Args:
            project_id: Project UUID
            name: Optional new name
            description: Optional new description
            github_token: Optional new GitHub token

        Returns:
            Updated project record or None if not found
        """
        # Build dynamic update query
        updates = []
        params = []
        param_idx = 1

        if name is not None:
            updates.append(f"name = ${param_idx}")
            params.append(name)
            param_idx += 1

        if description is not None:
            updates.append(f"description = ${param_idx}")
            params.append(description)
            param_idx += 1

        if github_token is not None:
            updates.append(f"github_token = ${param_idx}")
            params.append(github_token)
            param_idx += 1

        if not updates:
            # No updates provided, just fetch current record
            return await self.get_by_id(project_id)

        updates.append("updated_at = NOW()")
        params.append(project_id)

        query = f"""
            UPDATE projects
            SET {", ".join(updates)}
            WHERE id = ${param_idx}
            RETURNING id::text, company_id::text, name, description,
                      github_webhook_secret, github_token, webhook_url,
                      created_at, updated_at
        """

        row = await self.fetch_one(query, *params)

        if row:
            logger.info(f"Updated project: {project_id}")

        return row

    async def regenerate_webhook_secret(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Regenerate the webhook secret for a project.

        Args:
            project_id: Project UUID

        Returns:
            Updated project record with new secret, or None if not found
        """
        new_secret = secrets.token_urlsafe(32)

        query = """
            UPDATE projects
            SET github_webhook_secret = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id::text, company_id::text, name, description,
                      github_webhook_secret, github_token, webhook_url,
                      created_at, updated_at
        """

        row = await self.fetch_one(query, new_secret, project_id)

        if row:
            logger.info(f"Regenerated webhook secret for project: {project_id}")

        return row
