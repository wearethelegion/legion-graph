"""
Agent Workflow Repository
Database operations for agent behavioral workflows.

Access Control Fields:
- user_id: Owner of the workflow (NULL = system workflow)
- public: If TRUE, visible to others in same company/project

Scoping hierarchy (most to least specific):
- agent_id: Workflow for specific agent
- role: Workflow for agents with specific role
- project_id: Workflow for all agents in project
- company_id: Workflow for all agents in company
"""

import json
from typing import List, Optional, Dict, Any
from uuid import uuid4
from api.repositories.base_repository import BaseRepository
from api.utils.text import sanitize_text
from loguru import logger


class AgentWorkflowRepository(BaseRepository):
    """Repository for agent workflow operations."""

    # Column list for SELECT statements
    _WORKFLOW_COLUMNS = """
        id, company_id, project_id, agent_id, role, user_id, public,
        name, content, description, metadata, signals, version, created_at, updated_at, when_to_use
    """

    async def create(
        self,
        company_id: str,
        name: str,
        content: str,
        user_id: Optional[str] = None,
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
        Create a new agent workflow.

        Args:
            company_id: Company UUID (required)
            name: Workflow name
            content: Markdown workflow content
            user_id: Owner UUID (NULL = system workflow)
            project_id: Optional project scope
            agent_id: Optional specific agent scope
            role: Optional role scope
            public: If True, visible to others (default: False)
            description: Optional description
            metadata: Optional flexible metadata
            signals: Optional list of trigger hints for when to use this workflow
            when_to_use: When to activate this workflow

        Returns:
            Created workflow record
        """
        workflow_id = str(uuid4())

        query = """
            INSERT INTO agent_workflows (
                id, company_id, project_id, agent_id, role, user_id, public,
                name, content, description, metadata, signals, version, created_at, updated_at, when_to_use
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1, NOW(), NOW(), $13)
            RETURNING id, company_id, project_id, agent_id, role, user_id, public,
                      name, content, description, metadata, signals, version, created_at, updated_at, when_to_use
        """

        row = await self.fetch_one(
            query,
            workflow_id,
            company_id,
            project_id,
            agent_id,
            role,
            user_id,
            public,
            sanitize_text(name),
            sanitize_text(content),
            sanitize_text(description),
            json.dumps(metadata) if metadata else None,
            signals or [],
            sanitize_text(when_to_use),
        )

        if not row:
            raise RuntimeError("Failed to create agent workflow")

        logger.info(f"Created agent workflow: {row['id']} ({name})")
        return row

    async def get_by_id(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get workflow by ID.

        Args:
            workflow_id: Workflow UUID

        Returns:
            Workflow record or None if not found
        """
        query = f"""
            SELECT {self._WORKFLOW_COLUMNS}
            FROM agent_workflows
            WHERE id = $1
        """
        return await self.fetch_one(query, workflow_id)

    async def list_visible(
        self,
        company_id: str,
        current_user_id: str,
        accessible_project_ids: List[str],
        project_id_filter: Optional[str] = None,
        agent_id_filter: Optional[str] = None,
        role_filter: Optional[str] = None,
        include_public: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List workflows with visibility filtering.

        Visibility: owned OR (public AND same_company) OR (public AND same_project)

        Args:
            company_id: Company UUID (already validated for access)
            current_user_id: Current user's UUID
            accessible_project_ids: List of project UUIDs user can access
            project_id_filter: Optional filter by specific project
            agent_id_filter: Optional filter by specific agent
            role_filter: Optional filter by role
            include_public: Include public workflows (default: True)
            limit: Maximum results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'workflows' list
        """
        # Build visibility conditions
        visibility_conditions = [
            "w.user_id = $1"  # Owned by user
        ]

        # Build params list — dynamic param_idx to avoid SQL/param count mismatch
        params = [
            current_user_id,  # $1
            company_id,  # $2
            limit,  # $3
            offset,  # $4
        ]
        param_idx = 5

        if include_public:
            visibility_conditions.append("(w.public = true AND w.company_id = $2)")
            if accessible_project_ids:
                visibility_conditions.append(
                    f"(w.public = true AND w.project_id = ANY(${param_idx}::text[]))"
                )
                params.append(accessible_project_ids)
                param_idx += 1

        visibility_clause = f"({' OR '.join(visibility_conditions)})"

        # Build full query
        where_clauses = [f"w.company_id = $2", visibility_clause]

        if project_id_filter:
            where_clauses.append(f"w.project_id = ${param_idx}")
            params.append(project_id_filter)
            param_idx += 1

        if agent_id_filter:
            where_clauses.append(f"w.agent_id = ${param_idx}")
            params.append(agent_id_filter)
            param_idx += 1

        if role_filter:
            where_clauses.append(f"w.role = ${param_idx}")
            params.append(role_filter)
            param_idx += 1

        query = f"""
            SELECT {self._WORKFLOW_COLUMNS}, COUNT(*) OVER() as total_count
            FROM agent_workflows w
            WHERE {" AND ".join(where_clauses)}
            ORDER BY w.updated_at DESC
            LIMIT $3 OFFSET $4
        """

        rows = await self.fetch_all(query, *params)

        total = rows[0]["total_count"] if rows else 0
        # Remove total_count from each row
        workflows = []
        for row in rows:
            workflow = dict(row)
            workflow.pop("total_count", None)
            workflows.append(workflow)

        return {"total_count": total, "workflows": workflows}

    async def get_applicable(
        self,
        company_id: str,
        current_user_id: str,
        accessible_project_ids: List[str],
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        project_id: Optional[str] = None,
        include_public: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get applicable workflows for an agent, ordered by specificity.

        Resolution order (most to least specific):
        1. agent_id match (if provided)
        2. role match (if provided)
        3. project_id match (if provided)
        4. company-level (no filters)

        Only returns visible workflows (owned OR public).

        Args:
            company_id: Company UUID
            current_user_id: Current user's UUID
            accessible_project_ids: List of project UUIDs user can access
            agent_id: Optional agent UUID for agent-specific workflows
            role: Optional role for role-specific workflows
            project_id: Optional project UUID for project-specific workflows
            include_public: Include public workflows (default: True)

        Returns:
            List of applicable workflows ordered by specificity (most specific first)
        """
        # Build visibility conditions
        visibility_conditions = [
            "w.user_id = $1"  # Owned by user
        ]

        # Build params list — dynamic param_idx to avoid SQL/param count mismatch
        params = [
            current_user_id,  # $1
            company_id,  # $2
        ]
        param_idx = 3

        if include_public:
            visibility_conditions.append("(w.public = true AND w.company_id = $2)")
            if accessible_project_ids:
                visibility_conditions.append(
                    f"(w.public = true AND w.project_id = ANY(${param_idx}::text[]))"
                )
                params.append(accessible_project_ids)
                param_idx += 1

        visibility_clause = f"({' OR '.join(visibility_conditions)})"

        # Build scope conditions based on what filters are provided
        # We want workflows that match ANY of the scopes
        scope_conditions = []

        # Agent-specific workflows
        if agent_id:
            scope_conditions.append(f"w.agent_id = ${param_idx}")
            params.append(agent_id)
            param_idx += 1

        # Role-specific workflows (agent_id IS NULL)
        if role:
            scope_conditions.append(f"(w.agent_id IS NULL AND w.role = ${param_idx})")
            params.append(role)
            param_idx += 1

        # Project-specific workflows (agent_id IS NULL AND role IS NULL)
        if project_id:
            scope_conditions.append(
                f"(w.agent_id IS NULL AND w.role IS NULL AND w.project_id = ${param_idx})"
            )
            params.append(project_id)
            param_idx += 1

        # Company-level workflows (agent_id IS NULL AND role IS NULL AND project_id IS NULL)
        scope_conditions.append("(w.agent_id IS NULL AND w.role IS NULL AND w.project_id IS NULL)")

        scope_clause = f"({' OR '.join(scope_conditions)})"

        # Order by specificity: agent_id > role > project_id > company
        # Use CASE to assign priority scores
        query = f"""
            SELECT {self._WORKFLOW_COLUMNS},
                CASE
                    WHEN w.agent_id IS NOT NULL THEN 1
                    WHEN w.role IS NOT NULL THEN 2
                    WHEN w.project_id IS NOT NULL THEN 3
                    ELSE 4
                END as specificity
            FROM agent_workflows w
            WHERE w.company_id = $2
              AND {visibility_clause}
              AND {scope_clause}
            ORDER BY specificity, w.name
        """

        rows = await self.fetch_all(query, *params)

        # Remove specificity from each row
        workflows = []
        for row in rows:
            workflow = dict(row)
            workflow.pop("specificity", None)
            workflows.append(workflow)

        return workflows

    async def update(
        self,
        workflow_id: str,
        name: Optional[str] = None,
        content: Optional[str] = None,
        description: Optional[str] = None,
        public: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
        signals: Optional[List[str]] = None,
        project_id_provided: bool = False,
        project_id: Optional[str] = None,
        agent_id_provided: bool = False,
        agent_id: Optional[str] = None,
        role_provided: bool = False,
        role: Optional[str] = None,
        when_to_use: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update workflow with COALESCE semantics (only provided fields are updated).
        Increments version on each update.

        Scope fields (project_id, agent_id, role) use boolean flags to distinguish:
        - *_provided=False: keep current value (COALESCE behavior)
        - *_provided=True, value=None: set to NULL
        - *_provided=True, value=X: set to X

        Args:
            workflow_id: Workflow UUID
            name: Optional new name
            content: Optional new content
            description: Optional new description
            public: Optional new visibility setting
            metadata: Optional new metadata
            signals: Optional new list of trigger hints
            project_id_provided: True if project_id was in request (even if null)
            project_id: New project scope (if provided)
            agent_id_provided: True if agent_id was in request (even if null)
            agent_id: New agent scope (if provided)
            role_provided: True if role was in request (even if null)
            role: New role scope (if provided)
            when_to_use: Optional new when_to_use description

        Returns:
            Updated workflow record or None if not found
        """
        # Scope fields use CASE WHEN to handle "provided as null" vs "not provided"
        query = f"""
            UPDATE agent_workflows
            SET name = COALESCE($2, name),
                content = COALESCE($3, content),
                description = COALESCE($4, description),
                public = COALESCE($5, public),
                metadata = COALESCE($6, metadata),
                signals = COALESCE($7, signals),
                project_id = CASE WHEN $8::boolean THEN $9 ELSE project_id END,
                agent_id = CASE WHEN $10::boolean THEN $11 ELSE agent_id END,
                role = CASE WHEN $12::boolean THEN $13 ELSE role END,
                when_to_use = COALESCE($14, when_to_use),
                version = version + 1,
                updated_at = NOW()
            WHERE id = $1
            RETURNING {self._WORKFLOW_COLUMNS}
        """

        row = await self.fetch_one(
            query,
            workflow_id,
            sanitize_text(name) if name is not None else None,
            sanitize_text(content) if content is not None else None,
            sanitize_text(description) if description is not None else None,
            public,
            json.dumps(metadata) if metadata is not None else None,
            signals,
            project_id_provided,
            project_id,
            agent_id_provided,
            agent_id,
            role_provided,
            role,
            sanitize_text(when_to_use) if when_to_use is not None else None,
        )

        if row:
            logger.info(f"Updated agent workflow: {workflow_id} (version {row['version']})")

        return row

    async def delete(self, workflow_id: str) -> bool:
        """
        Delete workflow by ID.

        Args:
            workflow_id: Workflow UUID

        Returns:
            True if workflow was deleted, False otherwise
        """
        query = "DELETE FROM agent_workflows WHERE id = $1"
        result = await self.execute(query, workflow_id)
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted agent workflow: {workflow_id}")

        return deleted

    async def check_name_exists(
        self,
        company_id: str,
        name: str,
        project_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        exclude_id: Optional[str] = None,
    ) -> bool:
        """
        Check if a workflow name already exists within the same scope.

        Args:
            company_id: Company UUID
            name: Workflow name to check
            project_id: Project scope
            agent_id: Agent scope
            role: Role scope
            exclude_id: Optional workflow ID to exclude (for updates)

        Returns:
            True if name exists in scope, False otherwise
        """
        conditions = ["company_id = $1", "name = $2"]
        params = [company_id, name]
        param_idx = 3

        # Match the scope exactly
        if project_id:
            conditions.append(f"project_id = ${param_idx}")
            params.append(project_id)
            param_idx += 1
        else:
            conditions.append("project_id IS NULL")

        if agent_id:
            conditions.append(f"agent_id = ${param_idx}")
            params.append(agent_id)
            param_idx += 1
        else:
            conditions.append("agent_id IS NULL")

        if role:
            conditions.append(f"role = ${param_idx}")
            params.append(role)
            param_idx += 1
        else:
            conditions.append("role IS NULL")

        if exclude_id:
            conditions.append(f"id != ${param_idx}")
            params.append(exclude_id)

        query = f"""
            SELECT EXISTS(
                SELECT 1 FROM agent_workflows
                WHERE {" AND ".join(conditions)}
            )
        """

        return await self.fetch_val(query, *params)

    async def get_for_agent(
        self,
        company_id: str,
        agent_id: Optional[str] = None,
        role: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get workflows applicable to an agent using cascade logic.

        Query cascade (merged results, deduplicated by name):
        1. agent_id = agent.id (direct assignment)
        2. role = agent.role AND project_id = project (role+project scope)
        3. role = agent.role AND company_id = company AND project_id IS NULL (role+company scope)

        Args:
            company_id: Company UUID
            agent_id: Agent UUID (for direct assignment)
            role: Agent role (for role-based workflows)
            project_id: Project UUID (for project-scoped workflows)

        Returns:
            List of workflows with id, name, and signals (from metadata)
        """
        # Build three queries and UNION them, deduplicating by name
        # Priority: agent_id > role+project > role+company

        queries = []
        params = []
        param_idx = 1

        # Query 1: Direct agent assignment
        if agent_id:
            queries.append(f"""
                SELECT id, name, signals, when_to_use, 1 as priority
                FROM agent_workflows
                WHERE company_id = ${param_idx} AND agent_id = ${param_idx + 1}
            """)
            params.extend([company_id, agent_id])
            param_idx += 2

        # Query 2: Role + project scope
        if role and project_id:
            queries.append(f"""
                SELECT id, name, signals, when_to_use, 2 as priority
                FROM agent_workflows
                WHERE company_id = ${param_idx}
                  AND role = ${param_idx + 1}
                  AND project_id = ${param_idx + 2}
                  AND agent_id IS NULL
            """)
            params.extend([company_id, role, project_id])
            param_idx += 3

        # Query 3: Role + company scope (no project)
        if role:
            queries.append(f"""
                SELECT id, name, signals, when_to_use, 3 as priority
                FROM agent_workflows
                WHERE company_id = ${param_idx}
                  AND role = ${param_idx + 1}
                  AND project_id IS NULL
                  AND agent_id IS NULL
            """)
            params.extend([company_id, role])
            param_idx += 2

        if not queries:
            return []

        # Combine with UNION ALL, then dedupe by name keeping lowest priority
        union_query = " UNION ALL ".join(queries)
        query = f"""
            WITH all_workflows AS (
                {union_query}
            ),
            ranked AS (
                SELECT id, name, signals, when_to_use, priority,
                       ROW_NUMBER() OVER (PARTITION BY name ORDER BY priority) as rn
                FROM all_workflows
            )
            SELECT id, name, signals, when_to_use
            FROM ranked
            WHERE rn = 1
            ORDER BY name
        """

        rows = await self.fetch_all(query, *params)

        # Transform to output format with signals
        workflows = []
        for row in rows:
            workflows.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "signals": row.get("signals") or [],
                    "when_to_use": row.get("when_to_use") or "",
                }
            )

        return workflows
