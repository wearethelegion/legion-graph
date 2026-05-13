"""
Agent Repository
Database operations for agents and skill chunks.

Access Control Fields (added in migration 022):
- project_id: Scopes agent to a project (NULL = company-wide)
- user_id: Owner of the agent (NULL = system agent)
- sealed: If TRUE, agent cannot be modified
- public: If TRUE, visible to others in same company/project
"""

import json
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
from api.repositories.base_repository import BaseRepository
from api.utils.text import sanitize_text
from loguru import logger


class AgentRepository(BaseRepository):
    """Repository for agent and skill chunk operations."""

    # Column list for SELECT statements (includes access control fields)
    _AGENT_COLUMNS = """
        id, company_id, name, personality, main_responsibilities,
        system_prompt, metadata, created_by, created_at, updated_at,
        project_id, user_id, sealed, public, when_to_use
    """

    async def create_agent(
        self,
        company_id: UUID,
        name: str,
        personality: str,
        main_responsibilities: str,
        system_prompt: str,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: Optional[UUID] = None,
        project_id: Optional[str] = None,
        user_id: Optional[str] = None,
        sealed: bool = False,
        public: bool = False,
        when_to_use: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create new agent with access control fields.

        Args:
            company_id: Company UUID
            name: Agent name
            personality: Agent personality description
            main_responsibilities: Main responsibilities description
            system_prompt: System prompt for agent
            metadata: Optional metadata dictionary
            created_by: Optional UUID of creator
            project_id: Optional project UUID (NULL = company-wide)
            user_id: Optional owner UUID (NULL = system agent)
            sealed: If True, agent cannot be modified (default: False)
            public: If True, visible to others (default: False)
            when_to_use: When to delegate to this agent

        Returns:
            Created agent record
        """
        agent_id = str(uuid4())

        query = """
            INSERT INTO agents (
                id, company_id, name, personality, main_responsibilities,
                system_prompt, metadata, created_by, created_at, updated_at,
                project_id, user_id, sealed, public, when_to_use
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), $9, $10, $11, $12, $13)
            RETURNING id, company_id, name, personality, main_responsibilities,
                      system_prompt, metadata, created_by, created_at, updated_at,
                      project_id, user_id, sealed, public, when_to_use
        """

        row = await self.fetch_one(
            query,
            agent_id,
            str(company_id),
            sanitize_text(name),
            sanitize_text(personality),
            sanitize_text(main_responsibilities),
            sanitize_text(system_prompt),
            json.dumps(metadata or {}),
            str(created_by) if created_by else None,
            project_id,
            user_id,
            sealed,
            public,
            sanitize_text(when_to_use),
        )

        if not row:
            raise RuntimeError("Failed to create agent")

        logger.info(f"Created agent: {row['id']} ({name})")
        return row

    async def get_agent(self, agent_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get agent by ID.

        Args:
            agent_id: Agent UUID

        Returns:
            Agent record or None if not found
        """
        query = f"""
            SELECT {self._AGENT_COLUMNS}
            FROM agents
            WHERE id = $1
        """
        return await self.fetch_one(
            query, str(agent_id) if not isinstance(agent_id, str) else agent_id
        )

    async def get_agent_by_name(self, company_id: UUID, name: str) -> Optional[Dict[str, Any]]:
        """
        Get agent by company and name.

        Args:
            company_id: Company UUID
            name: Agent name

        Returns:
            Agent record or None if not found
        """
        query = f"""
            SELECT {self._AGENT_COLUMNS}
            FROM agents
            WHERE company_id = $1 AND name = $2
        """
        return await self.fetch_one(
            query, str(company_id) if not isinstance(company_id, str) else company_id, name
        )

    async def get_orchestrator(self, company_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the orchestrator agent (role='orchestrator' in metadata).

        If company_id provided, get orchestrator for that company.
        Otherwise, get the first orchestrator found (for single-company setups).

        Args:
            company_id: Optional company UUID

        Returns:
            Orchestrator agent record or None if not found
        """
        if company_id:
            query = f"""
                SELECT {self._AGENT_COLUMNS}
                FROM agents
                WHERE company_id = $1
                  AND (metadata->>'role' = 'orchestrator' OR metadata->>'is_orchestrator' = 'true')
                LIMIT 1
            """
            return await self.fetch_one(query, company_id)
        else:
            # Get first orchestrator found
            query = f"""
                SELECT {self._AGENT_COLUMNS}
                FROM agents
                WHERE metadata->>'role' = 'orchestrator' OR metadata->>'is_orchestrator' = 'true'
                LIMIT 1
            """
            return await self.fetch_one(query)

    async def get_available_agents(
        self, company_id: str, exclude_agent_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all available agents for a company (excluding orchestrator if specified).
        These are agents the orchestrator can delegate to.

        Args:
            company_id: Company UUID
            exclude_agent_id: Optional agent ID to exclude (usually the orchestrator)

        Returns:
            List of agent records
        """
        if exclude_agent_id:
            query = f"""
                SELECT {self._AGENT_COLUMNS}
                FROM agents
                WHERE company_id = $1 AND id != $2
                ORDER BY name
            """
            return await self.fetch_all(query, company_id, exclude_agent_id)
        else:
            query = f"""
                SELECT {self._AGENT_COLUMNS}
                FROM agents
                WHERE company_id = $1
                ORDER BY name
            """
            return await self.fetch_all(query, company_id)

    async def list_agents(
        self, company_id: UUID, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List all agents for a company (no visibility filtering).

        Args:
            company_id: Company UUID
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List of agent records
        """
        query = f"""
            SELECT {self._AGENT_COLUMNS}
            FROM agents
            WHERE company_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
        """
        return await self.fetch_all(
            query, str(company_id) if not isinstance(company_id, str) else company_id, limit, offset
        )

    async def list_visible_agents(
        self,
        company_id: str,
        current_user_id: str,
        accessible_project_ids: List[str],
        project_id_filter: Optional[str] = None,
        include_public: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List agents with visibility filtering.

        Visibility: owned OR (public AND same_company) OR (public AND same_project)

        Uses parameterized ANY($N::text[]) for project IDs per security review R1.

        Args:
            company_id: Company UUID (already validated for access)
            current_user_id: Current user's UUID
            accessible_project_ids: List of project UUIDs user can access
            project_id_filter: Optional filter by specific project
            include_public: Include public agents (default: True)
            limit: Maximum results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'agents' list
        """
        # Build visibility conditions
        # Visibility: owned OR (public AND same_company) OR (public AND same_project)
        visibility_conditions = [
            "a.user_id = $1"  # Owned by user
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
            visibility_conditions.append("(a.public = true AND a.company_id = $2)")
            if accessible_project_ids:
                # Use parameterized array for SQL injection safety (R1)
                visibility_conditions.append(
                    f"(a.public = true AND a.project_id = ANY(${param_idx}::text[]))"
                )
                params.append(accessible_project_ids)
                param_idx += 1

        visibility_clause = f"({' OR '.join(visibility_conditions)})"

        # Build full query
        where_clauses = [f"a.company_id = $2", visibility_clause]
        if project_id_filter:
            where_clauses.append(f"a.project_id = ${param_idx}")
            params.append(project_id_filter)
            param_idx += 1

        query = f"""
            SELECT {self._AGENT_COLUMNS}, COUNT(*) OVER() as total_count
            FROM agents a
            WHERE {" AND ".join(where_clauses)}
            ORDER BY a.updated_at DESC
            LIMIT $3 OFFSET $4
        """

        rows = await self.fetch_all(query, *params)

        total = rows[0]["total_count"] if rows else 0
        # Remove total_count from each row
        agents = []
        for row in rows:
            agent = dict(row)
            agent.pop("total_count", None)
            agents.append(agent)

        return {"total_count": total, "agents": agents}

    async def update_agent(
        self,
        agent_id: UUID,
        name: Optional[str] = None,
        personality: Optional[str] = None,
        main_responsibilities: Optional[str] = None,
        system_prompt: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        project_id: Optional[str] = None,
        public: Optional[bool] = None,
        project_id_provided: bool = False,
        when_to_use: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update agent with COALESCE semantics (only provided fields are updated).

        Note: sealed and user_id cannot be updated via this method.
        - sealed: Use seal_agent/unseal_agent for admin operations
        - user_id: Ownership transfer is a separate operation

        Args:
            agent_id: Agent UUID
            name: Optional new agent name
            personality: Optional new personality description
            main_responsibilities: Optional new responsibilities
            system_prompt: Optional new system prompt
            metadata: Optional new metadata dictionary
            project_id: Optional new project scope (None to clear)
            public: Optional new visibility setting
            project_id_provided: True if project_id was explicitly provided in request
                                 (allows distinguishing "not provided" vs "set to NULL")
            when_to_use: Optional new when_to_use description

        Returns:
            Updated agent record or None if agent not found
        """
        # project_id uses CASE WHEN pattern to handle "provided as null" vs "not provided"
        # COALESCE cannot distinguish these cases since both pass None

        query = f"""
            UPDATE agents
            SET name = COALESCE($2, name),
                personality = COALESCE($3, personality),
                main_responsibilities = COALESCE($4, main_responsibilities),
                system_prompt = COALESCE($5, system_prompt),
                metadata = COALESCE($6, metadata),
                project_id = CASE WHEN $7::boolean THEN $8 ELSE project_id END,
                public = COALESCE($9, public),
                when_to_use = COALESCE($10, when_to_use),
                updated_at = NOW()
            WHERE id = $1
            RETURNING {self._AGENT_COLUMNS}
        """

        row = await self.fetch_one(
            query,
            str(agent_id) if not isinstance(agent_id, str) else agent_id,
            sanitize_text(name) if name is not None else None,
            sanitize_text(personality) if personality is not None else None,
            sanitize_text(main_responsibilities) if main_responsibilities is not None else None,
            sanitize_text(system_prompt) if system_prompt is not None else None,
            json.dumps(metadata) if metadata is not None else None,
            project_id_provided,
            project_id,
            public,
            sanitize_text(when_to_use) if when_to_use is not None else None,
        )

        if row:
            logger.info(f"Updated agent: {agent_id}")

        return row

    async def delete_agent(self, agent_id: UUID) -> bool:
        """
        Delete agent (cascades to chunks).

        Args:
            agent_id: Agent UUID

        Returns:
            True if agent was deleted, False otherwise
        """
        query = "DELETE FROM agents WHERE id = $1"
        result = await self.execute(
            query, str(agent_id) if not isinstance(agent_id, str) else agent_id
        )
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted agent: {agent_id}")

        return deleted

    # Skill chunk operations

    async def create_chunks(
        self, agent_id: UUID, chunks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Batch create skill chunks.

        Args:
            agent_id: Agent UUID
            chunks: List of chunk dictionaries with required fields

        Returns:
            List of created chunk records
        """
        if not chunks:
            return []

        query = """
            INSERT INTO skill_chunks (
                id, agent_id, file_path, file_type, chunk_index,
                section_title, chunk_type, summary, content, token_count,
                key_concepts, dependencies, file_references, qdrant_point_id,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
            RETURNING id, agent_id, file_path, file_type, chunk_index,
                      section_title, chunk_type, summary, content, token_count,
                      key_concepts, dependencies, file_references, qdrant_point_id,
                      created_at
        """

        results = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for chunk in chunks:
                    chunk_id = str(uuid4())
                    row = await conn.fetchrow(
                        query,
                        chunk_id,
                        str(agent_id),
                        chunk["file_path"],
                        chunk["file_type"],
                        chunk["chunk_index"],
                        sanitize_text(chunk.get("section_title")),  # sanitize_text: prevent UTF-8 corruption
                        chunk.get("chunk_type"),
                        sanitize_text(chunk.get("summary")),  # sanitize_text: prevent UTF-8 corruption
                        sanitize_text(chunk["content"]),  # sanitize_text: prevent UTF-8 corruption
                        chunk.get("token_count"),
                        json.dumps(chunk.get("key_concepts", [])),  # Convert list to JSON string
                        json.dumps(chunk.get("dependencies", [])),  # Convert list to JSON string
                        json.dumps(chunk.get("file_references", [])),  # Convert list to JSON string
                        chunk.get("qdrant_point_id"),
                    )
                    results.append(self._parse_json_fields(dict(row)))

        logger.info(f"Created {len(results)} chunks for agent {agent_id}")
        return results

    async def get_chunks(
        self, agent_id: UUID, file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get skill chunks for an agent, optionally filtered by file path.

        Args:
            agent_id: Agent UUID
            file_path: Optional file path filter

        Returns:
            List of chunk records
        """
        if file_path:
            query = """
                SELECT id, agent_id, file_path, file_type, chunk_index,
                       section_title, chunk_type, summary, content, token_count,
                       key_concepts, dependencies, file_references, qdrant_point_id,
                       created_at
                FROM skill_chunks
                WHERE agent_id = $1 AND file_path = $2
                ORDER BY chunk_index
            """
            return await self.fetch_all(
                query, str(agent_id) if not isinstance(agent_id, str) else agent_id, file_path
            )
        else:
            query = """
                SELECT id, agent_id, file_path, file_type, chunk_index,
                       section_title, chunk_type, summary, content, token_count,
                       key_concepts, dependencies, file_references, qdrant_point_id,
                       created_at
                FROM skill_chunks
                WHERE agent_id = $1
                ORDER BY file_path, chunk_index
            """
            return await self.fetch_all(
                query, str(agent_id) if not isinstance(agent_id, str) else agent_id
            )

    async def get_chunk_count(self, agent_id: UUID) -> int:
        """
        Get total chunk count for agent.

        Args:
            agent_id: Agent UUID

        Returns:
            Number of chunks
        """
        query = "SELECT COUNT(*) FROM skill_chunks WHERE agent_id = $1"
        return await self.fetch_val(
            query, str(agent_id) if not isinstance(agent_id, str) else agent_id
        )

    async def get_file_count(self, agent_id: UUID) -> int:
        """
        Get unique file count for agent.

        Args:
            agent_id: Agent UUID

        Returns:
            Number of unique files
        """
        query = """
            SELECT COUNT(DISTINCT file_path)
            FROM skill_chunks
            WHERE agent_id = $1
        """
        return await self.fetch_val(
            query, str(agent_id) if not isinstance(agent_id, str) else agent_id
        )

    async def update_chunk_qdrant_id(self, chunk_id: UUID, qdrant_point_id: str) -> bool:
        """
        Update Qdrant point ID for chunk.

        Args:
            chunk_id: Chunk UUID
            qdrant_point_id: Qdrant point ID

        Returns:
            True if updated, False otherwise
        """
        query = """
            UPDATE skill_chunks
            SET qdrant_point_id = $1
            WHERE id = $2
        """
        result = await self.execute(
            query, qdrant_point_id, str(chunk_id) if not isinstance(chunk_id, str) else chunk_id
        )
        return result.endswith("1")
