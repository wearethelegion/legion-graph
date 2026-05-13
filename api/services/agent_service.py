"""Main agent service orchestrating upload, chunking, embedding, and sync.

Access Control:
- is_agent_visible: Checks if user can see an agent
- can_modify_agent: Checks if user can modify an agent
- get_agent_if_visible: Returns agent only if visible to user
- list_agents_visible: Returns filtered list based on visibility
- update_agent_with_access_check: Validates visibility + modification rights
- delete_agent_with_access_check: Validates visibility + modification rights
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
from fastapi import UploadFile, HTTPException
from loguru import logger

from api.repositories.agent_repository import AgentRepository
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.repositories.project_repository import ProjectRepository
from api.services.skill_chunk_extractor import SkillChunkExtractor
from api.services.agent_sync_service import AgentSyncService
from api.services.agent_skill_service import AgentSkillService
from api.services.agent_access import is_agent_visible, can_modify_agent
from api.utils.file_validator import FileValidator
from api.auth import CurrentUser
from kgrag.embeddings import get_embedder

# Learning skill ID - "How to Research and Create Skills"
# Same as grpc_server/servicers/agent_skill_servicer.py LEARNING_SKILL_ID
LEARNING_SKILL_ID = "4fb0f6b7-7a74-4f68-9e94-82e9a4220926"


class AgentService:
    """Main service for agent management operations."""

    def __init__(
        self,
        agent_repo: AgentRepository,
        neo4j_repo: Neo4jRepository,
        qdrant_repo: QdrantRepository,
        project_repo: Optional[ProjectRepository] = None
    ):
        self.agent_repo = agent_repo
        self.neo4j_repo = neo4j_repo
        self.qdrant_repo = qdrant_repo
        self.project_repo = project_repo
        self.chunk_extractor = SkillChunkExtractor()
        self.sync_service = AgentSyncService(agent_repo, neo4j_repo, qdrant_repo)
        self.skill_service = AgentSkillService(neo4j_repo=neo4j_repo)
        self.file_validator = FileValidator()
        self.embedder = get_embedder(provider="gemini")

    async def create_agent(
        self,
        company_id: UUID,
        name: str,
        personality: str,
        main_responsibilities: str,
        system_prompt: str,
        skill_package: UploadFile,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """
        Create agent from skill package upload.

        Pipeline:
        1. Validate ZIP
        2. Create agent in PostgreSQL
        3. Extract ZIP to memory
        4. Extract chunks with Gemini
        5. Store chunks in PostgreSQL
        6. Sync to Neo4j (agent + files + chunks + concepts/deps)
        7. Embed and sync to Qdrant

        Full rollback on any failure.

        Args:
            company_id: Company UUID
            name: Agent name
            personality: Agent personality
            main_responsibilities: Agent responsibilities
            system_prompt: System prompt
            skill_package: ZIP file upload
            metadata: Optional metadata
            created_by: Creator user ID

        Returns:
            Created agent dict

        Raises:
            HTTPException: On validation, creation, or sync failures
        """
        agent = None

        try:
            # 1. Validate ZIP
            logger.info(f"Validating skill package for agent '{name}'")
            is_valid, error_msg = await self.file_validator.validate_zip_archive(skill_package)
            if not is_valid:
                raise HTTPException(status_code=400, detail=error_msg)

            # 2. Check for duplicate name
            existing = await self.agent_repo.get_agent_by_name(company_id, name)
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent with name '{name}' already exists"
                )

            # 3. Create agent in PostgreSQL
            logger.info(f"Creating agent '{name}' in PostgreSQL")
            agent = await self.agent_repo.create_agent(
                company_id=company_id,
                name=name,
                personality=personality,
                main_responsibilities=main_responsibilities,
                system_prompt=system_prompt,
                metadata=metadata or {},
                created_by=created_by
            )
            agent_id = agent["id"]

            # 4. Extract ZIP contents to memory
            logger.info(f"Extracting skill package for agent {agent_id}")
            files = await self.file_validator.extract_zip_contents(skill_package)

            # 5. Extract chunks from all files using Gemini
            logger.info(f"Extracting chunks from {len(files)} files")
            all_chunks = []
            file_metadata = []

            for file_path, file_content_bytes in files.items():
                # Decode file content
                try:
                    file_content = file_content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    logger.warning("Skipping binary file: {}", file_path)
                    continue

                # Extract chunks with Gemini
                extraction_result = await self.chunk_extractor.extract_chunks_from_file(
                    file_path=file_path,
                    file_content=file_content
                )

                # Prepare chunks for storage
                for idx, chunk in enumerate(extraction_result.chunks):
                    chunk_data = {
                        "agent_id": agent_id,
                        "file_path": file_path,
                        "file_type": extraction_result.file_type,
                        "chunk_index": idx,
                        "section_title": chunk.get("section_title"),
                        "chunk_type": chunk.get("chunk_type"),
                        "summary": chunk.get("summary"),
                        "content": chunk["content"],
                        "token_count": len(chunk["content"].split()),  # Simple estimate
                        "key_concepts": chunk.get("key_concepts", []),
                        "dependencies": chunk.get("dependencies", []),
                        "file_references": chunk.get("file_references", []),
                    }
                    all_chunks.append(chunk_data)

                # Track file metadata
                file_metadata.append({
                    "file_path": file_path,
                    "file_type": extraction_result.file_type,
                    "chunk_count": len(extraction_result.chunks)
                })

            # 6. Store chunks in PostgreSQL
            logger.info(f"Storing {len(all_chunks)} chunks in PostgreSQL")
            stored_chunks = await self.agent_repo.create_chunks(agent_id, all_chunks)

            # 7. Sync to Neo4j
            logger.info(f"Syncing agent {agent_id} to Neo4j")
            try:
                await self.sync_service.sync_agent_to_graph(
                    agent_id=agent_id,
                    agent_data=agent,
                    file_metadata=file_metadata
                )

                await self.sync_service.sync_chunks_to_graph(stored_chunks)

            except Exception as neo4j_error:
                logger.error("Neo4j sync failed for agent {}: {}", agent_id, neo4j_error)
                await self.sync_service.rollback_agent(agent_id, company_id)
                raise HTTPException(status_code=500, detail="Failed to sync to knowledge graph")

            # 8. Sync to Qdrant
            logger.info(f"Syncing {len(stored_chunks)} chunks to Qdrant")
            try:
                await self.sync_service.sync_chunks_to_qdrant(
                    company_id=company_id,
                    chunks=stored_chunks
                )

            except Exception as qdrant_error:
                logger.error("Qdrant sync failed for agent {}: {}", agent_id, qdrant_error)
                await self.sync_service.rollback_agent(agent_id, company_id)
                raise HTTPException(status_code=500, detail="Failed to sync to vector store")

            logger.info(f"Successfully created agent {agent_id} with {len(stored_chunks)} chunks")

            # Return agent with stats
            agent["chunk_count"] = len(stored_chunks)
            agent["file_count"] = len(file_metadata)

            return agent

        except HTTPException:
            # Re-raise HTTP exceptions
            if agent:
                await self.sync_service.rollback_agent(agent["id"], company_id)
            raise

        except Exception as e:
            logger.error("Failed to create agent: {}", e)
            if agent:
                await self.sync_service.rollback_agent(agent["id"], company_id)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_agent(self, agent_id: UUID) -> Optional[Dict[str, Any]]:
        """Get agent by ID with stats."""
        agent = await self.agent_repo.get_agent(agent_id)
        if not agent:
            return None

        # Add stats
        agent["chunk_count"] = await self.agent_repo.get_chunk_count(agent_id)
        agent["file_count"] = await self.agent_repo.get_file_count(agent_id)

        return agent

    async def list_agents(
        self,
        company_id: UUID,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List all agents for a company."""
        agents = await self.agent_repo.list_agents(company_id, limit, offset)

        # Add stats to each agent
        for agent in agents:
            agent["chunk_count"] = await self.agent_repo.get_chunk_count(agent["id"])
            agent["file_count"] = await self.agent_repo.get_file_count(agent["id"])

        return agents

    async def delete_agent(self, agent_id: UUID, company_id: UUID) -> bool:
        """Delete agent and all related data."""
        logger.info(f"Deleting agent {agent_id}")

        try:
            # Delete from all layers
            await self.sync_service.rollback_agent(agent_id, company_id)
            return True

        except Exception as e:
            logger.error("Failed to delete agent {}: {}", agent_id, e)
            raise HTTPException(status_code=500, detail="Failed to delete agent")

    async def create_agent_json(
        self,
        company_id: UUID,
        name: str,
        role: str,
        personality: str,
        main_responsibilities: str,
        system_prompt: str,
        capabilities: Optional[List[str]] = None,
        specialization: Optional[str] = None,
        created_by: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """
        Create agent from JSON body (matching gRPC createAgent pattern).

        Args:
            company_id: Company UUID
            name: Agent name
            role: Agent role (e.g., developer, researcher)
            personality: Agent personality
            main_responsibilities: Agent responsibilities
            system_prompt: System prompt
            capabilities: Optional list of capabilities
            specialization: Optional specialization area
            created_by: Creator user ID

        Returns:
            Created agent dict

        Raises:
            HTTPException: On validation or creation failures
        """
        try:
            # Check for duplicate name
            existing = await self.agent_repo.get_agent_by_name(company_id, name)
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent with name '{name}' already exists"
                )

            # Build metadata
            metadata = {
                "role": role,
                "capabilities": capabilities or [],
                "specialization": specialization or "",
                "is_orchestrator": role == "orchestrator"
            }

            # Create agent in PostgreSQL
            logger.info(f"Creating agent '{name}' with role '{role}' in PostgreSQL")
            agent = await self.agent_repo.create_agent(
                company_id=company_id,
                name=name,
                personality=personality,
                main_responsibilities=main_responsibilities,
                system_prompt=system_prompt,
                metadata=metadata,
                created_by=created_by
            )

            # Create Agent node in Neo4j
            try:
                await self.neo4j_repo.create_agent_node(
                    agent_id=agent["id"],
                    company_id=str(company_id),
                    name=name,
                    personality=personality,
                    main_responsibilities=main_responsibilities
                )
            except Exception as neo4j_error:
                logger.error("Neo4j agent node creation failed: {}", neo4j_error)
                # Rollback PostgreSQL agent
                await self.agent_repo.delete_agent(agent["id"])
                raise HTTPException(status_code=500, detail="Failed to sync agent to knowledge graph")

            # Auto-link the learning skill ("How to Research and Create Skills")
            # Same behavior as gRPC CreateAgent - don't fail if skill doesn't exist
            learning_skill_linked = False
            try:
                await self.skill_service.link_skill(
                    agent_id=agent["id"],
                    expertise_id=LEARNING_SKILL_ID
                )
                learning_skill_linked = True
                logger.info(f"Auto-linked learning skill to agent {agent['id']}")
            except ValueError as e:
                # Learning skill might not exist yet - just log warning
                logger.warning("Could not auto-link learning skill: {}", e)

            logger.info(f"Successfully created agent {agent['id']} ({name}) with role {role}")
            agent["learning_skill_linked"] = learning_skill_linked
            return agent

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create agent: {}", e)
            raise HTTPException(status_code=500, detail=str(e))

    async def update_agent(
        self,
        agent_id: UUID,
        company_id: UUID,
        name: Optional[str] = None,
        role: Optional[str] = None,
        personality: Optional[str] = None,
        main_responsibilities: Optional[str] = None,
        system_prompt: Optional[str] = None,
        capabilities: Optional[List[str]] = None,
        specialization: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update agent with COALESCE semantics (only provided fields are updated).

        Args:
            agent_id: Agent UUID
            company_id: Company UUID (for validation)
            name: Optional new agent name
            role: Optional new role
            personality: Optional new personality
            main_responsibilities: Optional new responsibilities
            system_prompt: Optional new system prompt
            capabilities: Optional new capabilities list
            specialization: Optional new specialization

        Returns:
            Updated agent dict

        Raises:
            HTTPException: On validation or update failures
        """
        try:
            # Verify agent exists and belongs to company
            agent = await self.agent_repo.get_agent(agent_id)
            if not agent:
                raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

            if str(agent["company_id"]) != str(company_id):
                raise HTTPException(
                    status_code=404,
                    detail=f"Agent {agent_id} not found in company {company_id}"
                )

            # Check for duplicate name if name is being changed
            if name and name != agent["name"]:
                existing = await self.agent_repo.get_agent_by_name(company_id, name)
                if existing:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Agent with name '{name}' already exists"
                    )

            # Build metadata update if role, capabilities, or specialization provided
            metadata = None
            if role is not None or capabilities is not None or specialization is not None:
                # Get existing metadata
                existing_metadata = agent.get("metadata", {})
                if isinstance(existing_metadata, str):
                    import json
                    existing_metadata = json.loads(existing_metadata)

                metadata = {
                    "role": role if role is not None else existing_metadata.get("role", "specialist"),
                    "capabilities": capabilities if capabilities is not None else existing_metadata.get("capabilities", []),
                    "specialization": specialization if specialization is not None else existing_metadata.get("specialization", ""),
                    "is_orchestrator": (role or existing_metadata.get("role", "")) == "orchestrator"
                }

            # Update agent in PostgreSQL
            updated_agent = await self.agent_repo.update_agent(
                agent_id=agent_id,
                name=name,
                personality=personality,
                main_responsibilities=main_responsibilities,
                system_prompt=system_prompt,
                metadata=metadata
            )

            if not updated_agent:
                raise HTTPException(status_code=500, detail="Failed to update agent")

            logger.info(f"Successfully updated agent {agent_id}")
            return updated_agent

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to update agent: {}", e)
            raise HTTPException(status_code=500, detail=str(e))

    async def search_agents(
        self,
        company_id: UUID,
        query: str,
        limit: int = 10,
        file_type_filter: Optional[str] = None,
        chunk_type_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search agent skills using hybrid search (vector + graph).

        Args:
            company_id: Company UUID
            query: Search query
            limit: Max results
            file_type_filter: Optional file type filter
            chunk_type_filter: Optional chunk type filter

        Returns:
            List of search results with relevance scores
        """
        collection_name = f"company_{company_id}"

        # Perform vector search in Qdrant
        query_embedding = self.embedder.embed_query(query)

        # Use Qdrant client to search
        search_results = self.qdrant_repo.client.query_points(
            collection_name=collection_name,
            query=query_embedding,
            limit=limit * 2,  # Get more for filtering
            score_threshold=0.5
        ).points

        # Filter and format results
        results = []
        for hit in search_results:
            payload = hit.payload

            # Apply filters
            if file_type_filter and payload.get("file_type") != file_type_filter:
                continue
            if chunk_type_filter and payload.get("chunk_type") != chunk_type_filter:
                continue

            # Get agent data
            agent_id = UUID(payload["agent_id"])
            agent = await self.agent_repo.get_agent(agent_id)
            if not agent:
                continue

            results.append({
                "agent_id": str(agent["id"]),
                "agent_name": agent["name"],
                "chunk_id": payload["chunk_id"],
                "file_path": payload["file_path"],
                "file_type": payload["file_type"],
                "section_title": payload.get("section_title"),
                "chunk_type": payload.get("chunk_type"),
                "summary": payload.get("summary"),
                "key_concepts": payload.get("key_concepts", []),
                "relevance_score": hit.score
            })

            if len(results) >= limit:
                break

        return results

    # ─── Access-Controlled Methods ────────────────────────────────────────────

    async def get_user_accessible_projects(
        self,
        current_user: CurrentUser,
        company_id: str
    ) -> List[str]:
        """
        Get list of project IDs user has access to within a company.

        For now, users can access all projects in companies they belong to.
        Future: Implement project-level access control if needed.

        Args:
            current_user: Authenticated user
            company_id: Company to get projects for

        Returns:
            List of project UUID strings
        """
        if not self.project_repo:
            return []

        # Get all projects for the company
        projects = await self.project_repo.get_by_company(company_id)
        return [p["id"] for p in projects]

    async def get_agent_if_visible(
        self,
        agent_id: str,
        current_user: CurrentUser,
        company_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get agent only if visible to user.

        Returns None (not 403) if agent isn't visible - security best practice
        to avoid revealing existence of hidden agents.

        Args:
            agent_id: Agent UUID
            current_user: Authenticated user
            company_id: Optional company context for project lookup

        Returns:
            Agent dict with stats if visible, None otherwise
        """
        agent = await self.agent_repo.get_agent(agent_id)
        if not agent:
            return None

        # Get accessible projects for visibility check
        accessible_projects = []
        if company_id and self.project_repo:
            accessible_projects = await self.get_user_accessible_projects(
                current_user, company_id
            )

        if not is_agent_visible(agent, current_user, accessible_projects):
            return None  # Return None, not 403 (security: hide existence)

        # Add stats
        agent["chunk_count"] = await self.agent_repo.get_chunk_count(agent_id)
        agent["file_count"] = await self.agent_repo.get_file_count(agent_id)

        return agent

    async def list_agents_visible(
        self,
        company_id: str,
        current_user: CurrentUser,
        project_id: Optional[str] = None,
        include_public: bool = True,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List agents with visibility filtering.

        Args:
            company_id: Company UUID (already validated for access)
            current_user: Authenticated user
            project_id: Optional filter by specific project
            include_public: Include public agents (default: True)
            limit: Maximum results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'agents' list
        """
        # Get user's accessible projects
        accessible_projects = await self.get_user_accessible_projects(
            current_user, company_id
        )

        result = await self.agent_repo.list_visible_agents(
            company_id=company_id,
            current_user_id=current_user.user_id,
            accessible_project_ids=accessible_projects,
            project_id_filter=project_id,
            include_public=include_public,
            limit=limit,
            offset=offset
        )

        # Add stats to each agent
        for agent in result["agents"]:
            agent["chunk_count"] = await self.agent_repo.get_chunk_count(agent["id"])
            agent["file_count"] = await self.agent_repo.get_file_count(agent["id"])

        return result

    async def update_agent_with_access_check(
        self,
        agent_id: str,
        current_user: CurrentUser,
        company_id: str,
        name: Optional[str] = None,
        role: Optional[str] = None,
        personality: Optional[str] = None,
        main_responsibilities: Optional[str] = None,
        system_prompt: Optional[str] = None,
        capabilities: Optional[List[str]] = None,
        specialization: Optional[str] = None,
        project_id: Optional[str] = None,
        public: Optional[bool] = None,
        project_id_provided: bool = False,
        when_to_use: Optional[str] = None,
        when_to_use_provided: bool = False
    ) -> Dict[str, Any]:
        """
        Update agent with visibility and modification checks.

        Security checks (in order):
        1. Get agent and check visibility
        2. Check modification permission (sealed first, then ownership)
        3. Proceed with update

        Args:
            agent_id: Agent UUID
            current_user: Authenticated user
            company_id: Company UUID (for context)
            name: Optional new name
            role: Optional new role
            personality: Optional new personality
            main_responsibilities: Optional new responsibilities
            system_prompt: Optional new system prompt
            capabilities: Optional new capabilities list
            specialization: Optional new specialization
            project_id: Optional new project scope
            public: Optional new visibility
            project_id_provided: True if project_id was explicitly provided in request
                                 (allows distinguishing "not provided" vs "set to NULL")
            when_to_use: Optional new when_to_use description
            when_to_use_provided: True if when_to_use was explicitly provided in request

        Returns:
            Updated agent dict

        Raises:
            HTTPException 404: Agent not found or not visible
            HTTPException 403: Agent sealed or not owner
        """
        # 1. Get and check visibility
        agent = await self.get_agent_if_visible(agent_id, current_user, company_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

        # 2. Check modification permission (sealed check is FIRST per security review)
        allowed, error = can_modify_agent(agent, current_user)
        if not allowed:
            raise HTTPException(status_code=403, detail=error)

        # 3. Verify agent belongs to company
        if str(agent["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404,
                detail=f"Agent {agent_id} not found in company {company_id}"
            )

        # Check for duplicate name if name is being changed
        if name and name != agent["name"]:
            existing = await self.agent_repo.get_agent_by_name(company_id, name)
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent with name '{name}' already exists"
                )

        # Build metadata update if role, capabilities, or specialization provided
        metadata = None
        if role is not None or capabilities is not None or specialization is not None:
            existing_metadata = agent.get("metadata", {})
            if isinstance(existing_metadata, str):
                import json
                existing_metadata = json.loads(existing_metadata)

            metadata = {
                "role": role if role is not None else existing_metadata.get("role", "specialist"),
                "capabilities": capabilities if capabilities is not None else existing_metadata.get("capabilities", []),
                "specialization": specialization if specialization is not None else existing_metadata.get("specialization", ""),
                "is_orchestrator": (role or existing_metadata.get("role", "")) == "orchestrator"
            }

        # 4. Update agent in PostgreSQL
        # Only pass when_to_use if explicitly provided (sentinel pattern)
        when_to_use_value = when_to_use if when_to_use_provided else None
        updated_agent = await self.agent_repo.update_agent(
            agent_id=agent_id,
            name=name,
            personality=personality,
            main_responsibilities=main_responsibilities,
            system_prompt=system_prompt,
            metadata=metadata,
            project_id=project_id,
            public=public,
            project_id_provided=project_id_provided,
            when_to_use=when_to_use_value
        )

        if not updated_agent:
            raise HTTPException(status_code=500, detail="Failed to update agent")

        logger.info(f"Successfully updated agent {agent_id} with access check")
        return updated_agent

    async def delete_agent_with_access_check(
        self,
        agent_id: str,
        current_user: CurrentUser,
        company_id: str
    ) -> bool:
        """
        Delete agent with visibility and modification checks.

        Security checks (in order):
        1. Get agent and check visibility
        2. Check modification permission (sealed first, then ownership)
        3. Delete from all layers

        Args:
            agent_id: Agent UUID
            current_user: Authenticated user
            company_id: Company UUID

        Returns:
            True if deleted

        Raises:
            HTTPException 404: Agent not found or not visible
            HTTPException 403: Agent sealed or not owner
            HTTPException 500: Delete failure
        """
        # 1. Get and check visibility
        agent = await self.get_agent_if_visible(agent_id, current_user, company_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

        # 2. Check modification permission
        allowed, error = can_modify_agent(agent, current_user)
        if not allowed:
            raise HTTPException(status_code=403, detail=error)

        # 3. Verify agent belongs to company
        if str(agent["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=404,
                detail=f"Agent {agent_id} not found in company {company_id}"
            )

        # 4. Delete from all layers
        try:
            await self.sync_service.rollback_agent(agent_id, company_id)
            logger.info(f"Successfully deleted agent {agent_id} with access check")
            return True
        except Exception as e:
            logger.error("Failed to delete agent {}: {}", agent_id, e)
            raise HTTPException(status_code=500, detail="Failed to delete agent")

    async def create_agent_with_owner(
        self,
        company_id: UUID,
        name: str,
        role: str,
        personality: str,
        main_responsibilities: str,
        system_prompt: str,
        current_user: CurrentUser,
        capabilities: Optional[List[str]] = None,
        specialization: Optional[str] = None,
        project_id: Optional[str] = None,
        sealed: bool = False,
        public: bool = False,
        when_to_use: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create agent with owner set to current user.

        Note: user_id is automatically set to current_user.user_id.
        This ensures ownership cannot be spoofed via request body.

        Args:
            company_id: Company UUID
            name: Agent name
            role: Agent role
            personality: Agent personality
            main_responsibilities: Agent responsibilities
            system_prompt: System prompt
            current_user: Authenticated user (becomes owner)
            capabilities: Optional capabilities list
            specialization: Optional specialization
            project_id: Optional project scope
            sealed: If True, agent cannot be modified after creation
            public: If True, visible to others
            when_to_use: When to delegate to this agent

        Returns:
            Created agent dict

        Raises:
            HTTPException: On validation or creation failures
        """
        try:
            # Check for duplicate name
            existing = await self.agent_repo.get_agent_by_name(company_id, name)
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent with name '{name}' already exists"
                )

            # Build metadata
            metadata = {
                "role": role,
                "capabilities": capabilities or [],
                "specialization": specialization or "",
                "is_orchestrator": role == "orchestrator"
            }

            # Create agent in PostgreSQL with owner
            logger.info(f"Creating agent '{name}' with role '{role}' owned by {current_user.user_id}")
            agent = await self.agent_repo.create_agent(
                company_id=company_id,
                name=name,
                personality=personality,
                main_responsibilities=main_responsibilities,
                system_prompt=system_prompt,
                metadata=metadata,
                created_by=current_user.user_id,
                project_id=project_id,
                user_id=current_user.user_id,  # Owner = creator
                sealed=sealed,
                public=public,
                when_to_use=when_to_use,
            )

            # Create Agent node in Neo4j
            try:
                await self.neo4j_repo.create_agent_node(
                    agent_id=agent["id"],
                    company_id=str(company_id),
                    name=name,
                    personality=personality,
                    main_responsibilities=main_responsibilities
                )
            except Exception as neo4j_error:
                logger.error("Neo4j agent node creation failed: {}", neo4j_error)
                await self.agent_repo.delete_agent(agent["id"])
                raise HTTPException(status_code=500, detail="Failed to sync agent to knowledge graph")

            # Auto-link the learning skill
            learning_skill_linked = False
            try:
                await self.skill_service.link_skill(
                    agent_id=agent["id"],
                    expertise_id=LEARNING_SKILL_ID
                )
                learning_skill_linked = True
                logger.info(f"Auto-linked learning skill to agent {agent['id']}")
            except ValueError as e:
                logger.warning("Could not auto-link learning skill: {}", e)

            logger.info(f"Successfully created agent {agent['id']} ({name}) with owner {current_user.user_id}")
            agent["learning_skill_linked"] = learning_skill_linked
            return agent

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create agent: {}", e)
            raise HTTPException(status_code=500, detail=str(e))
