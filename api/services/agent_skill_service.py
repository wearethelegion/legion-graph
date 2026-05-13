"""
Agent Skill Service - Links agents to expertise documents.

Service for:
- Linking expertise to agents as skills
- Retrieving agent skill trees for LLM navigation
- Fetching specific skill content by chunk ID
- Semantic search within skill chunks (searchSkillDetails)
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from loguru import logger

from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from kgrag.embeddings import GeminiEmbedder


@dataclass(frozen=True)
class SkillSection:
    """Immutable representation of a skill section for LLM navigation."""
    chunk_id: str
    title: str
    summary: str
    has_code: bool
    level: int = 0
    position: int = 0


@dataclass(frozen=True)
class AgentSkill:
    """Immutable representation of a linked skill."""
    expertise_id: str
    title: str
    summary: str
    sections: tuple  # tuple of SkillSection for immutability


class AgentSkillService:
    """
    Service for agent skill operations.

    Enables agents to have named skills (links to Expertise documents)
    that they can navigate and retrieve at runtime.
    """

    def __init__(
        self,
        neo4j_repo: Neo4jRepository,
        qdrant_repo: Optional[QdrantRepository] = None
    ):
        """
        Initialize with repositories.

        Args:
            neo4j_repo: Neo4j repository instance
            qdrant_repo: Optional Qdrant repository for semantic search
        """
        self._neo4j_repo = neo4j_repo
        self._qdrant_repo = qdrant_repo
        self._embedder = GeminiEmbedder() if qdrant_repo else None

    async def link_skill(
        self,
        agent_id: str,
        expertise_id: str
    ) -> Dict[str, Any]:
        """
        Link an expertise document to an agent as a skill.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID to link

        Returns:
            Dict with status and linked IDs

        Raises:
            ValueError: If agent or expertise not found
        """
        # Verify agent exists
        agent_info = await self._neo4j_repo.get_agent_info(agent_id)
        if not agent_info:
            raise ValueError(f"Agent not found: {agent_id}")

        # Verify expertise exists
        expertise = await self._neo4j_repo.get_expertise_node(expertise_id)
        if not expertise:
            raise ValueError(f"Expertise not found: {expertise_id}")

        # Create the link
        success = await self._neo4j_repo.link_agent_skill(agent_id, expertise_id)

        logger.info(f"Linked skill {expertise_id} to agent {agent_id}")

        return {
            "status": "linked" if success else "already_linked",
            "agent_id": agent_id,
            "expertise_id": expertise_id,
            "expertise_title": expertise.get("title", "")
        }

    async def unlink_skill(
        self,
        agent_id: str,
        expertise_id: str
    ) -> Dict[str, Any]:
        """
        Remove a skill link from an agent.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID to unlink

        Returns:
            Dict with status
        """
        success = await self._neo4j_repo.unlink_agent_skill(agent_id, expertise_id)

        if success:
            logger.info(f"Unlinked skill {expertise_id} from agent {agent_id}")
        else:
            logger.warning("Skill link not found: agent={}, expertise={}", agent_id, expertise_id)

        return {
            "status": "unlinked" if success else "not_found",
            "agent_id": agent_id,
            "expertise_id": expertise_id
        }

    async def get_skills(
        self,
        agent_id: str
    ) -> Dict[str, Any]:
        """
        Get agent's complete skill tree for LLM navigation.

        Returns structure that LLM can understand and navigate:
        {
            "agent_id": "...",
            "agent_name": "Developer Agent",
            "skills": [
                {
                    "expertise_id": "...",
                    "title": "Python Backend",
                    "summary": "FastAPI, SQLAlchemy...",
                    "sections": [
                        {"chunk_id": "...", "title": "API Design", "summary": "...", "has_code": true}
                    ]
                }
            ]
        }

        Args:
            agent_id: Agent UUID

        Returns:
            Skill tree dictionary for LLM consumption
        """
        # Get agent info
        agent_info = await self._neo4j_repo.get_agent_info(agent_id)
        if not agent_info:
            return {
                "agent_id": agent_id,
                "agent_name": "Unknown",
                "skills": [],
                "error": "Agent not found"
            }

        # Get skills with sections
        skills = await self._neo4j_repo.get_agent_skills(agent_id)

        logger.info(f"Retrieved {len(skills)} skills for agent {agent_id}")

        return {
            "agent_id": agent_id,
            "agent_name": agent_info.get("name", ""),
            "skills_count": len(skills),
            "skills": skills
        }

    async def get_skill_content(
        self,
        chunk_id: str
    ) -> Dict[str, Any]:
        """
        Fetch full content of a specific skill section.

        Called after LLM picks a relevant section from the skill tree.

        Args:
            chunk_id: Chunk UUID to fetch

        Returns:
            Dict with full chunk content
        """
        chunk = await self._neo4j_repo.get_expertise_chunk(chunk_id)

        if not chunk:
            return {
                "chunk_id": chunk_id,
                "error": "Chunk not found"
            }

        # Extract has_code from metadata
        metadata = chunk.get("metadata", {})
        has_code = metadata.get("has_code", False) if isinstance(metadata, dict) else False

        logger.info(f"Retrieved content for chunk {chunk_id}")

        return {
            "chunk_id": chunk_id,
            "title": chunk.get("summary", "Section"),
            "content": chunk.get("content", ""),
            "has_code": has_code,
            "level": chunk.get("level", 0),
            "expertise_id": chunk.get("expertise_id")
        }

    async def get_skill_sections(
        self,
        expertise_id: str
    ) -> Dict[str, Any]:
        """
        Get sections for ONE expertise document (on-demand loading).

        Called after LLM identifies a relevant skill from get_skills().
        This is the second tier of progressive disclosure:
        1. get_skills() -> lightweight skill titles/summaries
        2. get_skill_sections(expertise_id) -> sections for ONE skill
        3. get_skill_content(chunk_id) -> full content for ONE section

        Args:
            expertise_id: Expertise UUID

        Returns:
            Dict with expertise info and sections list
        """
        # Get expertise node info
        expertise = await self._neo4j_repo.get_expertise_node(expertise_id)

        if not expertise:
            return {
                "expertise_id": expertise_id,
                "error": "Expertise not found"
            }

        # Get sections for this expertise
        sections = await self._neo4j_repo.get_expertise_sections(expertise_id)

        logger.info(f"Retrieved {len(sections)} sections for expertise {expertise_id}")

        return {
            "expertise_id": expertise_id,
            "title": expertise.get("title", ""),
            "summary": expertise.get("summary", ""),
            "sections": sections
        }

    async def list_available_expertise(
        self,
        company_id: str,
        project_id: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List expertise documents available to link as skills.

        Args:
            company_id: Company UUID
            project_id: Optional project UUID
            limit: Max results

        Returns:
            List of expertise documents
        """
        expertise_list = await self._neo4j_repo.list_expertise_nodes(
            company_id=company_id,
            limit=limit,
            offset=0,
        )

        return expertise_list

    async def search_skill_details(
        self,
        expertise_id: str,
        query: str,
        limit: int = 5
    ) -> Dict[str, Any]:
        """
        Semantic search within a skill's chunks.

        Instead of loading all 80+ sections (getSkillSections), LLM can search
        semantically for exactly what it needs. Returns top-k relevant chunks
        with full content.

        Progressive disclosure pattern:
        1. getAgentSkills() → lightweight overview (title, summary, sections_count)
        2. searchSkillDetails(expertise_id, query) → semantic search → top-k chunks
        3. getSkillContent(chunk_id) → optional, if need more context

        Args:
            expertise_id: Expertise UUID to search within
            query: Natural language query (e.g., "error handling patterns")
            limit: Maximum results (default: 5)

        Returns:
            Dict with expertise info and matching chunks with full content
        """
        if not self._qdrant_repo or not self._embedder:
            return {
                "expertise_id": expertise_id,
                "error": "Semantic search not available - Qdrant not configured"
            }

        # Get expertise to verify it exists and get company_id for collection
        expertise = await self._neo4j_repo.get_expertise_node(expertise_id)

        if not expertise:
            return {
                "expertise_id": expertise_id,
                "error": "Expertise not found"
            }

        company_id = expertise.get("company_id")
        if not company_id:
            return {
                "expertise_id": expertise_id,
                "error": "Expertise has no company_id - cannot determine Qdrant collection"
            }

        # Embed the query
        query_vector = self._embedder.embed_query(query)

        # Search Qdrant with expertise_id filter
        collection_name = f"company_{company_id}"
        results = await self._qdrant_repo.search_skill_details(
            collection_name=collection_name,
            query_vector=query_vector,
            expertise_id=expertise_id,
            limit=limit
        )

        logger.info(
            f"Skill search for '{query}' in expertise {expertise_id} "
            f"returned {len(results)} results"
        )

        return {
            "expertise_id": expertise_id,
            "expertise_title": expertise.get("title", ""),
            "query": query,
            "results_count": len(results),
            "results": results
        }