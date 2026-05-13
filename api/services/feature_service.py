"""
Feature Service
Business logic for feature management with smart chunking and 3-tier sync.
"""

import re
import json
from typing import List, Optional, Dict, Any
from uuid import UUID
from fastapi import HTTPException
from loguru import logger
import google.generativeai as genai

from api.repositories.feature_repository import FeatureRepository
from api.repositories.project_repository import ProjectRepository
from api.services.feature_sync_service import FeatureSyncService
from kgrag.config import config


class FeatureService:
    """Service for feature operations with smart chunking and sync."""

    def __init__(
        self,
        feature_repo: FeatureRepository,
        project_repo: ProjectRepository,
        sync_service: FeatureSyncService
    ):
        self.feature_repo = feature_repo
        self.project_repo = project_repo
        self.sync_service = sync_service

        # Initialize Gemini for chunk extraction
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.gemini_model = genai.GenerativeModel('gemini-2.5-flash')

    async def create_feature(
        self,
        company_id: str,
        project_id: str,
        name: str,
        description: str,
        status: str = "ready for refinement",
        priority: str = "medium",
        next_prompt: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create feature with smart chunking and 3-tier sync.

        Pipeline:
        1. Validate project exists
        2. Create feature in PostgreSQL
        3. Smart chunk description (only if substantial)
        4. Store chunks in PostgreSQL
        5. Sync to Neo4j (feature + chunks + concepts/deps)
        6. Embed and sync to Qdrant

        Full rollback on any failure.

        Args:
            company_id: Company UUID
            project_id: Project UUID
            name: Feature name
            description: Feature description
            status: Feature status
            priority: Feature priority
            next_prompt: Next step or prompt
            metadata: Optional metadata
            created_by: Creator user ID

        Returns:
            Created feature dict

        Raises:
            HTTPException: On validation, creation, or sync failures
        """
        feature = None

        try:
            # 1. Validate project exists
            logger.info(f"Validating project {project_id}")
            project = await self.project_repo.get_by_id(project_id)
            if not project:
                raise HTTPException(status_code=404, detail="Project not found")

            # Verify project belongs to company
            if project["company_id"] != company_id:
                raise HTTPException(status_code=403, detail="Project does not belong to company")

            # 2. Create feature in PostgreSQL
            logger.info(f"Creating feature '{name}' in PostgreSQL")
            feature = await self.feature_repo.create_feature(
                company_id=company_id,
                project_id=project_id,
                name=name,
                description=description,
                status=status,
                priority=priority,
                next_prompt=next_prompt,
                metadata=metadata or {},
                created_by=created_by
            )
            feature_id = feature["id"]

            # 3. Smart chunking - only chunk if description is substantial
            logger.info(f"Evaluating chunking for feature {feature_id}")
            should_chunk = self._should_chunk_description(description)

            stored_chunks = []
            if should_chunk:
                logger.info(f"Extracting chunks from feature description")
                # Extract chunks using Gemini
                extracted_chunks = await self._extract_chunks_from_description(
                    description=description,
                    feature_name=name
                )

                # Prepare chunks for storage
                all_chunks = []
                for idx, chunk in enumerate(extracted_chunks):
                    chunk_data = {
                        "feature_id": feature_id,
                        "chunk_index": idx,
                        "content": chunk["content"],
                        "summary": chunk.get("summary"),
                        "chunk_type": chunk.get("chunk_type"),
                        "token_count": len(chunk["content"].split()),  # Simple estimate
                        "key_concepts": chunk.get("key_concepts", []),
                        "dependencies": chunk.get("dependencies", []),
                    }
                    all_chunks.append(chunk_data)

                # 4. Store chunks in PostgreSQL
                logger.info(f"Storing {len(all_chunks)} chunks in PostgreSQL")
                stored_chunks = await self.feature_repo.create_chunks(feature_id, all_chunks)

                # Update chunk count
                await self.feature_repo.update_chunk_count(feature_id, len(stored_chunks))

            else:
                logger.info(f"Skipping chunking for feature {feature_id} (description too small)")

            # 5. Sync to Neo4j
            logger.info(f"Syncing feature {feature_id} to Neo4j")
            try:
                await self.sync_service.sync_feature_to_graph(
                    feature_id=feature_id,
                    feature_data=feature
                )

                if stored_chunks:
                    await self.sync_service.sync_chunks_to_graph(stored_chunks)

            except Exception as neo4j_error:
                logger.error("Neo4j sync failed for feature {}: {}", feature_id, neo4j_error)
                await self.sync_service.rollback_feature(feature_id, company_id)
                raise HTTPException(status_code=500, detail="Failed to sync to knowledge graph")

            # 6. Sync to Qdrant (only if chunks exist)
            if stored_chunks:
                logger.info(f"Syncing {len(stored_chunks)} chunks to Qdrant")
                try:
                    await self.sync_service.sync_chunks_to_qdrant(
                        company_id=company_id,
                        chunks=stored_chunks
                    )

                except Exception as qdrant_error:
                    logger.error("Qdrant sync failed for feature {}: {}", feature_id, qdrant_error)
                    await self.sync_service.rollback_feature(feature_id, company_id)
                    raise HTTPException(status_code=500, detail="Failed to sync to vector store")

            logger.info(f"Successfully created feature {feature_id} with {len(stored_chunks)} chunks")

            # Return feature with stats
            feature["chunk_count"] = len(stored_chunks)

            return feature

        except HTTPException:
            # Re-raise HTTP exceptions
            if feature:
                await self.sync_service.rollback_feature(feature["id"], company_id)
            raise

        except Exception as e:
            logger.error("Failed to create feature: {}", e)
            if feature:
                await self.sync_service.rollback_feature(feature["id"], company_id)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_feature(self, feature_id: str) -> Dict[str, Any]:
        """
        Get feature by ID.

        Args:
            feature_id: Feature UUID

        Returns:
            Feature dict

        Raises:
            HTTPException: If feature not found
        """
        feature = await self.feature_repo.get_feature(feature_id)
        if not feature:
            raise HTTPException(status_code=404, detail="Feature not found")

        return feature

    async def list_features(
        self,
        project_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List features for a project.

        Args:
            project_id: Project UUID
            limit: Maximum results
            offset: Results offset

        Returns:
            Dict with features list and total count
        """
        features = await self.feature_repo.list_features(
            project_id=project_id,
            limit=limit,
            offset=offset
        )

        return {
            "features": features,
            "total": len(features)
        }

    async def update_feature(
        self,
        feature_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        next_prompt: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update feature fields.

        Note: Updating description does NOT re-chunk automatically.
        Use a separate re-chunk endpoint if needed.

        Args:
            feature_id: Feature UUID
            name: New name
            description: New description
            status: New status
            priority: New priority
            next_prompt: New next_prompt
            metadata: New metadata

        Returns:
            Updated feature dict

        Raises:
            HTTPException: If feature not found
        """
        feature = await self.feature_repo.update_feature(
            feature_id=feature_id,
            name=name,
            description=description,
            status=status,
            priority=priority,
            next_prompt=next_prompt,
            metadata=metadata
        )

        if not feature:
            raise HTTPException(status_code=404, detail="Feature not found")

        # TODO: If description changed significantly, consider re-chunking
        # For now, just update PostgreSQL

        return feature

    async def delete_feature(self, feature_id: str, company_id: str) -> None:
        """
        Delete feature and rollback all syncs.

        Args:
            feature_id: Feature UUID
            company_id: Company UUID

        Raises:
            HTTPException: If feature not found
        """
        # Verify feature exists
        feature = await self.feature_repo.get_feature(feature_id)
        if not feature:
            raise HTTPException(status_code=404, detail="Feature not found")

        # Rollback uses the same cleanup logic
        await self.sync_service.rollback_feature(feature_id, company_id)

        logger.info(f"Deleted feature {feature_id}")

    async def _extract_chunks_from_description(
        self,
        description: str,
        feature_name: str
    ) -> List[Dict[str, Any]]:
        """
        Extract semantic chunks from feature description using Gemini.

        Args:
            description: Feature description text
            feature_name: Feature name for context

        Returns:
            List of chunk dictionaries with metadata
        """
        prompt = f"""Extract semantic chunks from this feature description.

Feature: {feature_name}

Description:
{description}

Extract chunks based on:
- Logical sections (requirements, acceptance criteria, technical details, examples)
- Markdown headings if present
- Natural paragraph breaks for long text

Return JSON array:
[
  {{
    "content": "chunk text",
    "summary": "1-sentence summary",
    "chunk_type": "requirement|acceptance_criteria|technical_detail|example",
    "key_concepts": ["concept1", "concept2"],
    "dependencies": ["dependency1"]
  }}
]

Requirements:
- Each chunk should be self-contained
- Include ALL original content across chunks
- Summaries should be concise
- Extract key concepts and dependencies
"""

        try:
            response = await self.gemini_model.generate_content_async(prompt)
            result_text = response.text.strip()

            # Extract JSON from markdown code blocks if present
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()

            chunks = json.loads(result_text)
            logger.info(f"Extracted {len(chunks)} chunks from feature description")
            return chunks

        except Exception as e:
            logger.error("Failed to extract chunks with Gemini: {}", e)
            # Fallback: return description as single chunk
            return [{
                "content": description,
                "summary": f"Feature: {feature_name}",
                "chunk_type": "description",
                "key_concepts": [],
                "dependencies": []
            }]

    def _should_chunk_description(self, description: str) -> bool:
        """
        Determine if description should be chunked.

        Criteria:
        - Length > 500 characters OR
        - Has markdown sections (## or ###)

        Args:
            description: Feature description text

        Returns:
            True if should chunk, False otherwise
        """
        # Check length
        if len(description) > 500:
            return True

        # Check for markdown sections
        if re.search(r'^##+ ', description, re.MULTILINE):
            return True

        return False
