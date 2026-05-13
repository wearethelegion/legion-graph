"""
Code Service
Business logic for code operations.
"""

from typing import Dict, Any, List, Optional
import uuid
import hashlib
from fastapi import HTTPException, status
from api.repositories import ProjectRepository, Neo4jRepository, QdrantRepository
from api.auth import CurrentUser
from loguru import logger

from kgrag.code_llm_processor import CodeLLMProcessor
from kgrag.embeddings import GeminiEmbedder

# Configuration
MAX_CODE_SIZE = 500_000  # 500KB max code file size


class CodeService:
    """Service for code business logic."""

    def __init__(
        self,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        project_repository: ProjectRepository,
    ):
        self.project_repository = project_repository
        self.neo4j_repository = neo4j_repository
        self.qdrant_repository = qdrant_repository

        # Initialize LLM processor and embedder
        self.code_processor = CodeLLMProcessor()
        self.embedder = GeminiEmbedder()

    async def create_code(
        self,
        code: str,
        filename: str,
        project_id: str,
        current_user: CurrentUser,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create code: process with LLM, store in Neo4j + Qdrant.

        Args:
            code: Source code content
            filename: Name of the code file
            project_id: Project UUID
            current_user: Current authenticated user
            metadata: Optional metadata (language, tags, etc.)

        Returns:
            Created code response with IDs

        Raises:
            HTTPException: On validation or database errors
        """
        try:
            # Get company_id from project lookup
            if not self.project_repository:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Project repository not initialized",
                )

            project = await self.project_repository.get_by_id(project_id)
            if not project:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
                )

            company_id = project["company_id"]

            # Verify user has access to this company
            if company_id not in current_user.companies:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"User does not have access to company {company_id}",
                )

            # Validate code size to prevent DoS and LLM context overflow
            if len(code) > MAX_CODE_SIZE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Code too large: {len(code)} bytes (max: {MAX_CODE_SIZE} bytes)",
                )

            code_id = str(uuid.uuid4())

            logger.info(
                f"Creating code {code_id} for project {project_id} "
                f"in company {company_id} by user {current_user.email}"
            )

            # Detect language early (needed for Neo4j node and LLM processing)
            language = None
            if metadata and metadata.get("language"):
                language = metadata["language"]  # Allow user override
            else:
                language = self.code_processor._detect_language(filename)

            logger.info(f"Detected language: {language}")

            # Step 0: Check for duplicates by querying Neo4j for content hash
            logger.info("Step 0: Checking for duplicate content")
            content_hash = hashlib.sha256(code.encode()).hexdigest()

            # Check if this exact content already exists
            existing = await self.neo4j_repository.check_duplicate_code(
                project_id=project_id, content_hash=content_hash
            )

            if existing:
                existing_code_id = existing["code_id"]
                existing_title = existing["title"]

                logger.warning(
                    f"Duplicate content detected! Exact match with existing code {existing_code_id}"
                )

                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Duplicate code detected. This exact content already exists as: '{existing_title}' (ID: {existing_code_id})",
                )

            # Step 1: Process with LLM (pass language explicitly)
            logger.info("Step 1: LLM processing (chunking, entities, summary)")
            processed = await self.code_processor.process_code(
                code=code,
                filename=filename,
                language=language,  # Pass detected language
            )

            title = processed["title"]
            summary = processed["summary"]
            chunks = processed["chunks"]
            entities = processed["entities"]
            relationships = processed["relationships"]

            # Step 2: Generate embeddings for chunks
            logger.info(f"Step 2: Generating embeddings for {len(chunks)} chunks")
            chunk_texts = [chunk["content"] for chunk in chunks]
            embeddings = self.embedder.embed_documents(chunk_texts)

            # Step 3: Store in Qdrant (company-level collection)
            logger.info(f"Step 3: Storing {len(chunks)} chunks in Qdrant")
            collection_name = f"company_{company_id}"

            # Ensure collection exists (idempotent operation)
            try:
                await self.qdrant_repository.create_collection(
                    collection_name=collection_name,
                    vector_size=768,  # Gemini embedding dimension
                )
                logger.info(f"Ensured collection {collection_name} exists")
            except Exception as e:
                # Collection likely already exists - log and continue
                # If it's a real error, upsert_points below will fail
                logger.debug(f"Collection creation skipped (may exist): {e}")

            # Store chunk embeddings
            chunk_ids = []
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                chunk_id = str(uuid.uuid4())
                chunk_ids.append(chunk_id)

                # Extract entity info from chunk
                entity_name = None
                entity_type = "code_block"

                # Try to extract entity name from function signature or class name
                if chunk.get("function_signature"):
                    # Extract function name from signature (e.g., "def foo()" -> "foo")
                    sig = chunk["function_signature"]
                    if "def " in sig:
                        entity_name = sig.split("def ")[1].split("(")[0].strip()
                        entity_type = "function"
                    elif "async def " in sig:
                        entity_name = sig.split("async def ")[1].split("(")[0].strip()
                        entity_type = "function"
                elif chunk.get("class_name"):
                    entity_name = chunk["class_name"]
                    entity_type = "class"

                # Build entity_id (filename:entity_name)
                entity_id = f"{filename}:{entity_name}" if entity_name else f"{filename}:chunk_{i}"

                # Enhanced payload with code-specific metadata
                point_payload = {
                    # Core fields
                    "code_id": code_id,
                    "chunk_index": i,
                    "content": chunk["content"],
                    "summary": chunk.get("summary", ""),
                    "company_id": company_id,
                    "project_id": project_id,
                    "user_id": current_user.user_id,
                    # Entity fields (required for findSimilarCode)
                    "entity_id": entity_id,
                    "entity_name": entity_name or f"chunk_{i}",
                    "entity_type": entity_type,
                    # Code-specific fields
                    "filename": filename,
                    "language": language,
                    "chunk_type": chunk.get("chunk_type", "utility"),
                    "complexity": chunk.get("complexity", "medium"),
                    "entry_point": chunk.get("entry_point", False),
                    "keywords": chunk.get("keywords", []),
                    "function_signature": chunk.get("function_signature"),
                    "class_name": chunk.get("class_name"),
                    "decorators": chunk.get("decorators", []),
                    # Metadata type to distinguish from knowledge
                    "metadata_type": "code",
                }

                await self.qdrant_repository.upsert_points(
                    collection_name=collection_name,
                    points=[{"id": chunk_id, "vector": embedding, "payload": point_payload}],
                )

            # Step 4: Store in Neo4j
            logger.info("Step 4: Storing code graph in Neo4j")

            # Create Code node with content hash for deduplication
            await self.neo4j_repository.create_code_node(
                code_id=code_id,
                company_id=company_id,
                project_id=project_id,
                filename=filename,
                language=language,
                title=title,
                content=code,
                metadata=metadata or {},
                content_hash=content_hash,
            )

            # Create CodeChunk nodes with rich metadata
            chunk_nodes = [
                {
                    "id": chunk_ids[i],
                    "code_id": code_id,
                    "chunk_index": i,
                    "content": chunk["content"],
                    "summary": chunk.get("summary"),
                    "token_count": len(chunk["content"].split()),
                    "qdrant_point_id": chunk_ids[i],
                    # Code-specific metadata
                    "chunk_type": chunk.get("chunk_type", "utility"),
                    "complexity": chunk.get("complexity", "medium"),
                    "entry_point": chunk.get("entry_point", False),
                    "keywords": chunk.get("keywords", []),
                    "function_signature": chunk.get("function_signature"),
                    "class_name": chunk.get("class_name"),
                    "decorators": chunk.get("decorators", []),
                }
                for i, chunk in enumerate(chunks)
            ]

            await self.neo4j_repository.create_code_chunk_nodes(chunk_nodes)

            # Create Entity nodes
            if entities:
                await self.neo4j_repository.create_code_entities(code_id=code_id, entities=entities)

            # Create relationships between entities
            if relationships:
                await self.neo4j_repository.create_code_relationships(relationships=relationships)

            logger.info(f"Code {code_id} created successfully")

            return {
                "status": "success",
                "code_id": code_id,
                "filename": filename,
                "language": language,
                "title": title,
                "summary": summary,
                "chunks_count": len(chunks),
                "entities_count": len(entities),
                "relationships_count": len(relationships),
            }

        except HTTPException:
            raise
        except Exception as e:
            error_msg = str(e)
            logger.error("Code creation failed: {}", error_msg, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Code creation failed: {error_msg}",
            )
