"""Cognee pipeline context builder for Neo4j Storage Service.

Provides helpers to build the User, Dataset, and Data objects required by Cognee's
upsert_nodes and upsert_edges methods.
"""

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid5, NAMESPACE_OID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

try:
    from cognee.modules.users.methods import get_default_user
    from cognee.modules.users.models import User
    from cognee.modules.data.models import Data, Dataset
    from cognee.infrastructure.databases.relational import get_relational_engine
    from cognee.infrastructure.engine.models.DataPoint import DataPoint
except Exception:  # pragma: no cover - fallback for unit tests without cognee runtime
    get_default_user = None  # type: ignore[assignment]
    User = Any  # type: ignore[assignment]
    Data = Any  # type: ignore[assignment]
    Dataset = Any  # type: ignore[assignment]
    get_relational_engine = None  # type: ignore[assignment]

    class DataPoint(BaseModel):
        model_config = ConfigDict(extra="allow")

        id: UUID


# DataPoint doesn't allow extra fields — create typed subclasses
# so upsert_nodes can serialize them properly.
class EntityDataPoint(DataPoint):
    """Entity node for Cognee Postgres."""

    name: str = ""
    entity_type: str = ""
    description: str = ""
    company_id: str = ""
    project_id: Optional[str] = None
    metadata: dict = {"index_fields": ["name"]}


class ChunkDataPoint(DataPoint):
    """DocumentChunk node for Cognee Postgres."""

    text: str = ""
    file_path: str = ""
    repository: str = ""
    branch: str = ""
    language: str = ""
    chunk_index: int = 0
    company_id: str = ""
    project_id: Optional[str] = None
    metadata: dict = {"index_fields": ["text"]}


class EntityTypeDataPoint(DataPoint):
    """EntityType node for Cognee Postgres."""

    name: str = ""
    description: str = ""
    metadata: dict = {"index_fields": ["name"]}


class DocumentDataPoint(DataPoint):
    """Document node for Cognee Postgres."""

    name: str = ""
    file_path: str = ""
    raw_data_location: str = ""
    mime_type: str = ""
    repository: str = ""
    branch: str = ""
    company_id: str = ""
    project_id: Optional[str] = None
    metadata: dict = {"index_fields": ["name", "file_path"]}


class TextSummaryDataPoint(DataPoint):
    """TextSummary node for Cognee Postgres."""

    summary_text: str = ""
    chunk_id: str = ""
    chunk_index: int = 0
    company_id: str = ""
    project_id: Optional[str] = None
    file_path: str = ""
    metadata: dict = {"index_fields": ["summary_text"]}


def _scope_key(company_id: str, project_id: str | None) -> str:
    """Return the active graph scope key.

    Document-path events fall back to company_id; code remains project-scoped.
    """
    return project_id or company_id


async def build_cognee_context(
    company_id: str,
    project_id: str | None,
    branch: str = "main",
    content_type: str = "code",
) -> Dict[str, Any]:
    """Build Cognee pipeline context (User, Dataset, Data).

    Creates or fetches the required Cognee entities for Postgres node/edge writes:
    1. Default Cognee user
    2. Dataset (named by project/branch)
    3. Representative Data record

    Args:
        company_id: Company UUID (for tenant isolation)
        project_id: Project UUID (optional for document events)
        branch: Git branch name

    Returns:
        Dict with keys: user, dataset, data (all as Cognee model objects)
    """
    if get_default_user is None or get_relational_engine is None:
        raise RuntimeError("Cognee runtime is unavailable in this environment")

    # 1. Get default user
    user: User = await get_default_user()

    db_engine = get_relational_engine()
    async with db_engine.get_async_session() as session:
        # 2. Dataset — match by name, create if missing
        scope_key = _scope_key(company_id, project_id)
        dataset_suffix = "knowledge" if content_type == "document" or not project_id else "code"
        dataset_name = f"{scope_key}_{branch}_{dataset_suffix}"
        result = await session.execute(select(Dataset).where(Dataset.name == dataset_name))
        dataset = result.scalars().first()

        if dataset is None:
            try:
                dataset = Dataset(
                    id=uuid5(NAMESPACE_OID, dataset_name),
                    name=dataset_name,
                    owner_id=user.id,
                    tenant_id=user.tenant_id,
                )
                session.add(dataset)
                await session.flush()
            except Exception:
                # Race condition — another worker created it
                await session.rollback()
                result = await session.execute(select(Dataset).where(Dataset.name == dataset_name))
                dataset = result.scalars().first()

        # 3. Data — representative record for this project/branch
        data_key = f"{scope_key}_{branch}_{dataset_suffix}"
        content_hash = hashlib.md5(data_key.encode()).hexdigest()
        data_id = uuid5(NAMESPACE_OID, f"{dataset_suffix}_{content_hash}")

        result = await session.execute(select(Data).where(Data.id == data_id))
        data = result.scalars().first()

        if data is None:
            try:
                data = Data(
                    id=data_id,
                    name=f"{scope_key}_{branch}",
                    extension="txt" if dataset_suffix == "knowledge" else "code",
                    mime_type="text/markdown" if dataset_suffix == "knowledge" else "text/x-code",
                    raw_data_location=f"file:///data/cognee/data/{dataset_suffix}_{content_hash}",
                    content_hash=content_hash,
                    owner_id=user.id,
                    tenant_id=user.tenant_id,
                )
                session.add(data)
                await session.flush()
            except Exception:
                await session.rollback()
                result = await session.execute(select(Data).where(Data.id == data_id))
                data = result.scalars().first()

        await session.commit()

    return {
        "user": user,
        "dataset": dataset,
        "data": data,
    }


def convert_entities_to_datapoints(
    entities: List[Dict[str, Any]],
) -> List[DataPoint]:
    """Convert entity dictionaries to Cognee DataPoint objects.

    Args:
        entities: List of entity dicts with entity_id, name, entity_type, etc.

    Returns:
        List of DataPoint objects for upsert_nodes
    """
    datapoints = []
    for entity in entities:
        scope_project_id = entity.get("project_id") or entity.get("company_id", "")
        dp = EntityDataPoint(
            id=UUID(entity["entity_id"]),
            name=entity["name"],
            entity_type=entity.get("entity_type", ""),
            description=entity.get("description", ""),
            company_id=entity.get("company_id", ""),
            project_id=scope_project_id,
        )
        datapoints.append(dp)
    return datapoints


def convert_chunks_to_datapoints(
    chunks: List[Dict[str, Any]],
) -> List[DataPoint]:
    """Convert chunk dictionaries to Cognee DataPoint objects.

    Args:
        chunks: List of chunk dicts with chunk_id, file_path, etc.

    Returns:
        List of DataPoint objects for upsert_nodes
    """
    datapoints = []
    for chunk in chunks:
        scope_project_id = chunk.get("project_id") or chunk.get("company_id", "")
        dp = ChunkDataPoint(
            id=UUID(chunk["chunk_id"]),
            file_path=chunk.get("file_path", ""),
            repository=chunk.get("repository", ""),
            branch=chunk.get("branch", ""),
            language=chunk.get("language", ""),
            chunk_index=chunk.get("chunk_index", 0),
            company_id=chunk.get("company_id", ""),
            project_id=scope_project_id,
        )
        datapoints.append(dp)
    return datapoints


def convert_entity_types_to_datapoints(
    entity_types: List[Dict[str, Any]],
) -> List[DataPoint]:
    """Convert entity type dictionaries to Cognee DataPoint objects.

    Args:
        entity_types: List of entity type dicts with name field

    Returns:
        List of DataPoint objects for upsert_nodes
    """
    datapoints = []
    for et in entity_types:
        # Scope-aware ID: same EntityType name in different node_sets → distinct IDs.
        # Mirror the formula used by neo4j_storage_service.writer._entity_id and
        # entity_extraction_service.models.make_entity_id (`name|node_set`).
        node_set = et.get("node_set", "")
        seed = f"{et['name']}|{node_set}".lower().replace(" ", "_").replace("'", "")
        et_id = uuid5(NAMESPACE_OID, seed)
        dp = EntityTypeDataPoint(
            id=et_id,
            name=et["name"],
        )
        datapoints.append(dp)
    return datapoints


def convert_edges_to_tuples(
    edges: List[Dict[str, Any]],
) -> List[Tuple[UUID, UUID, str, Dict[str, Any]]]:
    """Convert edge dictionaries to Cognee edge tuples.

    Args:
        edges: List of edge dicts with source_id, target_id, relationship_type

    Returns:
        List of tuples (source_id, target_id, label, attributes)
    """
    edge_tuples = []
    for edge in edges:
        source_id = UUID(edge["source_id"])
        target_id = UUID(edge["target_id"])
        label = edge["relationship_type"]
        attributes = edge.get("properties", {})
        edge_tuples.append((source_id, target_id, label, attributes))
    return edge_tuples


def _cognee_id(text: str) -> str:
    """Generate UUID5 matching Cognee's generate_node_id normalisation.

    Must match neo4j_storage_service/writer.py implementation exactly.
    """
    import uuid

    return str(uuid.uuid5(uuid.NAMESPACE_OID, text.lower().replace(" ", "_").replace("'", "")))


def _guess_mime_type(file_path: str) -> str:
    """Guess MIME type from file extension."""
    mime_map = {
        ".py": "text/x-python",
        ".js": "text/javascript",
        ".ts": "text/typescript",
        ".tsx": "text/typescript-jsx",
        ".jsx": "text/javascript-jsx",
        ".json": "application/json",
        ".html": "text/html",
        ".css": "text/css",
        ".md": "text/markdown",
        ".yaml": "text/yaml",
        ".yml": "text/yaml",
        ".xml": "text/xml",
        ".sql": "text/x-sql",
        ".sh": "text/x-shellscript",
    }
    ext = Path(file_path).suffix.lower()
    return mime_map.get(ext, "text/plain")


def convert_documents_to_datapoints(
    chunks: List[Dict[str, Any]],
) -> List[DataPoint]:
    """Convert chunks to unique Document DataPoint objects.

    Extracts unique documents by file_path (matching writer.write_document_nodes logic).
    Uses deterministic UUID5 generation: uuid5(NAMESPACE_OID, "{project_id}:{file_path}").

    Args:
        chunks: List of chunk dicts with file_path, project_id, etc.

    Returns:
        List of DocumentDataPoint objects for upsert_nodes
    """
    # Group by file_path — collect metadata from first chunk per file
    file_map: Dict[str, Dict[str, Any]] = {}
    for c in chunks:
        fp = c.get("file_path", "")
        if not fp:
            continue
        if fp not in file_map:
            file_map[fp] = c

    datapoints = []
    for fp, c in file_map.items():
        project_id = c.get("project_id") or c.get("company_id", "unknown")
        doc_id = _cognee_id(f"{project_id}:{fp}")

        dp = DocumentDataPoint(
            id=UUID(doc_id),
            name=fp,
            file_path=fp,
            raw_data_location=f"{c.get('repository', '')}/{fp}",
            mime_type=_guess_mime_type(fp),
            repository=c.get("repository", ""),
            branch=c.get("branch", ""),
            company_id=c.get("company_id", ""),
            project_id=project_id,
        )
        datapoints.append(dp)

    return datapoints


def convert_summaries_to_datapoints(
    summary_events: List[Any],
) -> List[DataPoint]:
    """Convert TextSummaryEvent objects to Cognee DataPoint objects.

    Args:
        summary_events: List of TextSummaryEvent objects

    Returns:
        List of TextSummaryDataPoint objects for upsert_nodes
    """
    datapoints = []
    for event in summary_events:
        scope_project_id = event.project_id or event.company_id
        dp = TextSummaryDataPoint(
            id=UUID(event.summary_id),
            summary_text=event.summary_text,
            chunk_id=event.chunk_id,
            chunk_index=getattr(event, "chunk_index", 0),
            company_id=event.company_id,
            project_id=scope_project_id,
            file_path=getattr(event, "file_path", ""),
        )
        datapoints.append(dp)

    return datapoints
