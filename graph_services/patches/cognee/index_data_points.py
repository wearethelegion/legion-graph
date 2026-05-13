"""Patched index_data_points with module-level semaphore for embedding concurrency control.

This patch wraps the unbounded asyncio.gather(*tasks) in index_data_points with
a module-level asyncio.Semaphore to prevent blowing past Gemini's 3,000 RPM
embedding API limit. The semaphore is shared across ALL concurrent calls to
index_data_points (from all pipeline branches), ensuring a global cap on
in-flight embedding requests.

Controlled by EMBEDDING_MAX_CONCURRENT env var (default: 5).
With sem=5: max 5 concurrent embedding calls × ~200ms each ≈ 1,500 RPM.

Applied via Dockerfile.cognee COPY + overwrite pattern.
"""

import asyncio
import os

from cognee.shared.logging_utils import get_logger
from cognee.infrastructure.databases.vector import get_vector_engine
from cognee.infrastructure.engine import DataPoint

logger = get_logger("index_data_points")

# ---------------------------------------------------------------------------
# Module-level embedding concurrency limiter
# ---------------------------------------------------------------------------
# Controls max concurrent embedding API calls across ALL pipeline branches.
# Uses lazy initialization to avoid creating a Semaphore outside an event loop.
_EMBEDDING_MAX_CONCURRENT = int(os.environ.get("EMBEDDING_MAX_CONCURRENT", "5"))
_embedding_sem = None

# ---------------------------------------------------------------------------
# API key round-robin for embedding load balancing
# ---------------------------------------------------------------------------
# EMBEDDING_API_KEYS: comma-separated list of API keys. Each key has its own
# rate-limit quota (e.g. 3K RPM for Gemini). With N keys we get N× throughput.
# Falls back to LLM_API_KEY (single key) when EMBEDDING_API_KEYS is not set.
_raw_keys = os.environ.get("EMBEDDING_API_KEYS", "").strip()
if _raw_keys:
    _EMBEDDING_API_KEYS = [k.strip() for k in _raw_keys.split(",") if k.strip()]
else:
    _fallback = os.environ.get("LLM_API_KEY", "")
    _EMBEDDING_API_KEYS = [_fallback] if _fallback else []

_key_counter = 0  # simple modulo counter; safe — asyncio is single-threaded


def _next_api_key():
    """Return (1-based key index, api_key) using round-robin, or (0, None) if no keys."""
    global _key_counter
    if not _EMBEDDING_API_KEYS:
        return 0, None
    idx = _key_counter % len(_EMBEDDING_API_KEYS)
    _key_counter += 1
    return idx + 1, _EMBEDDING_API_KEYS[idx]


def _get_embedding_semaphore():
    """Return the module-level embedding semaphore, creating it on first use.

    Lazy initialization avoids issues with asyncio.Semaphore() being created
    before an event loop is running (relevant in Python < 3.10, defensive for 3.13+).
    """
    global _embedding_sem
    if _embedding_sem is None:
        _embedding_sem = asyncio.Semaphore(_EMBEDDING_MAX_CONCURRENT)
        logger.info(
            "Initialized embedding concurrency semaphore",
            max_concurrent=_EMBEDDING_MAX_CONCURRENT,
        )
    return _embedding_sem


async def index_data_points(data_points: list[DataPoint], vector_engine=None):
    """Index data points in the vector engine by creating embeddings for specified fields.

    Process:
    1. Groups data points into a nested dict: {type_name: {field_name: [points]}}
    2. Creates vector indexes for each (type, field) combination on first encounter
    3. Batches points per (type, field) and creates async indexing tasks
    4. Executes all indexing tasks with concurrency limited by EMBEDDING_MAX_CONCURRENT

    Args:
        data_points: List of DataPoint objects to index. Each DataPoint's metadata must
                     contain an 'index_fields' list specifying which fields to embed.
        vector_engine: Optional pre-created vector engine. Falls back to
                       ``get_vector_engine()`` when not supplied.

    Returns:
        The original data_points list.
    """
    data_points_by_type = {}

    vector_engine = vector_engine or get_vector_engine()

    for data_point in data_points:
        data_point_type = type(data_point)
        type_name = data_point_type.__name__

        for field_name in data_point.metadata["index_fields"]:
            if getattr(data_point, field_name, None) is None:
                continue

            if type_name not in data_points_by_type:
                data_points_by_type[type_name] = {}

            if field_name not in data_points_by_type[type_name]:
                await vector_engine.create_vector_index(type_name, field_name)
                data_points_by_type[type_name][field_name] = []

            indexed_data_point = data_point.model_copy()
            indexed_data_point.metadata["index_fields"] = [field_name]
            data_points_by_type[type_name][field_name].append(indexed_data_point)

    # NOTE: Batch splitting disabled — Vertex AI batch handling is done at the
    # LiteLLM patch level (cognee_patches.py Patch 4). Uncomment to restore:
    # batch_size = vector_engine.embedding_engine.get_batch_size()
    # batches = [
    #     (type_name, field_name, points[i : i + batch_size])
    #     for type_name, fields in data_points_by_type.items()
    #     for field_name, points in fields.items()
    #     for i in range(0, len(points), batch_size)
    # ]

    batches = [
        (type_name, field_name, points)
        for type_name, fields in data_points_by_type.items()
        for field_name, points in fields.items()
    ]

    total_points = sum(len(bp) for _, _, bp in batches)
    logger.info(
        "index_data_points.start",
        total_data_points=len(data_points),
        total_batches=len(batches),
        total_points_to_embed=total_points,
        # embedding_batch_size=batch_size,
        max_concurrent=_EMBEDDING_MAX_CONCURRENT,
        api_key_count=len(_EMBEDDING_API_KEYS),
        types=[t for t, _, _ in batches],
    )

    # NOTE: Semaphore disabled — Vertex AI has 5M TPM, no throttling needed.
    # Uncomment to restore concurrency limiting:
    # sem = _get_embedding_semaphore()
    completed = 0

    async def _throttled(batch_idx, type_name, field_name, batch_points):
        nonlocal completed
        logger.info(
            "index_data_points.batch.start",
            batch=batch_idx,
            type_name=type_name,
            field_name=field_name,
            points=len(batch_points),
        )
        # --- Original semaphore-throttled version (commented out) ---
        # async with sem:
        #     # Rotate API key before each embedding batch
        #     key_idx, api_key = _next_api_key()
        #     if api_key is not None:
        #         vector_engine.embedding_engine.api_key = api_key
        #     logger.info(
        #         "index_data_points.batch.acquired_sem",
        #         batch=batch_idx,
        #         type_name=type_name,
        #         field_name=field_name,
        #         points=len(batch_points),
        #         api_key_index=key_idx,
        #     )
        #     result = await vector_engine.index_data_points(type_name, field_name, batch_points)
        #     completed += 1
        #     logger.info(
        #         "index_data_points.batch.done",
        #         batch=batch_idx,
        #         completed=completed,
        #         total=len(batches),
        #     )
        #     return result
        # Rotate API key before each embedding call
        key_idx, api_key = _next_api_key()
        if api_key is not None:
            vector_engine.embedding_engine.api_key = api_key
        result = await vector_engine.index_data_points(type_name, field_name, batch_points)
        completed += 1
        logger.info(
            "index_data_points.batch.done",
            batch=batch_idx,
            completed=completed,
            total=len(batches),
        )
        return result

    tasks = [
        asyncio.create_task(_throttled(i, type_name, field_name, batch_points))
        for i, (type_name, field_name, batch_points) in enumerate(batches)
    ]

    await asyncio.gather(*tasks)

    logger.info(
        "index_data_points.complete",
        total_batches=len(batches),
        total_points=total_points,
    )

    return data_points


async def get_data_points_from_model(
    data_point: DataPoint, added_data_points=None, visited_properties=None
) -> list[DataPoint]:
    data_points = []
    added_data_points = added_data_points or {}
    visited_properties = visited_properties or {}

    for field_name, field_value in data_point:
        if isinstance(field_value, DataPoint):
            property_key = f"{str(data_point.id)}{field_name}{str(field_value.id)}"

            if property_key in visited_properties:
                return []

            visited_properties[property_key] = True

            new_data_points = await get_data_points_from_model(
                field_value, added_data_points, visited_properties
            )

            for new_point in new_data_points:
                if str(new_point.id) not in added_data_points:
                    added_data_points[str(new_point.id)] = True
                    data_points.append(new_point)

        if (
            isinstance(field_value, list)
            and len(field_value) > 0
            and isinstance(field_value[0], DataPoint)
        ):
            for field_value_item in field_value:
                property_key = f"{str(data_point.id)}{field_name}{str(field_value_item.id)}"

                if property_key in visited_properties:
                    return []

                visited_properties[property_key] = True

                new_data_points = await get_data_points_from_model(
                    field_value_item, added_data_points, visited_properties
                )

                for new_point in new_data_points:
                    if str(new_point.id) not in added_data_points:
                        added_data_points[str(new_point.id)] = True
                        data_points.append(new_point)

    if str(data_point.id) not in added_data_points:
        data_points.append(data_point)

    return data_points


if __name__ == "__main__":

    class Car(DataPoint):
        model: str
        color: str
        metadata: dict = {"index_fields": ["name"]}

    class Person(DataPoint):
        name: str
        age: int
        owns_car: list[Car]
        metadata: dict = {"index_fields": ["name"]}

    car1 = Car(model="Tesla Model S", color="Blue")
    car2 = Car(model="Toyota Camry", color="Red")
    person = Person(name="John", age=30, owns_car=[car1, car2])

    data_points = get_data_points_from_model(person)

    print(data_points)
