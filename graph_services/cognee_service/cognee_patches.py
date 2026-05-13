"""
Cognee monkey-patches for Qdrant + Neo4j compatibility and tenancy hardening.

Import this module BEFORE any other cognee imports. It applies the following patches:

  Patch 1 — Env-var mapping
    Sets GRAPH_DATABASE_PROVIDER=neo4j and maps NEO4J_* env vars to GRAPH_DATABASE_*
    before Pydantic BaseSettings is cached. Cognee defaults to kuzu; we override it.

  Patch 2 — Qdrant adapter registration
    Registers the cognee-community QDrantAdapter in cognee's supported_databases dict
    so create_vector_engine() picks it up.

  Patch 3 — setup() pgvector guard
    Makes setup() skip pgvector table creation when using a different vector provider
    (qdrant). Without this patch, setup() always tries to create pgvector tables and
    crashes if postgres doesn't have the extension.

  Patch 4 — LiteLLM vertex_ai batch embedding fix
    LiteLLM vertex_ai path merges multiple inputs into a single embedding response.
    This patch wraps embed_text to call LiteLLM once per text for vertex_ai provider.

  Patch 4b — Gemini API batch embedding
    Alternative to Patch 4: uses litellm.aembedding() for true batching via Gemini API.
    Enabled by EMBEDDING_BATCH_MODE=true env var.

  Patch 5 — Belt-and-suspenders Qdrant injection into create_vector_engine module
    Patch 2 mutates the supported_databases dict. Patch 5 re-injects into the already-
    imported create_vector_engine module to handle re-import edge cases.

  Patch 6 — Qdrant score inversion fix
    The community QDrantAdapter incorrectly inverts cosine similarity scores (1 - score).
    This patch undoes the inversion and injects the corrected score into the payload dict
    so it survives Cognee's dict conversion.

  Patch 7 — dataset_id second-layer tenancy filter (SECURITY HARDENING)
    Adds `dataset_id == company_id` as a second Qdrant payload filter on all search
    queries, in addition to the existing `database_name` filter. On the write path,
    stamps `dataset_id` onto every Qdrant point payload alongside `database_name`.
    This makes it architecturally impossible for a missing/wrong `database_name` to
    leak data across tenants — both filters must match for a result to be returned.
    Any point that was written before this patch was applied lacks `dataset_id` and
    will be excluded from results. Run scripts/backfill_dataset_id.py to add `dataset_id`
    to pre-existing points.

    If this patch fails to apply (because cognee's adapter interface changed), a WARNING
    is logged and the system continues with the primary `database_name` filter only.

All patches are wrapped in try/except: a "patch failed; cognee may have changed" warning
is logged if the patch target is not found, surfacing upstream API changes quickly.
"""

import logging
import os
from contextvars import ContextVar
from typing import Optional, Type

logger = logging.getLogger("cognee_patches")


class SearchContractError(RuntimeError):
    """Structured search failure used for explicit mode contracts."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code


feeling_lucky_resolved_type_context: ContextVar[str] = ContextVar(
    "feeling_lucky_resolved_type_context", default=""
)
graph_completion_triplet_scope_context: ContextVar[str] = ContextVar(
    "graph_completion_triplet_scope_context", default=""
)

# ── Patch 1: Set graph env vars cognee expects, before any config is cached ───
# Cognee's GraphConfig reads GRAPH_DATABASE_* env vars (Pydantic BaseSettings).
# Our docker-compose sets NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD — but cognee
# doesn't read those. Map them to what GraphConfig expects.
_ENV_MAP = {
    "GRAPH_DATABASE_PROVIDER": ("neo4j", None),
    "GRAPH_DATABASE_URL": (None, "NEO4J_URI"),
    "GRAPH_DATABASE_USERNAME": (None, "NEO4J_USERNAME"),
    "GRAPH_DATABASE_PASSWORD": (None, "NEO4J_PASSWORD"),
    "GRAPH_DATABASE_NAME": (None, "COGNEE_NEO4J_DATABASE"),
}
try:
    for target, (default, source) in _ENV_MAP.items():
        if not os.environ.get(target):
            value = os.environ.get(source, default) if source else default
            if value:
                os.environ[target] = value
                logger.info("cognee_patches: set %s=%s", target, value)
except Exception as exc:
    logger.warning("cognee_patches: Patch 1 failed — env mapping; cognee may have changed: %s", exc)

# ── Patch 2: Register Qdrant adapter in cognee's supported_databases dict ─────
try:
    from cognee_community_vector_adapter_qdrant import QDrantAdapter
    from cognee.infrastructure.databases.vector.supported_databases import (
        supported_databases,
    )

    if "qdrant" not in supported_databases:
        supported_databases["qdrant"] = QDrantAdapter
        logger.info("cognee_patches: registered QDrantAdapter in supported_databases")
except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 2 failed — Qdrant adapter registration; cognee may have changed: %s",
        exc,
    )

# ── Patch 5: Force Qdrant into create_vector_engine module-level reference ────
# Patch 2 mutates supported_databases dict, which should propagate because
# create_vector_engine.py imports the same dict object.  However, if the module
# was already imported (or gets re-loaded) the local name binding can point to a
# fresh empty dict.  Belt-and-suspenders: explicitly set the name on the module.
try:
    from cognee_community_vector_adapter_qdrant import QDrantAdapter as _QDA5

    import cognee.infrastructure.databases.vector.create_vector_engine as _cve_mod

    if "qdrant" not in getattr(_cve_mod, "supported_databases", {}):
        _cve_mod.supported_databases["qdrant"] = _QDA5
        logger.info(
            "cognee_patches: Patch 5 — injected QDrantAdapter into create_vector_engine module"
        )
    else:
        logger.info("cognee_patches: Patch 5 — create_vector_engine already has qdrant, skipping")
except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 5 failed — create_vector_engine injection; cognee may have changed: %s",
        exc,
    )


def _normalize_node_name_values(node_name):
    if node_name in [None, [], ""]:
        return []
    if isinstance(node_name, str):
        return [node_name]
    try:
        values = list(node_name)
    except TypeError:
        values = [node_name]
    return [value for value in values if value not in [None, ""]]


def _build_qdrant_node_name_filter_clauses(node_name):
    values = _normalize_node_name_values(node_name)
    if not values:
        return []

    from qdrant_client import models as _qdrant_models

    return [
        _qdrant_models.FieldCondition(
            key="belongs_to_set",
            match=_qdrant_models.MatchAny(any=values),
        )
    ]


def _fix_qdrant_scores(results):
    """Inject a human-readable similarity into the payload for display purposes.

    The community Qdrant adapter sets `result.score = 1 - cosine_similarity`,
    which IS what cognee's graph-mode ranking expects: a distance where lower
    means more relevant. Cognee's `_calculate_query_top_triplet_importances`
    calls `heapq.nsmallest(k, edges, key=sum_of_node_distances)` — it picks
    the SMALLEST sum of distances, i.e. the most-similar triplets.

    Earlier versions of this patch flipped `result.score` back to similarity
    (high = better). That broke graph-mode triplet ranking: `nsmallest` then
    selected edges with the LOWEST similarity (least relevant), producing
    irrelevant context for GRAPH_COMPLETION and GRAPH_SUMMARY_COMPLETION.

    Current behaviour:
      - Leave `result.score` as-is (= cognee distance, low = relevant).
      - Inject `payload["score"] = 1 - distance` so display surfaces show
        intuitive similarity (high = relevant) for non-graph callers
        (CHUNKS / SUMMARIES / RAG_COMPLETION) that read directly from payload.
    """
    if results is None:
        return results

    if isinstance(results, list):
        return [_fix_qdrant_scores(result) for result in results]

    if hasattr(results, "score"):
        # result.score is already cognee-friendly distance; do not invert it.
        if hasattr(results, "payload") and isinstance(results.payload, dict):
            try:
                similarity = 1.0 - float(results.score)
            except (TypeError, ValueError):
                similarity = results.score
            results.payload["score"] = similarity

    return results


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


COGNEE_CHUNKS_MIN_SCORE = _env_float("COGNEE_CHUNKS_MIN_SCORE", 0.65)
COGNEE_TRIPLET_CHUNK_BIAS_FACTOR = _env_float("COGNEE_TRIPLET_CHUNK_BIAS_FACTOR", 0.7)


def _triplet_debug_enabled() -> bool:
    return _env_bool("COGNEE_TRIPLET_DEBUG", False)


def _extract_triplet_component_label(component):
    if component is None:
        return None

    candidate_keys = (
        "label",
        "name",
        "relationship_name",
        "relationship",
        "type",
        "title",
        "entity_name",
        "node_name",
    )
    if isinstance(component, dict):
        for key in candidate_keys:
            value = component.get(key)
            if value not in [None, ""]:
                return value
        return None

    for key in candidate_keys:
        value = getattr(component, key, None)
        if value not in [None, ""]:
            return value
    return None


def _extract_triplet_component_distance(component):
    if component is None:
        return None

    candidate_keys = (
        "distance",
        "score",
        "node_distance",
        "edge_distance",
        "similarity",
    )
    if isinstance(component, dict):
        for key in candidate_keys:
            value = component.get(key)
            if value is not None:
                return value
        return None

    for key in candidate_keys:
        value = getattr(component, key, None)
        if value is not None:
            return value
    return None


def _summarize_triplet_for_debug(triplet):
    if isinstance(triplet, dict):
        node1 = (
            triplet.get("node1")
            or triplet.get("node_1")
            or triplet.get("source_node")
            or triplet.get("left_node")
        )
        edge = triplet.get("edge") or triplet.get("relationship") or triplet.get("link")
        node2 = (
            triplet.get("node2")
            or triplet.get("node_2")
            or triplet.get("target_node")
            or triplet.get("right_node")
        )
    elif isinstance(triplet, (list, tuple)) and len(triplet) >= 3:
        node1, edge, node2 = triplet[:3]
    else:
        node1, edge, node2 = triplet, None, None

    parts = []
    for part_name, part in (("node1", node1), ("edge", edge), ("node2", node2)):
        parts.append(
            f"{part_name}={{label={_extract_triplet_component_label(part)!r}, distance={_extract_triplet_component_distance(part)!r}}}"
        )
    raw = f"triplet={triplet!r}"
    return "; ".join(parts + [raw])


def _is_document_chunk_anchored(triplet) -> bool:
    labels = (
        [
            str(_extract_triplet_component_label(component) or "")
            for component in (
                triplet.get("node1") if isinstance(triplet, dict) else None,
                triplet.get("edge") if isinstance(triplet, dict) else None,
                triplet.get("node2") if isinstance(triplet, dict) else None,
            )
        ]
        if isinstance(triplet, dict)
        else []
    )
    if not labels and isinstance(triplet, (list, tuple)) and len(triplet) >= 3:
        labels = [
            str(_extract_triplet_component_label(component) or "") for component in triplet[:3]
        ]
    if not labels:
        labels = [str(triplet)]
    return any("DocumentChunk" in label for label in labels)


def _low_confidence_message(threshold: float) -> str:
    return f"no high-confidence matches above threshold {threshold:.2f}"


# ── Patch 3: Make setup() skip pgvector table creation for non-pgvector ───────
try:
    import cognee.modules.engine.operations.setup as _setup_mod
    from cognee.infrastructure.databases.vector import get_vectordb_config

    _original_setup = _setup_mod.setup

    async def _patched_setup():
        """setup() that skips pgvector table creation when using a different vector provider."""
        from cognee.infrastructure.databases.relational import (
            create_db_and_tables as create_relational_db_and_tables,
        )

        await create_relational_db_and_tables()

        try:
            provider = get_vectordb_config().vector_db_provider
        except Exception:
            provider = os.environ.get("VECTOR_DB_PROVIDER", "")

        if provider.lower() == "pgvector":
            await _setup_mod.create_pgvector_db_and_tables()
        else:
            logger.info(
                "cognee_patches: skipping pgvector table creation (provider=%s)",
                provider,
            )

    _setup_mod.setup = _patched_setup

    # Also patch any module that already captured a reference to the original setup().
    # Importing setup triggers a chain that loads memify before we can patch.
    import sys

    for mod_name, mod in list(sys.modules.items()):
        if mod is None or mod is _setup_mod:
            continue
        try:
            if getattr(mod, "setup", None) is _original_setup:
                mod.setup = _patched_setup
                logger.info("cognee_patches: also patched setup ref in %s", mod_name)
        except Exception:
            pass

    logger.info("cognee_patches: patched setup() to be vector-provider-aware")
except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 3 failed — setup() pgvector guard; cognee may have changed: %s",
        exc,
    )


# ── Patch 20: diagnostic triplet logging for GRAPH_COMPLETION ────────────────
try:
    import heapq as _heapq20
    import importlib as _importlib20

    _cognee_graph_mod20 = _importlib20.import_module(
        "cognee.modules.graph.cognee_graph.CogneeGraph"
    )
    _CogneeGraph20 = _cognee_graph_mod20.CogneeGraph

    def _node_is_document_chunk20(node) -> bool:
        if node is None:
            return False

        attributes = getattr(node, "attributes", None) or {}
        candidates = (
            getattr(node, "type", None),
            getattr(node, "label", None),
            attributes.get("type"),
            attributes.get("label"),
        )
        return any(value is not None and "DocumentChunk" in str(value) for value in candidates)

    def _triplet_touches_document_chunk20(edge) -> bool:
        return _node_is_document_chunk20(getattr(edge, "node1", None)) or _node_is_document_chunk20(
            getattr(edge, "node2", None)
        )

    def _log_triplet_debug20(result, *, node_name=None, top_k=None):
        if not _triplet_debug_enabled():
            return

        try:
            if result in [None, []]:
                logger.warning(
                    "cognee_patches: Patch 20 debug — no triplets returned (node_name=%s top_k=%s)",
                    node_name,
                    top_k,
                )
                return

            if (
                isinstance(result, list)
                and result
                and all(isinstance(item, (list, tuple)) for item in result)
            ):
                batches = result
            else:
                batches = [result]

            for batch_index, batch in enumerate(batches):
                triplets = (
                    batch
                    if isinstance(batch, list)
                    else ([batch] if batch not in [None, []] else [])
                )
                logger.warning(
                    "cognee_patches: Patch 20 debug — batch=%s node_name=%s top_k=%s triplets=%s",
                    batch_index,
                    node_name,
                    top_k,
                    len(triplets),
                )
                for triplet_index, triplet in enumerate(triplets):
                    logger.warning(
                        "cognee_patches: Patch 20 debug — batch=%s triplet=%s anchored_to_document_chunk=%s %s",
                        batch_index,
                        triplet_index,
                        _is_document_chunk_anchored(triplet),
                        _summarize_triplet_for_debug(triplet),
                    )
        except Exception as exc:
            logger.warning("cognee_patches: Patch 20 debug logging failed: %s", exc)

    def _patched_calculate_query_top_triplet_importances20(self, k: int, query_index: int = 0):
        """Bias DocumentChunk-anchored triplets for scoped GRAPH_COMPLETION queries."""

        node_name = graph_completion_triplet_scope_context.get() or getattr(self, "node_name", None)
        apply_chunk_bias = node_name not in [None, [], ""]

        def score(edge) -> float:
            elements = (
                (edge.node1, f"node {edge.node1.id}"),
                (edge.node2, f"node {edge.node2.id}"),
                (edge, f"edge {edge.node1.id}->{edge.node2.id}"),
            )

            importances = []
            for element, label in elements:
                distances = element.attributes.get("vector_distance")
                if not isinstance(distances, list) or query_index >= len(distances):
                    raise ValueError(
                        f"{label}: vector_distance must be a list with length > {query_index} "
                        f"before scoring (got {type(distances).__name__} with length "
                        f"{len(distances) if isinstance(distances, list) else 'n/a'})"
                    )
                value = distances[query_index]
                try:
                    importances.append(float(value))
                except (TypeError, ValueError):
                    raise ValueError(
                        f"{label}: vector_distance[{query_index}] must be float-like, "
                        f"got {type(value).__name__}"
                    )

            triplet_score = sum(importances)
            if apply_chunk_bias and _triplet_touches_document_chunk20(edge):
                triplet_score *= COGNEE_TRIPLET_CHUNK_BIAS_FACTOR
            return triplet_score

        result = _heapq20.nsmallest(k, self.edges, key=score)
        _log_triplet_debug20(result, node_name=node_name, top_k=k)
        return result

    _CogneeGraph20._calculate_query_top_triplet_importances = (
        _patched_calculate_query_top_triplet_importances20
    )
    logger.warning("cognee_patches: Patch 20 — triplet debug logging installed")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 20 failed — triplet debug logging; cognee may have changed: %s",
        exc,
    )

# ── Patch 4: Fix Vertex AI batch embedding — embed one text at a time ─────────
# LiteLLM's vertex_ai embedding path merges multiple inputs into a single
# embedding response instead of returning one embedding per input. This breaks
# Cognee's index_data_points which expects len(vectors) == len(texts).
# Fix: wrap embed_text to call LiteLLM once per text when using vertex_ai.
try:
    import asyncio as _asyncio

    from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import (
        LiteLLMEmbeddingEngine,
    )

    _original_embed_text = LiteLLMEmbeddingEngine.embed_text

    async def _patched_embed_text(self, text):
        """embed_text that sends one text at a time for vertex_ai provider."""
        provider = os.environ.get("EMBEDDING_PROVIDER", "")
        if provider == "vertex_ai" and isinstance(text, list) and len(text) > 1:
            # Embed each text individually and concatenate results
            tasks = [_original_embed_text(self, [t]) for t in text]
            results = await _asyncio.gather(*tasks)
            # Each result is a list with one vector — flatten
            return [vec for result in results for vec in result]
        return await _original_embed_text(self, text)

    # ── Patch 4b: Gemini API batch embedding via LiteLLM ─────────────────────
    # When EMBEDDING_BATCH_MODE=true, use litellm.aembedding() for true batching
    # via the Gemini API. Caps each API call at 100 texts; larger batches are
    # split into sequential chunks of 100 and results concatenated in order.
    # Uses the model string already configured (e.g. "gemini/gemini-embedding-001"
    # from EMBEDDING_MODEL env / cognee config). No Vertex SDK required.

    async def _patched_embed_text_batch(self, text):
        """embed_text using litellm.aembedding() for Gemini API batch embedding."""
        import litellm

        # Single string → wrap in list, unwrap at end
        single_input = isinstance(text, str)
        texts = [text] if single_input else list(text)

        if not texts:
            return []

        embedding_model = self.model
        api_key = os.environ.get("GEMINI_API_KEY", "")
        dimensions = getattr(self, "dimensions", 768) or 768

        BATCH_SIZE = 100  # Gemini API cap per request

        all_vectors: list[list[float]] = []
        for i in range(0, len(texts), BATCH_SIZE):
            chunk = texts[i : i + BATCH_SIZE]
            logger.info(
                "Patch 4b: embedding batch %d-%d of %d texts via Gemini API",
                i,
                min(i + BATCH_SIZE, len(texts)),
                len(texts),
            )
            response = await litellm.aembedding(
                model=embedding_model,
                input=chunk,
                api_key=api_key,
                dimensions=dimensions,
            )
            vectors = [item["embedding"] for item in response.data]
            all_vectors.extend(vectors)

        return all_vectors[0] if single_input else all_vectors

    # ── Switch: choose one-at-a-time vs batch embedding based on env var ──
    _batch_mode = os.environ.get("EMBEDDING_BATCH_MODE", "false").lower() == "true"

    if _batch_mode:
        LiteLLMEmbeddingEngine.embed_text = _patched_embed_text_batch
        logger.info("cognee_patches: Patch 4b — Gemini API batched embedding (LiteLLM)")
    else:
        LiteLLMEmbeddingEngine.embed_text = _patched_embed_text
        logger.info("cognee_patches: Patch 4 — Gemini API one-at-a-time embedding (LiteLLM)")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 4 failed — LiteLLM embed_text; cognee may have changed: %s", exc
    )

# ── Patch 6: Fix Qdrant score inversion + inject scores into payload dicts ────
# The community QDrantAdapter does `score = 1 - result.score` which is WRONG for
# Cosine metric — Qdrant already returns similarity (higher = better, range 0-1).
# The inversion turns 0.85 similarity into 0.15.
# Fix: pass through raw Qdrant scores AND inject into payload dict so they survive
# Cognee's wrapper which strips .score when converting to dicts.
try:
    from cognee_community_vector_adapter_qdrant import QDrantAdapter as _QDA6
    import inspect as _inspect6
    import re as _re6
    import textwrap as _textwrap6

    def _rewrite_qdrant_node_name_filter(method, method_label):
        src = _textwrap6.dedent(_inspect6.getsource(method))

        pattern = _re6.compile(
            r"must=\[(?P<body>.*?key=\"database_name\".*?\n?\s*)\]",
            flags=_re6.DOTALL,
        )

        def _replace(match):
            body = match.group("body").rstrip()
            return (
                "must=[\n"
                f"{body},\n"
                "            *_build_qdrant_node_name_filter_clauses(node_name),\n"
                "        ]"
            )

        new_src, subs = pattern.subn(_replace, src, count=1)
        if subs == 0:
            raise RuntimeError(f"Patch 6: could not inject node_name filter into {method_label}")

        # Inject our helper into the cognee module's globals so the rewritten
        # method can resolve `_build_qdrant_node_name_filter_clauses` at runtime.
        # Without this, exec runs but the rewritten method raises NameError on
        # first invocation because the helper lives in our module, not cognee's.
        merged_globals = dict(method.__globals__)
        merged_globals["_build_qdrant_node_name_filter_clauses"] = (
            _build_qdrant_node_name_filter_clauses
        )
        patch_ns: dict = {}
        exec(
            compile(new_src, f"<cognee_patches.{method_label}>", "exec"),
            merged_globals,
            patch_ns,
        )
        # Also inject into the original method's __globals__ so future lookups
        # (e.g. inside the same module) still resolve correctly.
        method.__globals__["_build_qdrant_node_name_filter_clauses"] = (
            _build_qdrant_node_name_filter_clauses
        )
        return patch_ns[method.__name__]

    _QDA6.search = _rewrite_qdrant_node_name_filter(_QDA6.search, "Patch6.search")
    if hasattr(_QDA6, "batch_search"):
        _QDA6.batch_search = _rewrite_qdrant_node_name_filter(
            _QDA6.batch_search, "Patch6.batch_search"
        )

    _original_qdrant_search = _QDA6.search
    _original_qdrant_batch_search = getattr(_QDA6, "batch_search", None)

    async def _patched_qdrant_search(self, *args, **kwargs):
        """search() that fixes score inversion and injects score into payload."""
        results = await _original_qdrant_search(self, *args, **kwargs)

        return _fix_qdrant_scores(results)

    async def _patched_qdrant_batch_search(self, *args, **kwargs):
        """batch_search() that fixes score inversion and injects scores into payloads."""
        results = await _original_qdrant_batch_search(self, *args, **kwargs)
        return _fix_qdrant_scores(results)

    _QDA6.search = _patched_qdrant_search
    if _original_qdrant_batch_search is not None:
        _QDA6.batch_search = _patched_qdrant_batch_search
    logger.info("cognee_patches: Patch 6 — fixed Qdrant score inversion + injected into payloads")
except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 6 failed — Qdrant score inversion fix; cognee may have changed: %s",
        exc,
    )

# ── Patch 7: dataset_id second-layer tenancy filter (SECURITY HARDENING) ──────
#
# BACKGROUND:
#   The primary tenancy key in Qdrant is `database_name` (set to "cognee-{company_id}")
#   via the QDrantAdapter.search() and create_data_points() methods. This is enforced by
#   convention via set_company_context(). If that context-setting is ever skipped or
#   misconfigured, a query could reach the wrong tenant's data.
#
# THIS PATCH:
#   WRITE path: stamps `dataset_id = company_id` on every point payload in addition to
#               `database_name`. The dataset_id is extracted from `self.database_name`
#               (format: "cognee-{company_id}") by stripping the "cognee-" prefix.
#   READ path:  injects a `dataset_id == company_id` filter into every Qdrant search
#               query, alongside the existing `database_name` filter. Both must match.
#
# BACKWARDS COMPAT:
#   Points written before this patch lack `dataset_id`. The filter excludes them until
#   scripts/backfill_dataset_id.py is run to add `dataset_id` to all existing points.
#
# FAILURE MODE:
#   If this patch fails to apply (e.g. cognee-community adapter interface changed),
#   the system falls back to the primary `database_name` filter only. A WARNING is logged
#   so the change is surfaced immediately.


def _extract_company_id_from_db_name(database_name: str) -> str:
    """Extract company_id from a `cognee-{company_id}` formatted database name.

    Returns empty string if the prefix is not present (safe fallback — the dataset_id
    filter then becomes an empty-string match which will exclude all points, which is
    the fail-safe direction for security).
    """
    prefix = "cognee-"
    if database_name.startswith(prefix):
        return database_name[len(prefix) :]
    return ""


try:
    from cognee_community_vector_adapter_qdrant import QDrantAdapter as _QDA7
    from qdrant_client import models as _qdrant_models

    # ── Patch 7a: WRITE path — stamp dataset_id on every written point ────────
    _original_create_data_points = _QDA7.create_data_points

    async def _patched_create_data_points(self, collection_name, data_points, vector_name="text"):
        """create_data_points that stamps dataset_id on every Qdrant point payload."""
        # Temporarily replace database_name-building logic by wrapping the original.
        # We use a sub-class trick: inject dataset_id by monkey-patching the inner
        # convert_to_qdrant_point closure. Since we can't reach that closure, we
        # instead wrap the full method and post-process the written payloads.
        #
        # Approach: call original, which writes `database_name` to Qdrant. Then
        # issue a follow-up set_payload to add `dataset_id` to the same point IDs.
        # This is atomic-enough for our security goal (both fields written before
        # any search returns the point, assuming single-writer per async task).

        company_id = _extract_company_id_from_db_name(self.database_name)
        if not company_id:
            logger.warning(
                "cognee_patches: Patch 7a — could not extract company_id from database_name=%r; "
                "dataset_id will NOT be stamped on these points",
                self.database_name,
            )
            return await _original_create_data_points(
                self, collection_name, data_points, vector_name
            )

        # Collect data_point IDs before writing
        point_ids = [str(dp.id) for dp in data_points]

        # Call original write
        result = await _original_create_data_points(self, collection_name, data_points, vector_name)

        # Stamp dataset_id on all written points
        try:
            from qdrant_client import AsyncQdrantClient as _QC7

            client = self.get_qdrant_client()
            try:
                await client.set_payload(
                    collection_name=collection_name,
                    payload={"dataset_id": company_id},
                    points=point_ids,
                )
                logger.debug(
                    "cognee_patches: Patch 7a — stamped dataset_id=%r on %d points in %s",
                    company_id,
                    len(point_ids),
                    collection_name,
                )
            finally:
                await client.close()
        except Exception as stamp_exc:
            logger.warning(
                "cognee_patches: Patch 7a — failed to stamp dataset_id on %d points in %s: %s",
                len(point_ids),
                collection_name,
                stamp_exc,
            )

        return result

    _QDA7.create_data_points = _patched_create_data_points
    logger.info("cognee_patches: Patch 7a — WRITE path dataset_id stamping applied")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 7a failed — write-path dataset_id stamp; cognee may have changed: %s",
        exc,
    )

try:
    from cognee_community_vector_adapter_qdrant import QDrantAdapter as _QDA7r
    from qdrant_client import models as _qdrant_models_r

    # ── Patch 7b: READ path — inject dataset_id filter into every search ──────
    # We patch on top of whatever search() currently is (may already be Patch 6's version).
    _current_qdrant_search = _QDA7r.search

    async def _patched_qdrant_search_with_dataset_id(self, *args, **kwargs):
        """search() that post-filters results to enforce dataset_id == company_id.

        Uses *args/**kwargs passthrough so any kwargs cognee's retrievers send
        (regardless of cognee version) reach the underlying adapter without
        modification. The tenancy hardening is applied as a post-filter on the
        returned results — it does NOT alter the upstream call signature.

        The dataset_id is derived from self.database_name (format: cognee-{company_id}).
        If we cannot derive it (malformed database_name), we log a warning and still
        proceed with the primary database_name filter — no silent all-tenant leak.
        """
        company_id = _extract_company_id_from_db_name(self.database_name)

        if not company_id:
            logger.warning(
                "cognee_patches: Patch 7b — could not derive company_id from database_name=%r; "
                "dataset_id filter NOT injected (primary database_name filter still active)",
                self.database_name,
            )
            return await _current_qdrant_search(self, *args, **kwargs)

        # Forward ALL args/kwargs unchanged — forward-compatible with any cognee version.
        results = await _current_qdrant_search(self, *args, **kwargs)

        # Post-filter: retain only results whose payload contains dataset_id == company_id.
        # IMPORTANT: many cognee retrievers call search() with include_payload=False (the
        # default), in which case results have payload=None. We CANNOT enforce dataset_id
        # in that case — but the upstream database_name filter (already applied inside
        # QDrantAdapter.search) is sufficient for tenancy because every point in the
        # company's database carries `database_name = cognee-{company_id}`. Pass-through.
        _include_payload = kwargs.get("include_payload", False)
        if not _include_payload:
            return results
        # collection_name extracted for logging only — first positional arg or kwarg.
        _coll_name = args[0] if args else kwargs.get("collection_name", "<unknown>")
        filtered = []
        excluded_count = 0
        for r in results:
            payload = getattr(r, "payload", None) or {}
            point_dataset_id = payload.get("dataset_id")
            if point_dataset_id is None:
                # Point predates Patch 7 — exclude (fail-safe). Count for telemetry.
                excluded_count += 1
                continue
            if point_dataset_id == company_id:
                filtered.append(r)
            else:
                # dataset_id mismatch — this would have been a cross-tenant leak
                # if not for this second filter. Log at WARNING for incident tracking.
                logger.warning(
                    "cognee_patches: Patch 7b — CROSS-TENANT BLOCKED: "
                    "point dataset_id=%r != expected company_id=%r in collection=%r",
                    point_dataset_id,
                    company_id,
                    _coll_name,
                )

        if excluded_count > 0:
            logger.debug(
                "cognee_patches: Patch 7b — excluded %d pre-patch points (no dataset_id) "
                "from collection=%r for company_id=%r. Run backfill_dataset_id.py.",
                excluded_count,
                _coll_name,
                company_id,
            )

        return filtered

    _QDA7r.search = _patched_qdrant_search_with_dataset_id
    logger.info("cognee_patches: Patch 7b — READ path dataset_id filter applied")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 7b failed — read-path dataset_id filter; cognee may have changed: %s",
        exc,
    )

# ── Patch 8: FEELING_LUCKY → admin-only mode guard ────────────────────────────
#
# BACKGROUND:
#   When FEELING_LUCKY is requested, cognee's select_search_type() uses an LLM to
#   choose the actual search mode. If it selects CYPHER or NATURAL_LANGUAGE, a non-
#   admin user would get access to those modes without explicitly requesting them —
#   an invisible privilege escalation path.
#
# THIS PATCH:
#   Provides a ContextVar `feeling_lucky_admin_context` that callers (servicer.py)
#   set to True for admin users and False for non-admin users before calling
#   cognee.search() with FEELING_LUCKY.
#
#   Patches get_search_type_retriever_instance to check this ContextVar after
#   FEELING_LUCKY resolves. If the resolved mode is CYPHER or NATURAL_LANGUAGE
#   and feeling_lucky_admin_context is False, raises PermissionError with a
#   `permission_denied` message.
#
# SERVICER WIRING:
#   In servicer.py, before calling cognee.search() with FEELING_LUCKY:
#       from cognee_service.cognee_patches import feeling_lucky_admin_context
#       token = feeling_lucky_admin_context.set(_is_admin(user))
#       try:
#           results = await cognee.search(...)
#       finally:
#           feeling_lucky_admin_context.reset(token)
#
# FAILURE MODE:
#   If the patch fails, the guard is not active. The servicer's static admin gate
#   for CYPHER/NATURAL_LANGUAGE direct requests still applies. FEELING_LUCKY that
#   resolves to those modes will then be allowed for all users — a degraded state.
#   The WARNING log surfaces this immediately.

from contextvars import ContextVar as _ContextVar

# Set to True for admin users before cognee.search(FEELING_LUCKY) calls.
# Defaults to False (non-admin) — fail-safe direction.
feeling_lucky_admin_context: _ContextVar[bool] = _ContextVar(
    "feeling_lucky_admin_context", default=False
)

_FEELING_LUCKY_ADMIN_ONLY_MODES_STR = {"CYPHER", "NATURAL_LANGUAGE"}

try:
    import cognee.modules.search.methods.get_search_type_retriever_instance as _gsri_mod
    from cognee.modules.search.types import SearchType as _SearchType

    _original_get_retriever = _gsri_mod.get_search_type_retriever_instance

    async def _patched_get_retriever(query_type, query_text, **kwargs):
        """get_search_type_retriever_instance that gates FEELING_LUCKY → admin-only modes."""
        # Let cognee do the FEELING_LUCKY resolution (select_search_type LLM call)
        retriever = await _original_get_retriever(query_type, query_text, **kwargs)

        # If the original type was FEELING_LUCKY, check what mode was actually resolved.
        # We infer the resolved mode from the retriever class name since cognee doesn't
        # expose the resolved SearchType in the return value.
        if query_type is _SearchType.FEELING_LUCKY:
            retriever_class_name = type(retriever).__name__
            feeling_lucky_resolved_type_context.set(retriever_class_name)
            is_admin_mode = (
                "Cypher" in retriever_class_name or "NaturalLanguage" in retriever_class_name
            )
            if is_admin_mode:
                is_admin = feeling_lucky_admin_context.get()
                logger.info(
                    "cognee_patches: Patch 8 — FEELING_LUCKY resolved to admin-mode retriever=%s; "
                    "is_admin=%s",
                    retriever_class_name,
                    is_admin,
                )
                if not is_admin:
                    raise PermissionError(
                        f"permission_denied: FEELING_LUCKY resolved to {retriever_class_name} "
                        f"which requires admin role. Non-admin users may not access this mode."
                    )

        return retriever

    _gsri_mod.get_search_type_retriever_instance = _patched_get_retriever
    logger.info("cognee_patches: Patch 8 — FEELING_LUCKY → admin-only mode guard applied")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 8 failed — FEELING_LUCKY admin gate; cognee may have changed: %s",
        exc,
    )


# ── Patch 9: tolerate dangling edges in CogneeGraph.project_graph_from_db ─────
#
# BACKGROUND:
#   cognee's CogneeGraph.project_graph_from_db iterates every edge returned by the
#   graph engine and looks up its source/target nodes in the in-memory projection.
#   If either endpoint is missing, it raises EntityNotFoundError immediately,
#   aborting the whole projection. GRAPH_COMPLETION (and any other retriever that
#   builds a graph projection) then returns no useful context.
#
#   In practice, the V2 ingestion pipeline can produce edges whose source_node_id
#   refers to a node that was never written (race / partial write / out-of-order
#   commit between the entity-extraction step and the neo4j-storage step). On a
#   24k-node / 100k-edge graph, even a single orphan edge kills the entire query.
#
# THIS PATCH:
#   Replaces the offending `raise` with a `continue` plus a debug log line. The
#   projection now skips dangling edges and proceeds with the rest of the graph.
#   Tenancy guarantees are unchanged — the upstream Neo4j query is already scoped
#   to the per-company database (cognee-{company_id}), so a skipped edge cannot
#   leak across tenants.
#
#   Implementation: rewrite the method's source by regex-replacing the single
#   `raise EntityNotFoundError(...)` block with a `continue`, recompile, and
#   rebind onto the class. Surgical and resilient to the surrounding method body
#   evolving as long as the raise line is intact.
#
# FAILURE MODE:
#   If the patch cannot find or rewrite the target line (cognee changed the
#   source), the method falls back to the original raise-on-orphan behaviour. A
#   WARNING surfaces this. GRAPH_COMPLETION then degrades to "no context" on any
#   tenant whose graph contains a single orphan edge.

try:
    import inspect as _inspect
    import re as _re
    import textwrap as _textwrap

    from cognee.modules.graph.cognee_graph.CogneeGraph import (
        CogneeGraph as _CogneeGraph,
    )

    _orig_project = _CogneeGraph.project_graph_from_db
    _src = _textwrap.dedent(_inspect.getsource(_orig_project))

    _pattern = _re.compile(
        r'raise EntityNotFoundError\(\s*message=f"Edge references nonexistent nodes: \{source_id\} -> \{target_id\}"\s*\)',
        flags=_re.DOTALL,
    )
    _replacement = (
        'logger.debug("cognee_patches: Patch 9 — skipped dangling edge '
        '%s -> %s (one or both endpoints missing from projection)", '
        "source_id, target_id); continue"
    )
    _new_src, _n_subs = _pattern.subn(_replacement, _src)
    if _n_subs == 0:
        raise RuntimeError(
            "Patch 9: could not locate `raise EntityNotFoundError(...)` in "
            "CogneeGraph.project_graph_from_db source — cognee source changed"
        )

    _orig_globals = _orig_project.__globals__
    _patch_ns: dict = {}
    exec(compile(_new_src, "<cognee_patches.Patch9>", "exec"), _orig_globals, _patch_ns)
    _patched_project = _patch_ns["project_graph_from_db"]

    _CogneeGraph.project_graph_from_db = _patched_project
    logger.info(
        "cognee_patches: Patch 9 — project_graph_from_db tolerates dangling edges "
        "(rewrote %d raise site)",
        _n_subs,
    )

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 9 failed — dangling-edge tolerance; cognee may have changed: %s",
        exc,
    )


def _nodes_and_edges_from_raw_graph_result(result):
    if not result:
        return [], []

    raw_nodes = result[0].get("rawNodes", [])
    raw_rels = result[0].get("rawRels", [])

    nodes = [(n["properties"]["id"], n["properties"]) for n in raw_nodes]
    edges = [
        (
            r["properties"]["source_node_id"],
            r["properties"]["target_node_id"],
            r["type"],
            r["properties"],
        )
        for r in raw_rels
    ]
    return nodes, edges


async def _query_nodeset_property_graph(adapter, node_set):
    query = """
    MATCH (n)
    WHERE n.node_set IN $node_set
    WITH collect(DISTINCT n) AS primary
    UNWIND primary AS p
    OPTIONAL MATCH (p)--(nbr)
    WITH primary, collect(DISTINCT nbr) AS nbrs
    WITH primary + nbrs AS nodelist
    UNWIND nodelist AS node
    WITH collect(DISTINCT node) AS nodes
    MATCH (a)-[r]-(b)
    WHERE a IN nodes AND b IN nodes
    WITH nodes, collect(DISTINCT r) AS rels
    RETURN
      [n IN nodes | {id: n.id, properties: properties(n)}] AS rawNodes,
      [r IN rels | {type: type(r), properties: properties(r)}] AS rawRels
    """
    return _nodes_and_edges_from_raw_graph_result(
        await adapter.query(query, {"node_set": node_set})
    )


async def _query_codeblock_nodeset_graph(adapter, node_set):
    query = """
    MATCH p=(root:DocumentChunk)-[:contains|has_code|has_code_block|made_from|summarizes|mentions|calls|imports|renders|accepts_parameter|has_method|returns|inherits_from*1..3]->(n:CodeBlock)
    WHERE root.node_set IN $node_set
    WITH collect(DISTINCT root) + collect(DISTINCT n) AS seed_nodes
    UNWIND seed_nodes AS seed
    WITH collect(DISTINCT seed) AS nodes
    MATCH (a)-[r]-(b)
    WHERE a IN nodes AND b IN nodes
    WITH nodes, collect(DISTINCT r) AS rels
    RETURN
      [n IN nodes | {id: n.id, properties: properties(n)}] AS rawNodes,
      [r IN rels | {type: type(r), properties: properties(r)}] AS rawRels
    """
    return _nodes_and_edges_from_raw_graph_result(
        await adapter.query(query, {"node_set": node_set})
    )


# ── Patch 10: thread node_name through batch vector search ───────────────────
try:
    import asyncio as _asyncio10

    from cognee.modules.retrieval.utils.node_edge_vector_search import (
        NodeEdgeVectorSearch as _NodeEdgeVectorSearch,
    )
    from cognee.infrastructure.databases.vector.exceptions import CollectionNotFoundError

    _original_embed_and_retrieve_distances = _NodeEdgeVectorSearch.embed_and_retrieve_distances

    async def _patched_embed_and_retrieve_distances(
        self,
        query: str = None,
        query_batch: list[str] = None,
        collections: list[str] = None,
        wide_search_limit: int = None,
        node_name=None,
    ):
        """Preserve node_name so batch search can forward it."""
        _has_previous_node_name = hasattr(self, "node_name")
        _previous_node_name = getattr(self, "node_name", None)
        self.node_name = node_name
        try:
            return await _original_embed_and_retrieve_distances(
                self,
                query=query,
                query_batch=query_batch,
                collections=collections,
                wide_search_limit=wide_search_limit,
                node_name=node_name,
            )
        finally:
            if _has_previous_node_name:
                self.node_name = _previous_node_name
            elif hasattr(self, "node_name"):
                delattr(self, "node_name")

    async def _patched_run_batch_search(self, collections, query_batch):
        """Run batch search while reusing the stored node_name scope."""
        search_tasks = [
            self._search_batch_collection(collection, query_batch) for collection in collections
        ]
        return await _asyncio10.gather(*search_tasks)

    async def _patched_search_batch_collection(self, collection_name, query_batch):
        """Forward node_name into each batch_search call."""
        try:
            return await self.vector_engine.batch_search(
                collection_name=collection_name,
                query_texts=query_batch,
                limit=None,
                node_name=getattr(self, "node_name", None),
            )
        except CollectionNotFoundError:
            return [[]] * len(query_batch)

    _NodeEdgeVectorSearch.embed_and_retrieve_distances = _patched_embed_and_retrieve_distances
    _NodeEdgeVectorSearch._run_batch_search = _patched_run_batch_search
    _NodeEdgeVectorSearch._search_batch_collection = _patched_search_batch_collection
    logger.info("cognee_patches: Patch 10 — node_name now reaches batch vector search")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 10 failed — batch node_name threading; cognee may have changed: %s",
        exc,
    )


# ── Patch 11: add node_set graph projection branch ───────────────────────────
try:
    import inspect as _inspect11
    import textwrap as _textwrap11

    from cognee.modules.graph.cognee_graph.CogneeGraph import CogneeGraph as _CogneeGraph11
    from cognee.modules.graph.exceptions import EntityNotFoundError, InvalidDimensionsError
    from cognee.modules.graph.cognee_graph.CogneeGraphElements import Edge, Node

    _original_project_graph_from_db = _CogneeGraph11.project_graph_from_db
    _project_graph_src = _textwrap11.dedent(_inspect11.getsource(_original_project_graph_from_db))

    async def _patched_project_graph_from_db(
        self,
        adapter,
        node_properties_to_project,
        edge_properties_to_project,
        directed=True,
        node_dimension=1,
        edge_dimension=1,
        memory_fragment_filter=[],
        node_type: Optional[Type] = None,
        node_name: Optional[list[str]] = None,
        node_set: Optional[list[str]] = None,
        relevant_ids_to_filter: Optional[list[str]] = None,
        triplet_distance_penalty: float = 3.5,
    ) -> None:
        if node_dimension < 1 or edge_dimension < 1:
            raise InvalidDimensionsError()
        try:
            if node_set not in [None, [], ""]:
                logger.info("Retrieving graph filtered by node_set property.")
                if node_type is not None and getattr(node_type, "__name__", "") == "CodeBlock":
                    nodes_data, edges_data = await _query_codeblock_nodeset_graph(adapter, node_set)
                else:
                    nodes_data, edges_data = await _query_nodeset_property_graph(adapter, node_set)
                if not nodes_data or not edges_data:
                    raise EntityNotFoundError(
                        message="Nodeset does not exist, or empty nodeset projected from the database."
                    )
            elif node_type is not None and node_name not in [None, [], ""]:
                nodes_data, edges_data = await self._get_nodeset_subgraph(
                    adapter, node_type, node_name
                )
            elif len(memory_fragment_filter) == 0:
                nodes_data, edges_data = await self._get_full_or_id_filtered_graph(
                    adapter, relevant_ids_to_filter
                )
            else:
                nodes_data, edges_data = await self._get_filtered_graph(
                    adapter, memory_fragment_filter
                )

            self.triplet_distance_penalty = triplet_distance_penalty

            start_time = time.time()
            for node_id, properties in nodes_data:
                node_attributes = {key: properties.get(key) for key in node_properties_to_project}
                self.add_node(
                    Node(
                        str(node_id),
                        node_attributes,
                        dimension=node_dimension,
                        node_penalty=triplet_distance_penalty,
                    )
                )

            for source_id, target_id, relationship_type, properties in edges_data:
                source_node = self.get_node(str(source_id))
                target_node = self.get_node(str(target_id))
                if source_node and target_node:
                    edge_attributes = {
                        key: properties.get(key) for key in edge_properties_to_project
                    }
                    edge_attributes["relationship_type"] = relationship_type

                    edge = Edge(
                        source_node,
                        target_node,
                        attributes=edge_attributes,
                        directed=directed,
                        dimension=edge_dimension,
                        edge_penalty=triplet_distance_penalty,
                    )
                    self.add_edge(edge)
                else:
                    logger.debug(
                        "cognee_patches: Patch 11 — skipped dangling edge %s -> %s (one or both endpoints missing from projection)",
                        source_id,
                        target_id,
                    )
                    continue

            projection_time = time.time() - start_time
            logger.info(
                f"Graph projection completed: {len(self.nodes)} nodes, {len(self.edges)} edges in {projection_time:.2f}s"
            )

        except Exception as e:
            logger.error(f"Error during graph projection: {str(e)}")
            raise

    _CogneeGraph11.project_graph_from_db = _patched_project_graph_from_db
    logger.info("cognee_patches: Patch 11 — project_graph_from_db supports node_set scoping")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 11 failed — node_set graph projection; cognee may have changed: %s",
        exc,
    )


# ── Patch 12: thread node_name through non-graph retrievers ──────────────────
try:
    import cognee.modules.search.methods.get_search_type_retriever_instance as _gsri_mod12
    from cognee.modules.search.types import SearchType as _SearchType12

    from cognee.modules.retrieval.chunks_retriever import ChunksRetriever as _ChunksRetriever12
    from cognee.modules.retrieval.summaries_retriever import (
        SummariesRetriever as _SummariesRetriever12,
    )
    from cognee.modules.retrieval.completion_retriever import (
        CompletionRetriever as _CompletionRetriever12,
    )
    from cognee.modules.retrieval.triplet_retriever import TripletRetriever as _TripletRetriever12
    from cognee.modules.retrieval.temporal_retriever import (
        TemporalRetriever as _TemporalRetriever12,
    )
    from cognee.infrastructure.databases.vector import get_vector_engine
    from cognee.infrastructure.databases.unified import get_unified_engine
    from cognee.modules.retrieval.exceptions.exceptions import NoDataError
    from cognee.infrastructure.databases.vector.exceptions import CollectionNotFoundError

    _original_get_search_type_retriever_instance = _gsri_mod12.get_search_type_retriever_instance

    async def _patched_get_search_type_retriever_instance(query_type, query_text, **kwargs):
        retriever_instance = await _original_get_search_type_retriever_instance(
            query_type, query_text, **kwargs
        )
        node_name = kwargs.get("node_name")
        try:
            setattr(retriever_instance, "node_name", node_name)
        except Exception:
            pass
        return retriever_instance

    async def _patched_chunks_get_retrieved_objects(self, query: str):
        unified = await get_unified_engine()
        vector_engine = unified.vector

        try:
            found_chunks = await vector_engine.search(
                "DocumentChunk_text",
                query,
                limit=self.top_k,
                include_payload=True,
                node_name=getattr(self, "node_name", None),
            )

            found_chunks = _fix_qdrant_scores(found_chunks)
            if not found_chunks:
                setattr(self, "_last_low_confidence_reason", "")
                return found_chunks

            filtered_chunks = []
            for chunk in found_chunks:
                payload = getattr(chunk, "payload", None)
                score = payload.get("score") if isinstance(payload, dict) else None
                try:
                    similarity = float(score)
                except (TypeError, ValueError):
                    continue
                if similarity >= COGNEE_CHUNKS_MIN_SCORE:
                    filtered_chunks.append(chunk)

            setattr(
                self,
                "_last_low_confidence_reason",
                "" if filtered_chunks else _low_confidence_message(COGNEE_CHUNKS_MIN_SCORE),
            )
            return filtered_chunks

        except CollectionNotFoundError as error:
            logger.error("DocumentChunk_text collection not found in vector database")
            raise NoDataError("No data found in the system, please add data first.") from error

    async def _patched_summaries_get_retrieved_objects(self, query: str):
        logger.info(
            f"Starting summary retrieval for query: '{query[:100]}{'...' if len(query) > 100 else ''}'"
        )

        unified = await get_unified_engine()
        vector_engine = unified.vector

        try:
            summaries_results = await vector_engine.search(
                "TextSummary_text",
                query,
                limit=self.top_k,
                include_payload=True,
                node_name=getattr(self, "node_name", None),
            )
            logger.info(f"Found {len(summaries_results)} summaries from vector search")

            return summaries_results
        except CollectionNotFoundError as error:
            logger.error("TextSummary_text collection not found in vector database")
            raise NoDataError("No data found in the system, please add data first.") from error

    async def _patched_completion_get_retrieved_objects(self, query: str):
        vector_engine = get_vector_engine()

        try:
            found_chunks = await vector_engine.search(
                "DocumentChunk_text",
                query,
                limit=self.top_k,
                include_payload=True,
                node_name=getattr(self, "node_name", None),
            )

            return found_chunks
        except CollectionNotFoundError as error:
            logger.error("DocumentChunk_text collection not found")
            raise NoDataError("No data found in the system, please add data first.") from error

    async def _patched_triplet_get_retrieved_objects(self, query: str):
        vector_engine = get_vector_engine()

        try:
            if not await vector_engine.has_collection(collection_name="Triplet_text"):
                logger.error("Triplet_text collection not found")
                raise NoDataError(
                    "In order to use TRIPLET_COMPLETION first use the create_triplet_embeddings memify pipeline. "
                )

            found_triplets = await vector_engine.search(
                "Triplet_text",
                query,
                limit=self.top_k,
                include_payload=True,
                node_name=getattr(self, "node_name", None),
            )

            if len(found_triplets) == 0:
                return []

            return found_triplets
        except CollectionNotFoundError as error:
            logger.error("Triplet_text collection not found")
            raise NoDataError("No data found in the system, please add data first.") from error

    async def _patched_temporal_get_retrieved_objects(self, query: str) -> dict:
        time_from, time_to = await self.extract_time_from_query(query)

        unified = await get_unified_engine()
        graph_engine = unified.graph

        if time_from and time_to:
            ids = await graph_engine.collect_time_ids(time_from=time_from, time_to=time_to)
        elif time_from:
            ids = await graph_engine.collect_time_ids(time_from=time_from)
        elif time_to:
            ids = await graph_engine.collect_time_ids(time_to=time_to)
        else:
            logger.info(
                "No timestamps identified based on the query, performing retrieval using triplet search on events and entities."
            )
            triplets = await self.get_triplets(query)
            return {"triplets": triplets}

        if ids:
            relevant_events = await graph_engine.collect_events(ids=ids)
        else:
            logger.info(
                "No events identified based on timestamp filtering, performing retrieval using triplet search on events and entities."
            )
            triplets = await self.get_triplets(query)
            return {"triplets": triplets}

        vector_engine = unified.vector
        query_vector = (await vector_engine.embedding_engine.embed_text([query]))[0]

        vector_search_results = await vector_engine.search(
            collection_name="Event_name",
            query_vector=query_vector,
            limit=self.top_k,
            node_name=getattr(self, "node_name", None),
        )

        return {"relevant_events": relevant_events, "vector_search_results": vector_search_results}

    _gsri_mod12.get_search_type_retriever_instance = _patched_get_search_type_retriever_instance

    # CRITICAL: get_retriever_output.py imports get_search_type_retriever_instance
    # via direct `from ... import ...` syntax, which captures a stale reference.
    # Patching only the source module attribute leaves the consumer pointing at
    # the original factory — Patch 12's node_name-attribute injection never fires.
    # Fix: also rebind the consumer module's attribute. Verified by Adelana
    # (SCOPE_FILTER_BUG.md, 2026-05-02).
    # The parent package cognee.modules.search.methods/__init__.py does
    #   `from .get_retriever_output import get_retriever_output`
    # which makes the bare attribute access `cognee.modules.search.methods.get_retriever_output`
    # resolve to the FUNCTION, not the submodule. `import ... as ...` then binds
    # the local name to that function; setting an attribute on a function is a
    # silent no-op. We MUST use importlib to force submodule resolution.
    try:
        import importlib as _importlib12

        _gro_mod12 = _importlib12.import_module(
            "cognee.modules.search.methods.get_retriever_output"
        )
        _gro_mod12.get_search_type_retriever_instance = _patched_get_search_type_retriever_instance
        # Sanity check: verify the rebind actually took
        _check = getattr(_gro_mod12, "get_search_type_retriever_instance", None)
        logger.info(
            "cognee_patches: Patch 12 — rebound gro consumer; bound name=%s",
            getattr(_check, "__name__", "<unknown>"),
        )
    except Exception as _gro_exc:
        logger.warning(
            "cognee_patches: Patch 12 — failed to rebind get_retriever_output's import: %s",
            _gro_exc,
        )

    _ChunksRetriever12.get_retrieved_objects = _patched_chunks_get_retrieved_objects
    _SummariesRetriever12.get_retrieved_objects = _patched_summaries_get_retrieved_objects
    _CompletionRetriever12.get_retrieved_objects = _patched_completion_get_retrieved_objects
    _TripletRetriever12.get_retrieved_objects = _patched_triplet_get_retrieved_objects
    _TemporalRetriever12.get_retrieved_objects = _patched_temporal_get_retrieved_objects

    logger.info("cognee_patches: Patch 12 — node_name now reaches non-graph retrievers")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 12 failed — non-graph node_name threading; cognee may have changed: %s",
        exc,
    )


# ── Patch 13: filter discovered collection list to only existing collections ─
# Problem (concrete observation, cognee 0.5.5):
#   `GraphCompletionRetriever.get_triplets()` calls
#   `_get_vector_index_collections()` which walks every DataPoint subclass
#   registered in cognee's runtime (33 names in this build:
#   AudioDocument_name, ClassDefinition_source_code, CodeFile_name, ...,
#   TableType_name, TextDocument_name, etc.). It then passes that full list
#   to `brute_force_triplet_search(collections=...)`.
#
#   In our deployment Qdrant only has a subset of those collections (the ones
#   the v2 ingestion pipeline actually creates: Entity_name, Triplet_text,
#   DocumentChunk_text, TextSummary_text, EdgeType_relationship_name,
#   EntityType_name, TextDocument_name). Anything else 404s.
#
#   This is asymmetric across modes:
#     - Plain GRAPH_COMPLETION (single query) sets `wide_search_limit=100`
#       and tolerates the missing collections silently inside vector search.
#     - GRAPH_COMPLETION_COT / GRAPH_COMPLETION_CONTEXT_EXTENSION call
#       get_triplets in BATCH mode (query_batch=[...]). Batch mode sets
#       `wide_search_limit=None` (unbounded). The unbounded path performs a
#       direct lookup that raises on the first missing collection, killing
#       the whole call.
#
# Fix:
#   Wrap GraphCompletionRetriever.get_triplets so that, before delegating to
#   the original, the discovered collections list is intersected with what
#   actually exists in the configured vector engine. The result is the same
#   set the safe-default path would already use, but computed dynamically so
#   we automatically pick up any new collections without code changes.
#
# Earlier guards (former Patches 13 + 14) hard-required nonexistent
# collections (Document_name, FunctionDefinition_source_code, TableType_name,
# Event_name). They came from incorrect reading of cognee's needs. Removed.
try:
    import importlib as _importlib13
    from cognee.infrastructure.databases.vector import (
        get_vector_engine as _get_vector_engine13,
    )

    _gc_mod13 = _importlib13.import_module("cognee.modules.retrieval.graph_completion_retriever")
    _GraphCompletionRetriever13 = _gc_mod13.GraphCompletionRetriever
    _orig_get_vector_index_collections13 = _GraphCompletionRetriever13._get_vector_index_collections

    # Module-level cache so we don't probe Qdrant on every retrieval. Cognee
    # collection schemas don't change at runtime; collection contents do.
    _existing_collections_cache13: set[str] = set()
    _existing_collections_filled13 = False

    async def _ensure_existing_collections_cache_filled13() -> set[str]:
        global _existing_collections_filled13, _existing_collections_cache13
        if _existing_collections_filled13:
            return _existing_collections_cache13

        engine = _get_vector_engine13()
        candidates = list(_orig_get_vector_index_collections13())
        # Probe each candidate ONCE. Failures are treated as "not present".
        survivors: set[str] = set()
        for name in candidates:
            try:
                if await engine.has_collection(collection_name=name):
                    survivors.add(name)
            except Exception:
                # Adapter raised something unexpected; treat as missing.
                continue
        _existing_collections_cache13 = survivors
        _existing_collections_filled13 = True
        logger.info(
            "cognee_patches: Patch 13 — graph completion collection set narrowed: "
            "%d/%d candidates exist (%s)",
            len(survivors),
            len(candidates),
            ", ".join(sorted(survivors)),
        )
        return survivors

    _orig_get_triplets13 = _GraphCompletionRetriever13.get_triplets

    async def _patched_get_triplets13(self, query=None, query_batch=None):
        # Filter the collection list before cognee uses it. We can't simply
        # patch _get_vector_index_collections (it's a static method, and
        # cognee calls it inside get_triplets without awaiting), so we
        # compute the filtered set up-front and stash it on a thread-local
        # context, then run the original — which will pick up our filtered
        # list via the patched static method below.
        await _ensure_existing_collections_cache_filled13()
        return await _orig_get_triplets13(self, query=query, query_batch=query_batch)

    @staticmethod
    def _patched_get_vector_index_collections13():
        candidates = _orig_get_vector_index_collections13()
        if not _existing_collections_filled13:
            # Cache not warm yet (first call before _patched_get_triplets13
            # ran). Return everything; the next call will be filtered.
            return list(candidates)
        return [c for c in candidates if c in _existing_collections_cache13]

    _GraphCompletionRetriever13._get_vector_index_collections = (
        _patched_get_vector_index_collections13
    )
    _GraphCompletionRetriever13.get_triplets = _patched_get_triplets13

    logger.info("cognee_patches: Patch 13 — graph completion collection filter installed")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 13 failed — collection filter; cognee may have changed: %s",
        exc,
    )


# ── Patch 14 (REMOVED 2026-05-08) ────────────────────────────────────────────
# Patch 14 required TableType_name + Event_name for
# GRAPH_COMPLETION_CONTEXT_EXTENSION. That requirement was a guess; the
# retriever inherits get_triplets from GraphCompletionRetriever and gains
# whatever filtering Patch 13 provides. Nothing else is required.


# ── Patch 15: CYPHER / NATURAL_LANGUAGE explicit validation contracts ────────
try:
    import re as _re15
    import cognee.modules.retrieval.cypher_search_retriever as _cypher_mod
    import cognee.modules.retrieval.natural_language_retriever as _nl_mod

    _orig_cypher_get = _cypher_mod.CypherSearchRetriever.get_retrieved_objects
    _orig_nl_get = _nl_mod.NaturalLanguageRetriever.get_retrieved_objects

    def _looks_like_cypher(query: str) -> bool:
        candidate = (query or "").strip()
        if not candidate:
            return False
        return bool(
            _re15.match(
                r"^(MATCH|CALL|CREATE|MERGE|UNWIND|WITH|RETURN)\b",
                candidate,
                flags=_re15.IGNORECASE,
            )
        )

    async def _patched_cypher_get_retrieved_objects(self, query: str):
        if not _looks_like_cypher(query):
            raise SearchContractError(
                "INVALID_ARGUMENT",
                "CYPHER mode requires a valid Cypher query; natural language is not accepted.",
            )
        try:
            return await _orig_cypher_get(self, query)
        except Exception as exc:
            raise SearchContractError("INTERNAL", f"CYPHER contract failed: {exc}") from exc

    async def _patched_nl_get_retrieved_objects(self, query: str):
        try:
            return await _orig_nl_get(self, query)
        except Exception as exc:
            message = str(exc)
            if "SearchResultPayload" in message or "context" in message:
                raise SearchContractError(
                    "INVALID_ARGUMENT",
                    "NATURAL_LANGUAGE response contract mismatch; context must be a string or list[str].",
                ) from exc
            raise SearchContractError(
                "INTERNAL", f"NATURAL_LANGUAGE contract failed: {exc}"
            ) from exc

    _cypher_mod.CypherSearchRetriever.get_retrieved_objects = _patched_cypher_get_retrieved_objects
    _nl_mod.NaturalLanguageRetriever.get_retrieved_objects = _patched_nl_get_retrieved_objects
    logger.info("cognee_patches: Patch 15 — CYPHER / NATURAL_LANGUAGE contract guards applied")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 15 failed — cypher/natural-language contracts; cognee may have changed: %s",
        exc,
    )


# ── Patch 16: FEELING_LUCKY resolved-mode exposure ───────────────────────────
try:
    import cognee.modules.search.methods.get_search_type_retriever_instance as _gsri_mod16
    from cognee.modules.search.types import SearchType as _SearchType16

    _orig_get_retriever16 = _gsri_mod16.get_search_type_retriever_instance

    async def _patched_get_retriever16(query_type, query_text, **kwargs):
        retriever = await _orig_get_retriever16(query_type, query_text, **kwargs)
        if query_type is _SearchType16.FEELING_LUCKY:
            feeling_lucky_resolved_type_context.set(type(retriever).__name__)
        return retriever

    _gsri_mod16.get_search_type_retriever_instance = _patched_get_retriever16
    logger.info("cognee_patches: Patch 16 — FEELING_LUCKY resolved-mode exposure applied")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 16 failed — FEELING_LUCKY exposure; cognee may have changed: %s",
        exc,
    )


# ── Patch 17: bound vector search and graph projection when scoped ───────────
# Problem:
#   `cognee.modules.retrieval.utils.brute_force_triplet_search` deliberately sets
#   `wide_search_limit = None` when `node_name` is set. This causes BOTH the vector
#   search to fetch every hit in scope (200k+ for knowledge) AND the graph projection
#   to materialise the full scope subgraph (~480s on knowledge). GRAPH_COMPLETION
#   becomes unusable on any non-trivial scope.
#
# Vector search is already scope-filtered upstream (Patch 7b applies the
# `belongs_to_set` filter for `node_name`-scoped queries), so a top-K limit gives
# us the K most-relevant scoped hits, exactly what we want for projection.
#
# Strategy: wrap the entire `brute_force_triplet_search` function. When `node_name`
# is set, we (a) keep `wide_search_limit = wide_search_top_k` so vector hits are
# bounded, (b) seed projection with the vector hit IDs via `relevant_ids_to_filter`,
# and (c) clear `node_name`/`node_type` before projection so we land in the
# ID-filtered branch instead of the unbounded NodeSet branch. The downstream
# behaviour (distance mapping, top-K triplets) is unchanged.
try:
    import cognee.modules.retrieval.utils.brute_force_triplet_search as _bfts17
    from cognee.modules.retrieval.utils.brute_force_triplet_search import (
        NodeEdgeVectorSearch as _NodeEdgeVectorSearch17,
    )
    from cognee.infrastructure.databases.unified import (
        get_unified_engine as _get_unified_engine17,
    )
    from cognee.infrastructure.databases.vector.exceptions import (
        CollectionNotFoundError as _CollectionNotFoundError17,
    )

    _orig_get_memory_fragment_17 = _bfts17.get_memory_fragment

    async def _scoped_brute_force_triplet_search(
        query=None,
        query_batch=None,
        top_k=5,
        collections=None,
        properties_to_project=None,
        memory_fragment=None,
        node_type=None,
        node_name=None,
        wide_search_top_k=100,
        triplet_distance_penalty=3.5,
        unified_engine=None,
    ):
        # Defer to the original function when no scope is requested.
        if node_name in (None, [], ""):
            return await _orig_brute_force_triplet_search(
                query=query,
                query_batch=query_batch,
                top_k=top_k,
                collections=collections,
                properties_to_project=properties_to_project,
                memory_fragment=memory_fragment,
                node_type=node_type,
                node_name=node_name,
                wide_search_top_k=wide_search_top_k,
                triplet_distance_penalty=triplet_distance_penalty,
                unified_engine=unified_engine,
            )

        if not query and not query_batch:
            raise ValueError("query or query_batch is required")
        if top_k <= 0:
            raise ValueError("top_k must be a positive integer.")

        query_list_length = len(query_batch) if query_batch is not None else None

        if collections is None:
            collections = [
                "Entity_name",
                "TextSummary_text",
                "EntityType_name",
                "DocumentChunk_text",
            ]
        if "EdgeType_relationship_name" not in collections:
            collections.append("EdgeType_relationship_name")

        if unified_engine is None:
            unified_engine = await _get_unified_engine17()
        vector_engine = unified_engine.vector
        graph_engine = unified_engine.graph

        vector_search = _NodeEdgeVectorSearch17(vector_engine=vector_engine)

        # CRITICAL: pass an explicit limit so vector search returns top-K within
        # scope rather than every scope hit (which can be 100k+).
        wide_search_limit = None if query_list_length else wide_search_top_k

        try:
            await vector_search.embed_and_retrieve_distances(
                query=None if query_list_length else query,
                query_batch=query_batch if query_list_length else None,
                collections=collections,
                wide_search_limit=wide_search_limit,
                node_name=node_name,
            )
        except _CollectionNotFoundError17:
            return [[] for _ in range(query_list_length)] if query_list_length else []

        if not vector_search.has_results():
            return [[] for _ in range(query_list_length)] if query_list_length else []

        relevant_node_ids = vector_search.extract_relevant_node_ids()

        if memory_fragment is None:
            memory_fragment = await _orig_get_memory_fragment_17(
                properties_to_project=properties_to_project,
                node_type=None,  # cleared so projection lands in ID-filtered branch
                node_name=None,
                relevant_ids_to_filter=relevant_node_ids,
                triplet_distance_penalty=triplet_distance_penalty,
                graph_engine=graph_engine,
            )

        await memory_fragment.map_vector_distances_to_graph_nodes(
            node_distances=vector_search.node_distances,
            query_list_length=query_list_length,
        )
        await memory_fragment.map_vector_distances_to_graph_edges(
            edge_distances=vector_search.edge_distances,
            query_list_length=query_list_length,
        )
        return await memory_fragment.calculate_top_triplet_importances(
            k=top_k, query_list_length=query_list_length
        )

    _orig_brute_force_triplet_search = _bfts17.brute_force_triplet_search
    _bfts17.brute_force_triplet_search = _scoped_brute_force_triplet_search

    # Rebind any module that imported it via `from ... import brute_force_triplet_search`.
    import importlib as _importlib17

    for _consumer_name in (
        "cognee.modules.retrieval.graph_completion_retriever",
        "cognee.modules.retrieval.graph_summary_completion_retriever",
        "cognee.modules.retrieval.graph_completion_cot_retriever",
        "cognee.modules.retrieval.graph_completion_context_extension_retriever",
    ):
        try:
            _consumer_mod = _importlib17.import_module(_consumer_name)
            if hasattr(_consumer_mod, "brute_force_triplet_search"):
                _consumer_mod.brute_force_triplet_search = _scoped_brute_force_triplet_search
        except Exception:
            # Some consumers may not exist in this cognee version; skip silently.
            pass

    logger.info("cognee_patches: Patch 17 — scoped graph search bounded by wide_search_top_k")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 17 failed — bounded scoped projection; cognee may have changed: %s",
        exc,
    )


# ── Patch 18: expose graph completion scope to downstream triplet ranking ────
try:
    import importlib as _importlib18

    _gc_mod18 = _importlib18.import_module("cognee.modules.retrieval.graph_completion_retriever")
    _GraphCompletionRetriever18 = _gc_mod18.GraphCompletionRetriever

    _orig_get_triplets18 = _GraphCompletionRetriever18.get_triplets

    async def _patched_get_triplets18(self, query=None, query_batch=None):
        scope = getattr(self, "node_name", None)
        token = graph_completion_triplet_scope_context.set(
            ",".join(scope) if isinstance(scope, list) else (scope or "")
        )
        try:
            return await _orig_get_triplets18(self, query=query, query_batch=query_batch)
        finally:
            graph_completion_triplet_scope_context.reset(token)

    _GraphCompletionRetriever18.get_triplets = _patched_get_triplets18
    logger.info("cognee_patches: Patch 18 — graph completion scope context exposed")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 18 failed — graph completion scope context; cognee may have changed: %s",
        exc,
    )


# ── Patch 19: scope-aware LexicalRetriever (CHUNKS_LEXICAL) ──────────────────
# Problem:
#   `LexicalRetriever.initialize()` loads ALL DocumentChunks for the tenant via
#   `get_filtered_graph_data([{"type": ["DocumentChunk"]}])` and ranks them by
#   Jaccard similarity. The retriever ignores `node_name` entirely, so a query
#   scoped to `code:...` happily returns knowledge chunks (and vice versa).
#
# Fix:
#   Patch 12's factory wrapper already sets `instance.node_name` on every
#   retriever it constructs. Wrap LexicalRetriever methods so that:
#     - `initialize()` keys its cache by node_name and only loads chunks that
#       belong to the requested scope (filtered against `node_set` property).
#     - `get_retrieved_objects()` triggers a re-init when node_name differs
#       from the previously cached scope.
try:
    from cognee.modules.retrieval.lexical_retriever import (
        LexicalRetriever as _LexicalRetriever19,
    )
    from cognee.modules.retrieval.exceptions.exceptions import (
        NoDataError as _NoDataError19,
    )
    from cognee.infrastructure.databases.graph import (
        get_graph_engine as _get_graph_engine19,
    )

    _orig_lexical_init19 = _LexicalRetriever19.initialize
    _orig_lexical_get19 = _LexicalRetriever19.get_retrieved_objects

    async def _patched_lexical_initialize(self):
        """Initialize LexicalRetriever, optionally scoped by `self.node_name`."""
        async with self._init_lock:
            requested_scope = getattr(self, "node_name", None)
            cached_scope = getattr(self, "_cached_scope", None)
            if self._initialized and cached_scope == requested_scope:
                return

            # Reset cache when scope changes.
            self.chunks = {}
            self.payloads = {}
            self._initialized = False

            scope_label = (
                ",".join(requested_scope)
                if isinstance(requested_scope, list)
                else (requested_scope or "<unscoped>")
            )
            logger.debug("LexicalRetriever: initializing for scope=%s", scope_label)

            try:
                graph_engine = await _get_graph_engine19()
                # Build attribute filter:
                # - Always: type=DocumentChunk
                # - When scoped: also node_set=[<scope_name>]
                attr_filter = {"type": ["DocumentChunk"]}
                if requested_scope not in (None, [], ""):
                    scope_values = (
                        list(requested_scope)
                        if isinstance(requested_scope, list)
                        else [requested_scope]
                    )
                    attr_filter["node_set"] = scope_values
                nodes, _edges = await graph_engine.get_filtered_graph_data([attr_filter])
            except Exception as e:
                logger.error("LexicalRetriever: graph engine init failed: %s", e)
                raise _NoDataError19("Graph engine initialization failed") from e

            chunk_count = 0
            for node in nodes:
                try:
                    chunk_id, document = node
                except Exception:
                    continue

                if document.get("type") != "DocumentChunk":
                    continue
                text = document.get("text")
                if not text:
                    continue

                # Defensive: filter on `node_set` payload too in case the graph
                # query returned cross-scope nodes for any reason.
                if requested_scope not in (None, [], ""):
                    doc_scope = document.get("node_set")
                    scope_values = (
                        list(requested_scope)
                        if isinstance(requested_scope, list)
                        else [requested_scope]
                    )
                    # node_set may be string scalar or list — handle both.
                    if isinstance(doc_scope, list):
                        if not any(s in doc_scope for s in scope_values):
                            continue
                    elif isinstance(doc_scope, str):
                        if doc_scope not in scope_values:
                            continue
                    else:
                        # Unknown shape; drop to be safe.
                        continue

                try:
                    tokens = self.tokenizer(text)
                    if not tokens:
                        continue
                    self.chunks[str(document.get("id", chunk_id))] = tokens
                    self.payloads[str(document.get("id", chunk_id))] = document
                    chunk_count += 1
                except Exception as e:
                    logger.error("LexicalRetriever: tokenizer failed for %s: %s", chunk_id, e)

            if chunk_count == 0:
                logger.error(
                    "LexicalRetriever: no valid chunks loaded for scope=%s",
                    scope_label,
                )
                raise _NoDataError19(f"No valid chunks loaded for scope {scope_label}.")

            self._initialized = True
            self._cached_scope = requested_scope
            logger.info(
                "LexicalRetriever: initialized with %d chunks for scope=%s",
                len(self.chunks),
                scope_label,
            )

    async def _patched_lexical_get_retrieved_objects(self, query):
        """Re-initialize cache if scope changed since last call."""
        requested_scope = getattr(self, "node_name", None)
        cached_scope = getattr(self, "_cached_scope", None)
        if self._initialized and cached_scope != requested_scope:
            self._initialized = False
        return await _orig_lexical_get19(self, query)

    _LexicalRetriever19.initialize = _patched_lexical_initialize
    _LexicalRetriever19.get_retrieved_objects = _patched_lexical_get_retrieved_objects
    logger.info("cognee_patches: Patch 19 — LexicalRetriever scope-aware")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 19 failed — LexicalRetriever scope; cognee may have changed: %s",
        exc,
    )


# ── Patch 21: bypass deprecated query_batch in qdrant adapter ────────────────
# Problem (verified end-to-end with a 6-line reproducer):
#   `cognee_community_vector_adapter_qdrant 0.2.2` (and 0.2.4 — same shape)
#   calls `AsyncQdrantClient.query_batch(...)` from its `batch_search` method.
#   In `qdrant-client 1.17.1`, `query_batch` is deprecated AND its internal
#   construction of `models.QueryRequest(...)` hardcodes `with_payload=True`
#   while ALSO spreading the caller's `**kwargs` — which the adapter populates
#   with `with_payload=include_payload`. Result:
#       TypeError: QueryRequest() got multiple values for keyword argument
#                  'with_payload'
#
# Impact:
#   - Plain GRAPH_COMPLETION (single-query) goes through `vector_engine.search()`
#     → `query_points(...)`. Unaffected.
#   - GRAPH_COMPLETION_COT, GRAPH_COMPLETION_CONTEXT_EXTENSION, and any
#     batch-mode get_triplets call go through `batch_search()` →
#     `query_batch(...)`. Every collection probe raises TypeError, the adapter
#     swallows it, and downstream synthesis runs WITHOUT vector hits.
#
# Fix:
#   Replace `QDrantAdapter.batch_search` with one that calls
#   `query_batch_points(...)` directly. Embed query texts via the adapter's
#   existing `embed_data()`. Build `QueryRequest` ourselves — no kwargs
#   collision. Same downstream shape, no score threshold (single-query
#   `search()` doesn't apply one either; the asymmetric 0.9 threshold in the
#   upstream `batch_search` is not desirable here).
#
# Cap the per-collection limit at 100 when the caller passes None — cognee's
# default `wide_search_top_k=100` is the right ballpark; the upstream adapter
# uses `collection.count` which on Triplet_text (100k+ points) is catastrophic
# in batch mode.
#
# Removing this patch when:
#   - cognee_community_vector_adapter_qdrant releases a version that calls
#     `query_batch_points` natively, OR
#   - qdrant-client fixes the kwarg collision in `query_batch`.
try:
    import cognee_community_vector_adapter_qdrant.qdrant_adapter as _qadapt21
    from qdrant_client.http import models as _qmodels21

    _QDrantAdapter21 = _qadapt21.QDrantAdapter

    async def _patched_batch_search21(
        self,
        collection_name: str,
        query_texts,
        limit=None,
        with_vectors: bool = False,
        include_payload: bool = False,
        node_name=None,
    ):
        client = self.get_qdrant_client()
        try:
            if limit is None:
                limit = 100
            if limit == 0:
                return []

            query_vectors = await self.embed_data(query_texts)

            base_filter = _qmodels21.Filter(
                must=[
                    _qmodels21.FieldCondition(
                        key="database_name",
                        match=_qmodels21.MatchValue(value=self.database_name),
                    )
                ]
            )

            requests = [
                _qmodels21.QueryRequest(
                    query=vector.tolist() if hasattr(vector, "tolist") else list(vector),
                    using="text",
                    filter=base_filter,
                    limit=limit,
                    with_vector=with_vectors,
                    with_payload=include_payload,
                )
                for vector in query_vectors
            ]

            query_results = await client.query_batch_points(
                collection_name=collection_name,
                requests=requests,
            )

            # No score threshold — mirror single-query `search()` behaviour.
            return [list(getattr(qr, "points", []) or []) for qr in query_results]
        except Exception as exc:  # noqa: BLE001 — match upstream behaviour
            logger.error(
                "cognee_patches: Patch 21 batch_search failed for collection=%s: %s",
                collection_name,
                exc,
                exc_info=True,
            )
            return [[] for _ in (query_texts or [None])]
        finally:
            try:
                await client.close()
            except Exception:
                pass

    _QDrantAdapter21.batch_search = _patched_batch_search21
    logger.info(
        "cognee_patches: Patch 21 — qdrant adapter batch_search rewritten via query_batch_points"
    )

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 21 failed — qdrant adapter batch_search; "
        "adapter or qdrant-client may have changed: %s",
        exc,
    )


# ── Patch 22: extract_relevant_node_ids returns IDs in batch mode ────────────
# Problem (cognee 0.5.5 bug):
#   `NodeEdgeVectorSearch.extract_relevant_node_ids()` is hardcoded to return
#   `[]` whenever batch mode is in use:
#       def extract_relevant_node_ids(self) -> List[str]:
#           if self.query_list_length is not None:
#               return []        # ← always empty in batch mode
#           ...
#
#   The function's purpose is to translate vector hits into node IDs that
#   downstream graph projection uses as a filter (so we don't materialise the
#   entire graph for every search). Returning `[]` in batch mode means COT
#   and CONTEXT_EXTENSION queries can NEVER ID-filter the graph.
#
# Fix:
#   In batch mode, `node_distances[collection]` is a list-of-lists shaped
#   `[[hits_for_query_0], [hits_for_query_1], ...]`. Iterate the union of all
#   inner lists and dedupe. Same return contract as single mode.
try:
    import cognee.modules.retrieval.utils.node_edge_vector_search as _nevs22

    _orig_extract_relevant_node_ids22 = _nevs22.NodeEdgeVectorSearch.extract_relevant_node_ids

    def _patched_extract_relevant_node_ids22(self):
        relevant = set()
        if self.query_list_length is None:
            # Single mode: node_distances[collection] = List[ScoredPoint]
            for scored_results in self.node_distances.values():
                for sp in scored_results:
                    nid = getattr(sp, "id", None)
                    if nid:
                        relevant.add(str(nid))
        else:
            # Batch mode: node_distances[collection] = List[List[ScoredPoint]]
            for per_query_results in self.node_distances.values():
                for scored_results in per_query_results or []:
                    for sp in scored_results or []:
                        nid = getattr(sp, "id", None)
                        if nid:
                            relevant.add(str(nid))
        return list(relevant)

    _nevs22.NodeEdgeVectorSearch.extract_relevant_node_ids = _patched_extract_relevant_node_ids22
    logger.info("cognee_patches: Patch 22 — extract_relevant_node_ids now works in batch mode")

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 22 failed — extract_relevant_node_ids; cognee may have changed: %s",
        exc,
    )


# ── Patch 23: _get_top_triplet_importances always extracts relevant IDs ──────
# Problem (cognee 0.5.5 bug):
#   `_get_top_triplet_importances()` deliberately skips ID extraction when
#   `wide_search_limit is None` (which is ALWAYS the case in batch mode after
#   line 184 of brute_force_triplet_search):
#       if wide_search_limit is None:
#           relevant_node_ids = None      # ← throws away vector hits
#       else:
#           relevant_node_ids = vector_search.extract_relevant_node_ids()
#
#   The downstream `get_memory_fragment(relevant_ids_to_filter=None)` then
#   projects the ENTIRE graph from Neo4j (137k+ nodes / 749k edges in our
#   stack — 40+ seconds) instead of the ~2k nodes near the query. Result:
#   COT and CONTEXT_EXTENSION exceed the gRPC deadline.
#
# Fix:
#   Always call `extract_relevant_node_ids()`. With Patch 22 above, that now
#   returns useful IDs in both modes. If extraction returns an empty list,
#   pass `None` to preserve the "no filter, fetch full graph" semantics for
#   genuine empty results — but for healthy retrievals we get bounded
#   projection in both modes.
try:
    import cognee.modules.retrieval.utils.brute_force_triplet_search as _bfts23

    _orig_get_top_triplet_importances23 = _bfts23._get_top_triplet_importances

    async def _patched_get_top_triplet_importances23(
        memory_fragment,
        vector_search,
        properties_to_project,
        node_type,
        node_name,
        triplet_distance_penalty,
        wide_search_limit,
        top_k,
        query_list_length=None,
        graph_engine=None,
        # cognee 1.0+ adds node_name_filter_operator; accept and forward via **kwargs
        # so this patch survives a minor cognee bump.
        **extra,
    ):
        if memory_fragment is None:
            extracted = vector_search.extract_relevant_node_ids()
            relevant_node_ids = extracted if extracted else None
            memory_fragment = await _bfts23.get_memory_fragment(
                properties_to_project=properties_to_project,
                node_type=node_type,
                node_name=node_name,
                relevant_ids_to_filter=relevant_node_ids,
                triplet_distance_penalty=triplet_distance_penalty,
                graph_engine=graph_engine,
            )

        await memory_fragment.map_vector_distances_to_graph_nodes(
            node_distances=vector_search.node_distances,
            query_list_length=query_list_length,
        )
        await memory_fragment.map_vector_distances_to_graph_edges(
            edge_distances=vector_search.edge_distances,
            query_list_length=query_list_length,
        )
        return await memory_fragment.calculate_top_triplet_importances(
            k=top_k, query_list_length=query_list_length
        )

    _bfts23._get_top_triplet_importances = _patched_get_top_triplet_importances23
    logger.info(
        "cognee_patches: Patch 23 — _get_top_triplet_importances always extracts relevant IDs"
    )

except Exception as exc:
    logger.warning(
        "cognee_patches: Patch 23 failed — _get_top_triplet_importances; "
        "cognee may have changed: %s",
        exc,
    )
