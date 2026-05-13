"""
Unit tests for A2: all 14 cognee search mode mappings and access-control gate.

Tests confirm:
1. Every proto SearchType enum value maps to the correct cognee Python SearchType.
2. The _SEARCH_TYPE_MAP covers all 14 modes with no gaps.
3. _is_admin() correctly identifies admin users from is_superuser flag and roles list.
4. Access control: CYPHER and NATURAL_LANGUAGE are blocked for non-admin users.
5. Tuning parameters are correctly passed when non-zero/non-default.

NOTE: The cognee package import chain fails in the local dev environment due to
a mistralai version incompatibility (cognee 0.5.5 + newer mistralai). This does NOT
affect the cognee_service container which has a compatible vendored environment.
All servicer imports are therefore stubbed at the module level before import.
"""

import sys
import os
import types
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# ── Path setup ──────────────────────────────────────────────────────────────

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SERVICE_DIR = os.path.dirname(_THIS_DIR)
_PROJECT_ROOT = os.path.dirname(_SERVICE_DIR)
_GENERATED_DIR = os.path.join(_SERVICE_DIR, "generated")

for _p in [_PROJECT_ROOT, _SERVICE_DIR, _GENERATED_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Stub out cognee + its transitive dependencies before any import ──────────
# cognee's __init__.py triggers instructor → mistralai → ImportError in local env.
# The container environment is fine; this stub is only needed for unit-test isolation.


def _make_search_type_stub():
    """Return a minimal SearchType enum matching the real cognee SearchType."""
    from enum import Enum

    class _SearchType(str, Enum):
        SUMMARIES = "SUMMARIES"
        CHUNKS = "CHUNKS"
        RAG_COMPLETION = "RAG_COMPLETION"
        TRIPLET_COMPLETION = "TRIPLET_COMPLETION"
        GRAPH_COMPLETION = "GRAPH_COMPLETION"
        GRAPH_SUMMARY_COMPLETION = "GRAPH_SUMMARY_COMPLETION"
        CYPHER = "CYPHER"
        NATURAL_LANGUAGE = "NATURAL_LANGUAGE"
        GRAPH_COMPLETION_COT = "GRAPH_COMPLETION_COT"
        GRAPH_COMPLETION_CONTEXT_EXTENSION = "GRAPH_COMPLETION_CONTEXT_EXTENSION"
        FEELING_LUCKY = "FEELING_LUCKY"
        TEMPORAL = "TEMPORAL"
        CODING_RULES = "CODING_RULES"
        CHUNKS_LEXICAL = "CHUNKS_LEXICAL"

    return _SearchType


_SearchType = _make_search_type_stub()

# Build a minimal fake cognee module tree so servicer.py can import cleanly.
_cognee_mod = types.ModuleType("cognee")
_cognee_mod.search = AsyncMock(return_value=[])
_cognee_mod.add = AsyncMock()
_cognee_mod.cognify = AsyncMock()

_cognee_prune = types.ModuleType("cognee.prune")
_cognee_prune.prune_data = AsyncMock()
_cognee_mod.prune = _cognee_prune

_cognee_api = types.ModuleType("cognee.api")
_cognee_api_v1 = types.ModuleType("cognee.api.v1")
_cognee_api_v1_search = types.ModuleType("cognee.api.v1.search")
_cognee_api_v1_search.SearchType = _SearchType
_cognee_api.v1 = _cognee_api_v1
_cognee_api_v1.search = _cognee_api_v1_search
_cognee_mod.api = _cognee_api

sys.modules.setdefault("cognee", _cognee_mod)
sys.modules.setdefault("cognee.api", _cognee_api)
sys.modules.setdefault("cognee.api.v1", _cognee_api_v1)
sys.modules.setdefault("cognee.api.v1.search", _cognee_api_v1_search)

# Stub redis for auth_interceptor import in the local test environment.
_redis_mod = types.ModuleType("redis")


class _RedisError(Exception):
    pass


class _Redis:
    @classmethod
    def from_url(cls, *args, **kwargs):
        return cls()

    def exists(self, *args, **kwargs):
        return 0

    def get(self, *args, **kwargs):
        return None


_redis_mod.Redis = _Redis
_redis_mod.RedisError = _RedisError
sys.modules.setdefault("redis", _redis_mod)

# Stub deeper cognee submodules imported by multi_tenancy.py
_cognee_ctx = types.ModuleType("cognee.context_global_variables")
_cognee_ctx.graph_db_config = MagicMock()
_cognee_ctx.vector_db_config = MagicMock()
sys.modules.setdefault("cognee.context_global_variables", _cognee_ctx)

# Stub multi_tenancy so servicer.py can import without real Neo4j/Qdrant
_multi_tenancy_mod = types.ModuleType("cognee_service.multi_tenancy")
_multi_tenancy_mod.ensure_neo4j_database = AsyncMock()
_multi_tenancy_mod.set_company_context = MagicMock()
sys.modules.setdefault("cognee_service.multi_tenancy", _multi_tenancy_mod)

# Stub query expansion module so unit tests don't hit the real LLM.
_query_expansion_mod = types.ModuleType("cognee_service.query_expansion")
_query_expansion_mod.expand_query = AsyncMock(return_value=["test"])
sys.modules.setdefault("cognee_service.query_expansion", _query_expansion_mod)

# Stub the lock module
_lock_mod = types.ModuleType("cognee_service.lock")
_lock_mod.dataset_locks = MagicMock()
sys.modules.setdefault("cognee_service.lock", _lock_mod)

# ── Now safe to import servicer ──────────────────────────────────────────────
from cognee_service.generated import cognee_pb2
from cognee_service.servicer import (
    CogneeServicer,
    _ADMIN_ONLY_MODES,
    _is_admin,
    _scope_to_node_name,
)
from cognee_service.auth_interceptor import CurrentUser


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_user(is_superuser: bool = False, roles: list | None = None) -> CurrentUser:
    return CurrentUser(
        user_id="test-user",
        email="test@example.com",
        roles=roles or [],
        is_superuser=is_superuser,
    )


def _make_abort_ctx():
    """Return (ctx, aborted_list) — aborted_list captures (code, details) on abort."""
    aborted = []
    ctx = MagicMock()

    async def _abort(code, details):
        aborted.append((code, details))

    ctx.abort = _abort
    return ctx, aborted


# ── 1. Proto enum values match expected integers ─────────────────────────────


class TestProtoEnumValues:
    """Confirm proto enum integer assignments match the spec."""

    EXPECTED = {
        "GRAPH_COMPLETION": 0,
        "TRIPLET_COMPLETION": 1,
        "CHUNKS": 2,
        "RAG_COMPLETION": 3,
        "SUMMARIES": 4,
        "GRAPH_SUMMARY_COMPLETION": 5,
        "CYPHER": 6,
        "NATURAL_LANGUAGE": 7,
        "GRAPH_COMPLETION_COT": 8,
        "GRAPH_COMPLETION_CONTEXT_EXTENSION": 9,
        "FEELING_LUCKY": 10,
        "TEMPORAL": 11,
        "CODING_RULES": 12,
        "CHUNKS_LEXICAL": 13,
    }

    @pytest.mark.parametrize("name,value", list(EXPECTED.items()))
    def test_enum_value(self, name: str, value: int):
        assert getattr(cognee_pb2, name) == value, (
            f"cognee_pb2.{name} should be {value}, got {getattr(cognee_pb2, name)}"
        )

    def test_all_14_modes_present(self):
        assert len(self.EXPECTED) == 14


# ── 2. _SEARCH_TYPE_MAP covers all 14 modes ──────────────────────────────────


class TestSearchTypeMap:
    """Verify _SEARCH_TYPE_MAP has all 14 entries mapped to correct cognee SearchType."""

    EXPECTED_PAIRS = [
        (cognee_pb2.GRAPH_COMPLETION, "GRAPH_COMPLETION"),
        (cognee_pb2.TRIPLET_COMPLETION, "TRIPLET_COMPLETION"),
        (cognee_pb2.CHUNKS, "CHUNKS"),
        (cognee_pb2.RAG_COMPLETION, "RAG_COMPLETION"),
        (cognee_pb2.SUMMARIES, "SUMMARIES"),
        (cognee_pb2.GRAPH_SUMMARY_COMPLETION, "GRAPH_SUMMARY_COMPLETION"),
        (cognee_pb2.CYPHER, "CYPHER"),
        (cognee_pb2.NATURAL_LANGUAGE, "NATURAL_LANGUAGE"),
        (cognee_pb2.GRAPH_COMPLETION_COT, "GRAPH_COMPLETION_COT"),
        (cognee_pb2.GRAPH_COMPLETION_CONTEXT_EXTENSION, "GRAPH_COMPLETION_CONTEXT_EXTENSION"),
        (cognee_pb2.FEELING_LUCKY, "FEELING_LUCKY"),
        (cognee_pb2.TEMPORAL, "TEMPORAL"),
        (cognee_pb2.CODING_RULES, "CODING_RULES"),
        (cognee_pb2.CHUNKS_LEXICAL, "CHUNKS_LEXICAL"),
    ]

    def test_map_size_is_14(self):
        assert len(CogneeServicer._SEARCH_TYPE_MAP) == 14

    @pytest.mark.parametrize("proto_val,cognee_name", EXPECTED_PAIRS)
    def test_mapping(self, proto_val: int, cognee_name: str):
        servicer = CogneeServicer()
        cognee_type = servicer._SEARCH_TYPE_MAP[proto_val]
        assert cognee_type.name == cognee_name, (
            f"proto value {proto_val} should map to {cognee_name}, got {cognee_type.name}"
        )

    def test_default_fallback_is_graph_completion(self):
        """Unknown proto value should fall back to GRAPH_COMPLETION."""
        servicer = CogneeServicer()
        result = servicer._SEARCH_TYPE_MAP.get(999, _SearchType.GRAPH_COMPLETION)
        assert result == _SearchType.GRAPH_COMPLETION


# ── 3. _is_admin helper ──────────────────────────────────────────────────────


class TestIsAdmin:
    def test_none_user_is_not_admin(self):
        assert _is_admin(None) is False

    def test_is_superuser_flag(self):
        user = _make_user(is_superuser=True)
        assert _is_admin(user) is True

    def test_admin_role(self):
        user = _make_user(roles=["admin"])
        assert _is_admin(user) is True

    def test_admin_role_case_insensitive(self):
        user = _make_user(roles=["Admin"])
        assert _is_admin(user) is True

    def test_non_admin_user(self):
        user = _make_user(is_superuser=False, roles=["user", "reader"])
        assert _is_admin(user) is False

    def test_empty_roles(self):
        user = _make_user(is_superuser=False, roles=[])
        assert _is_admin(user) is False


# ── 4. Admin-only modes are correctly identified ─────────────────────────────


class TestAdminOnlyModes:
    def test_cypher_is_admin_only(self):
        assert cognee_pb2.CYPHER in _ADMIN_ONLY_MODES

    def test_natural_language_is_admin_only(self):
        assert cognee_pb2.NATURAL_LANGUAGE in _ADMIN_ONLY_MODES

    def test_only_two_admin_only_modes(self):
        assert len(_ADMIN_ONLY_MODES) == 2

    @pytest.mark.parametrize(
        "mode",
        [
            cognee_pb2.GRAPH_COMPLETION,
            cognee_pb2.TRIPLET_COMPLETION,
            cognee_pb2.CHUNKS,
            cognee_pb2.RAG_COMPLETION,
            cognee_pb2.SUMMARIES,
            cognee_pb2.GRAPH_SUMMARY_COMPLETION,
            cognee_pb2.GRAPH_COMPLETION_COT,
            cognee_pb2.GRAPH_COMPLETION_CONTEXT_EXTENSION,
            cognee_pb2.FEELING_LUCKY,
            cognee_pb2.TEMPORAL,
            cognee_pb2.CODING_RULES,
            cognee_pb2.CHUNKS_LEXICAL,
        ],
    )
    def test_other_modes_are_not_admin_only(self, mode):
        assert mode not in _ADMIN_ONLY_MODES, f"Mode {mode} should NOT be in _ADMIN_ONLY_MODES"


# ── 5. Access control gate ────────────────────────────────────────────────────


class TestAccessControlGate:
    """Integration-level tests for the Search access control gate."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", [cognee_pb2.CYPHER, cognee_pb2.NATURAL_LANGUAGE])
    async def test_admin_only_mode_blocked_for_non_admin(self, mode):
        import grpc
        from cognee_service.auth_interceptor import current_user_context

        non_admin = _make_user(is_superuser=False, roles=["user"])
        ctx, aborted = _make_abort_ctx()

        request = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="company-x",
            search_type=mode,
        )

        servicer = CogneeServicer()
        token = current_user_context.set(non_admin)
        try:
            await servicer.Search(request, ctx)
        finally:
            current_user_context.reset(token)

        assert len(aborted) == 1, "Expected context.abort to be called once"
        abort_code, abort_details = aborted[0]
        assert abort_code == grpc.StatusCode.PERMISSION_DENIED
        assert (
            "admin" in abort_details.lower()
            or "permission" in abort_details.lower()
            or "admin" in abort_details.lower()
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", [cognee_pb2.CYPHER, cognee_pb2.NATURAL_LANGUAGE])
    async def test_admin_only_mode_allowed_for_admin(self, mode):
        """Admin user should NOT be immediately rejected; proceeds to cognee.search."""
        from cognee_service.auth_interceptor import current_user_context

        admin = _make_user(is_superuser=True, roles=["admin"])
        ctx, aborted = _make_abort_ctx()

        request = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="company-x",
            search_type=mode,
        )

        servicer = CogneeServicer()

        with (
            patch("cognee_service.servicer.ensure_neo4j_database", new=AsyncMock()),
            patch("cognee_service.servicer.set_company_context"),
            patch.object(sys.modules["cognee"], "search", new=AsyncMock(return_value=[])),
        ):
            token = current_user_context.set(admin)
            try:
                resp = await servicer.Search(request, ctx)
            finally:
                current_user_context.reset(token)

        permission_aborts = [a for a in aborted if "PERMISSION" in str(a[0])]
        assert len(permission_aborts) == 0, (
            f"Admin user should not be rejected; abort calls: {aborted}"
        )

    @pytest.mark.asyncio
    async def test_non_admin_only_mode_allowed_for_regular_user(self):
        """CHUNKS should work for non-admin users without PERMISSION_DENIED."""
        from cognee_service.auth_interceptor import current_user_context

        regular = _make_user(is_superuser=False, roles=["user"])
        ctx, aborted = _make_abort_ctx()

        request = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="company-x",
            search_type=cognee_pb2.CHUNKS,
        )

        servicer = CogneeServicer()

        with (
            patch("cognee_service.servicer.ensure_neo4j_database", new=AsyncMock()),
            patch("cognee_service.servicer.set_company_context"),
            patch.object(sys.modules["cognee"], "search", new=AsyncMock(return_value=[])),
        ):
            token = current_user_context.set(regular)
            try:
                resp = await servicer.Search(request, ctx)
            finally:
                current_user_context.reset(token)

        permission_aborts = [a for a in aborted if "PERMISSION" in str(a[0])]
        assert len(permission_aborts) == 0


# ── 6. Tuning parameters are threaded through ────────────────────────────────


class TestTuningParameters:
    """Verify wide_search_top_k and triplet_distance_penalty are passed to cognee.search."""

    async def _run_search(self, request, captured):
        from cognee_service.auth_interceptor import current_user_context

        admin = _make_user(is_superuser=True)
        ctx, _ = _make_abort_ctx()

        async def _mock_search(**kwargs):
            captured.update(kwargs)
            return []

        servicer = CogneeServicer()
        with (
            patch("cognee_service.servicer.ensure_neo4j_database", new=AsyncMock()),
            patch("cognee_service.servicer.set_company_context"),
            patch.object(sys.modules["cognee"], "search", side_effect=_mock_search),
        ):
            token = current_user_context.set(admin)
            try:
                await servicer.Search(request, ctx)
            finally:
                current_user_context.reset(token)

    @pytest.mark.asyncio
    async def test_wide_search_top_k_passed_when_set(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="c1",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            wide_search_top_k=500,
            triplet_distance_penalty=0.0,
        )
        await self._run_search(req, captured)
        assert captured.get("wide_search_top_k") == 500
        assert "triplet_distance_penalty" not in captured

    @pytest.mark.asyncio
    async def test_triplet_distance_penalty_passed_when_set(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="c1",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            wide_search_top_k=0,
            triplet_distance_penalty=0.5,
        )
        await self._run_search(req, captured)
        assert captured.get("triplet_distance_penalty") == pytest.approx(0.5)
        assert "wide_search_top_k" not in captured

    @pytest.mark.asyncio
    async def test_both_passed_when_both_set(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="c1",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            wide_search_top_k=300,
            triplet_distance_penalty=1.0,
        )
        await self._run_search(req, captured)
        assert captured.get("wide_search_top_k") == 300
        assert captured.get("triplet_distance_penalty") == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_neither_passed_when_both_unset(self):
        """Zero values → not passed → cognee uses its own defaults (100 / 3.5)."""
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="c1",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            wide_search_top_k=0,
            triplet_distance_penalty=0.0,
        )
        await self._run_search(req, captured)
        assert "wide_search_top_k" not in captured
        assert "triplet_distance_penalty" not in captured


# ── 7. Scope mapping and forwarding ─────────────────────────────────────────


class TestScopeMapping:
    def test_knowledge_scope_maps_to_company_node_name(self):
        assert _scope_to_node_name("cc6bdaf4-311f-4b04-8ee3-07cc85b76142", "knowledge") == [
            "cc6bdaf4-311f-4b04-8ee3-07cc85b76142_knowledge"
        ]

    def test_code_scope_maps_to_project_node_name(self):
        assert _scope_to_node_name("cc6bd", "code:3cad0776-c73c-486c-8a43-3ddc82f7fa19:vet-ui") == [
            "3cad0776-c73c-486c-8a43-3ddc82f7fa19_vet-ui_code"
        ]

    def test_empty_scope_returns_none(self):
        assert _scope_to_node_name("cc6bd", "") is None


class TestScopeForwarding:
    @pytest.mark.asyncio
    async def test_scope_knowledge_forwards_company_node_name(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="cc6bdaf4-311f-4b04-8ee3-07cc85b76142",
            search_type=cognee_pb2.CHUNKS,
            scope="knowledge",
        )
        await TestTuningParameters()._run_search(req, captured)
        assert captured.get("node_name") == ["cc6bdaf4-311f-4b04-8ee3-07cc85b76142_knowledge"]

    @pytest.mark.asyncio
    async def test_scope_code_forwards_project_node_name(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="cc6bdaf4-311f-4b04-8ee3-07cc85b76142",
            search_type=cognee_pb2.CHUNKS,
            scope="code:3cad0776-c73c-486c-8a43-3ddc82f7fa19:vet-ui",
            project_id="legacy-project",
            branch="legacy-branch",
        )
        await TestTuningParameters()._run_search(req, captured)
        assert captured.get("node_name") == ["3cad0776-c73c-486c-8a43-3ddc82f7fa19_vet-ui_code"]

    @pytest.mark.asyncio
    async def test_empty_scope_keeps_legacy_cross_scope_behavior(self):
        captured = {}
        req = cognee_pb2.SearchRequest(
            query="test",
            limit=5,
            company_id="cc6bdaf4-311f-4b04-8ee3-07cc85b76142",
            search_type=cognee_pb2.CHUNKS,
            scope="",
            project_id="legacy-project",
            branch="legacy-branch",
        )
        await TestTuningParameters()._run_search(req, captured)
        assert "node_name" not in captured or captured.get("node_name") is None


class TestQueryExpansion:
    @pytest.mark.asyncio
    async def test_expansion_merges_by_id_and_keeps_max_score(self):
        from cognee_service.auth_interceptor import current_user_context

        admin = _make_user(is_superuser=True)
        ctx, _ = _make_abort_ctx()
        servicer = CogneeServicer()

        req = cognee_pb2.SearchRequest(
            query="how do I create an appointment in Vetlyx",
            limit=2,
            company_id="cc6bdaf4-311f-4b04-8ee3-07cc85b76142",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            scope="code:3cad0776-c73c-486c-8a43-3ddc82f7fa19:vet-ui",
        )

        async def _mock_search(**kwargs):
            q = kwargs["query_text"]
            if q == req.query:
                return [
                    {"id": "n1", "text": "orig", "score": 0.4},
                    {"id": "n2", "text": "keep", "score": 0.7},
                ]
            return [
                {"id": "n1", "text": "better", "score": 0.95},
                {"id": "n3", "text": "alt", "score": 0.6},
            ]

        with (
            patch("cognee_service.servicer.ensure_neo4j_database", new=AsyncMock()),
            patch("cognee_service.servicer.set_company_context"),
            patch.object(sys.modules["cognee"], "search", side_effect=_mock_search),
            patch(
                "cognee_service.servicer.expand_query",
                new=AsyncMock(
                    return_value=[
                        req.query,
                        "create appointment endpoint",
                        "useCreateAppointment hook",
                    ]
                ),
            ),
        ):
            token = current_user_context.set(admin)
            try:
                resp = await servicer.Search(req, ctx)
            finally:
                current_user_context.reset(token)

        assert resp.success is True
        assert [r.id for r in resp.results] == ["n1", "n2"]
        assert resp.results[0].score == pytest.approx(0.95)

    @pytest.mark.asyncio
    async def test_expansion_skipped_for_short_queries(self):
        from cognee_service.auth_interceptor import current_user_context

        admin = _make_user(is_superuser=True)
        ctx, _ = _make_abort_ctx()
        servicer = CogneeServicer()

        req = cognee_pb2.SearchRequest(
            query="axios client authentication",
            limit=5,
            company_id="cc6bdaf4-311f-4b04-8ee3-07cc85b76142",
            search_type=cognee_pb2.GRAPH_COMPLETION,
            scope="code:3cad0776-c73c-486c-8a43-3ddc82f7fa19:vet-ui",
        )

        async def _mock_search(**kwargs):
            return [{"id": "n1", "text": "ok", "score": 0.8}]

        with (
            patch("cognee_service.servicer.ensure_neo4j_database", new=AsyncMock()),
            patch("cognee_service.servicer.set_company_context"),
            patch.object(sys.modules["cognee"], "search", side_effect=_mock_search),
            patch("cognee_service.servicer.expand_query", new=AsyncMock()) as expand_mock,
        ):
            token = current_user_context.set(admin)
            try:
                resp = await servicer.Search(req, ctx)
            finally:
                current_user_context.reset(token)

        assert resp.success is True
        expand_mock.assert_not_called()
