"""Unit tests for code_preprocessor.project_analyzer.

All LLM calls and DB interactions are mocked — no real network or DB required.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from code_preprocessor.project_analyzer import (
    ProjectProfile,
    _AnalyzerRouter,
    _detect_language_framework,
    _framework_to_key,
    _split_camel_case,
    analyze_business_domains,
    analyze_project,
    analyze_technical_domains,
    deduplicate_domains,
    generate_chunker_config,
    generate_extraction_prompt,
    identify_project,
    normalize_domain_key,
)


# ---------------------------------------------------------------------------
# normalize_domain_key
# ---------------------------------------------------------------------------


class TestSplitCamelCase:
    """Tests for the CamelCase boundary splitter."""

    def test_lowercase_uppercase_boundary(self):
        assert _split_camel_case("aiIntegration") == "ai Integration"

    def test_allcaps_plus_capword(self):
        assert _split_camel_case("HTTPSConnection") == "HTTPS Connection"

    def test_already_spaced(self):
        assert _split_camel_case("AI Integration") == "AI Integration"

    def test_no_boundary(self):
        assert _split_camel_case("payment") == "payment"

    def test_multiple_boundaries(self):
        result = _split_camel_case("AIIntegration")
        # Should split "AI" boundary: AI + Integration
        assert "AI" in result
        assert "Integration" in result


class TestNormalizeDomainKey:
    def test_payment_processing(self):
        # "payment" base=7-4=3 < 4 → kept; "processing" → "process"
        assert normalize_domain_key("Payment Processing") == "payment_process"

    def test_user_authentication(self):
        # "authentication" → strip "tion" → "authentica" (base=10 ≥ 4); "user" kept
        assert normalize_domain_key("User Authentication") == "authentica_user"

    def test_order_management(self):
        # "order" kept; "management" → strip "ment" → "manage" (base=6 ≥ 4)
        assert normalize_domain_key("Order Management") == "manage_order"

    def test_single_word(self):
        assert normalize_domain_key("Billing") == "bill"

    def test_already_lowercase(self):
        key = normalize_domain_key("inventory tracking")
        assert key == key.lower()
        assert " " not in key

    def test_deterministic(self):
        assert normalize_domain_key("Foo Bar") == normalize_domain_key("bar foo")

    def test_empty_string(self):
        # Should return empty string gracefully
        result = normalize_domain_key("")
        assert result == ""

    # -- CamelCase normalisation tests (Story 4) --

    def test_camel_case_ai_integration(self):
        """AIIntegration must produce the same key as 'AI Integration'."""
        assert normalize_domain_key("AIIntegration") == normalize_domain_key("AI Integration")

    def test_camel_case_ai_integration_spaced_collides(self):
        """Explicit collision check: both forms give the identical string key."""
        key1 = normalize_domain_key("AIIntegration")
        key2 = normalize_domain_key("AI Integration")
        assert key1 == key2, f"Expected collision: {key1!r} != {key2!r}"

    def test_camel_case_https_connection(self):
        """HTTPSConnection must be split and produce a deterministic key."""
        key = normalize_domain_key("HTTPSConnection")
        assert key == normalize_domain_key("HTTPS Connection")

    def test_camel_case_user_payment(self):
        """userPayment (lowerCamelCase) splits correctly."""
        key = normalize_domain_key("userPayment")
        assert key == normalize_domain_key("user Payment")

    def test_lowercase_payment_unchanged(self):
        """Plain 'payment' with no CamelCase is unaffected by the splitter."""
        assert normalize_domain_key("payment") == "payment"

    def test_title_case_payment_processing(self):
        """'Payment Processing' (space-separated) works as before."""
        assert normalize_domain_key("Payment Processing") == "payment_process"


class TestFrameworkToKey:
    def test_rails_variants(self):
        assert _framework_to_key("Ruby on Rails") == "rails"
        assert _framework_to_key("Rails") == "rails"
        assert _framework_to_key("rails") == "rails"

    def test_react(self):
        assert _framework_to_key("React") == "react"
        assert _framework_to_key("react native") == "react"

    def test_nextjs_variants(self):
        assert _framework_to_key("Next.js") == "nextjs"
        assert _framework_to_key("NextJS") == "nextjs"
        assert _framework_to_key("next") == "nextjs"

    def test_django(self):
        assert _framework_to_key("Django") == "django"

    def test_fastapi(self):
        assert _framework_to_key("FastAPI") == "fastapi"

    def test_unknown_falls_back_to_default(self):
        assert _framework_to_key("Spring") == "default"
        assert _framework_to_key("unknown") == "default"
        assert _framework_to_key(None) == "default"
        assert _framework_to_key("") == "default"


# ---------------------------------------------------------------------------
# deduplicate_domains
# ---------------------------------------------------------------------------


class TestDeduplicateDomains:
    def _make_existing(self) -> list[dict]:
        return [
            {
                "id": "uuid-1",
                "canonical_name": "Payment Processing",
                "normalised_key": "payment_process",
                "description": "old description",
            },
            {
                "id": "uuid-2",
                "canonical_name": "User Management",
                "normalised_key": "manag_user",
                "description": "manages users",
            },
        ]

    def test_matching_key_goes_to_upsert(self):
        existing = self._make_existing()
        new = [
            {
                "canonical_name": "Payment Processing",
                "normalised_key": "payment_process",
                "description": "updated description",
            }
        ]
        upsert, insert = deduplicate_domains(existing, new)
        assert len(upsert) == 1
        assert len(insert) == 0
        assert upsert[0]["description"] == "updated description"
        assert upsert[0]["id"] == "uuid-1"

    def test_new_key_goes_to_insert(self):
        existing = self._make_existing()
        new = [
            {
                "canonical_name": "Inventory Management",
                "normalised_key": "inventori_manag",
                "description": "tracks inventory",
            }
        ]
        upsert, insert = deduplicate_domains(existing, new)
        assert len(upsert) == 0
        assert len(insert) == 1
        assert insert[0]["canonical_name"] == "Inventory Management"

    def test_mixed_upsert_and_insert(self):
        existing = self._make_existing()
        new = [
            {
                "canonical_name": "Payment Processing",
                "normalised_key": "payment_process",
                "description": "new payment desc",
            },
            {
                "canonical_name": "Scheduling",
                "normalised_key": "schedul",
                "description": "handles scheduling",
            },
        ]
        upsert, insert = deduplicate_domains(existing, new)
        assert len(upsert) == 1
        assert len(insert) == 1

    def test_dedup_within_llm_output(self):
        """Two LLM domains with same key — only first is kept."""
        existing: list[dict] = []
        new = [
            {"canonical_name": "Payments", "normalised_key": "payment", "description": "a"},
            {"canonical_name": "Payments Again", "normalised_key": "payment", "description": "b"},
        ]
        upsert, insert = deduplicate_domains(existing, new)
        assert len(upsert) + len(insert) == 1

    def test_no_normalised_key_falls_back_to_canonical_name(self):
        """If normalised_key is missing, derive it from canonical_name."""
        existing: list[dict] = []
        new = [{"canonical_name": "Order Tracking", "description": "tracks orders"}]
        upsert, insert = deduplicate_domains(existing, new)
        assert len(insert) == 1
        assert "order" in insert[0]["normalised_key"]

    def test_empty_inputs(self):
        upsert, insert = deduplicate_domains([], [])
        assert upsert == []
        assert insert == []


# ---------------------------------------------------------------------------
# _detect_language_framework
# ---------------------------------------------------------------------------


class TestDetectLanguageFramework:
    def test_rails(self):
        tree = "Gemfile\napp/controllers/\n  users_controller.rb\n"
        lang, fw = _detect_language_framework(tree)
        assert lang == "Ruby"
        assert "Rails" in fw

    def test_fastapi(self):
        tree = "main.py\npyproject.toml\napp/\n  routes.py\n"
        lang, fw = _detect_language_framework(tree)
        assert lang == "Python"
        assert fw == "FastAPI"

    def test_nextjs(self):
        tree = "package.json\nnext.config.js\npages/\n  index.tsx\n"
        lang, fw = _detect_language_framework(tree)
        assert lang == "TypeScript"
        assert fw == "Next.js"

    def test_go(self):
        tree = "go.mod\ncmd/\n  main.go\npkg/\n  service.go\n"
        lang, fw = _detect_language_framework(tree)
        assert lang == "Go"

    def test_unknown(self):
        tree = "some_file.xyz\nother/\n"
        lang, fw = _detect_language_framework(tree)
        assert lang == "unknown"


# ---------------------------------------------------------------------------
# _AnalyzerRouter.probe_provider
# ---------------------------------------------------------------------------


class TestAnalyzerRouterProbe:
    @pytest.mark.asyncio
    async def test_probe_returns_gemini_when_api_key_set_and_call_succeeds(self, monkeypatch):
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        router = _AnalyzerRouter()

        mock_resp = MagicMock()
        with patch("litellm.acompletion", new_callable=AsyncMock, return_value=mock_resp):
            model, region = await router.probe_provider()

        assert "gemini" in model
        assert region is None

    @pytest.mark.asyncio
    async def test_probe_returns_vertex_when_no_api_key(self, monkeypatch):
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        router = _AnalyzerRouter()

        # litellm.acompletion should NOT be called for Gemini (no key)
        with patch("litellm.acompletion", new_callable=AsyncMock) as mock_llm:
            model, region = await router.probe_provider()
            # Vertex path does call acompletion for the probe... but here we
            # skip calling it entirely since there's no key — verify model
            # Actually probe_provider only calls litellm when api_key exists
            # With no key it goes straight to vertex without calling litellm

        assert "vertex_ai" in model
        assert region is not None

    @pytest.mark.asyncio
    async def test_probe_falls_back_to_vertex_on_rate_limit(self, monkeypatch):
        import litellm as _litellm

        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        router = _AnalyzerRouter()

        async def _raise_rate_limit(**kwargs):
            raise _litellm.RateLimitError(
                message="billing cap", llm_provider="gemini", model="gemini-2.5-pro"
            )

        with patch("litellm.acompletion", side_effect=_raise_rate_limit):
            model, region = await router.probe_provider()

        assert "vertex_ai" in model
        assert region is not None

    @pytest.mark.asyncio
    async def test_probe_falls_back_to_vertex_on_any_error(self, monkeypatch):
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        router = _AnalyzerRouter()

        async def _raise_generic(**kwargs):
            raise Exception("connection refused")

        with patch("litellm.acompletion", side_effect=_raise_generic):
            model, region = await router.probe_provider()

        assert "vertex_ai" in model
        assert region is not None

    @pytest.mark.asyncio
    async def test_probe_called_exactly_once_per_run(self, monkeypatch):
        """probe_provider() issues at most 1 litellm call regardless of outcome."""
        import litellm as _litellm

        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        router = _AnalyzerRouter()

        mock_resp = MagicMock()
        with patch("litellm.acompletion", new_callable=AsyncMock, return_value=mock_resp) as m:
            await router.probe_provider()
            assert m.call_count == 1

    @pytest.mark.asyncio
    async def test_probe_uses_configured_gemini_model(self, monkeypatch):
        """ANALYZER_MODEL_GEMINI env var is read at module load; patch the module constant."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        # Patch the module-level constant that _AnalyzerRouter.__init__ reads
        with patch(
            "code_preprocessor.project_analyzer._ANALYZER_MODEL_GEMINI",
            "gemini/gemini-custom-model",
        ):
            router = _AnalyzerRouter()

        mock_resp = MagicMock()
        with patch("litellm.acompletion", new_callable=AsyncMock, return_value=mock_resp) as m:
            model, _ = await router.probe_provider()
            assert model == "gemini/gemini-custom-model"
            assert m.call_args.kwargs["model"] == "gemini/gemini-custom-model"

    @pytest.mark.asyncio
    async def test_probe_uses_configured_vertex_model(self, monkeypatch):
        """ANALYZER_MODEL_VERTEX env var is read at module load; patch the module constant."""
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        with patch(
            "code_preprocessor.project_analyzer._ANALYZER_MODEL_VERTEX",
            "vertex_ai/gemini-custom-vertex",
        ):
            router = _AnalyzerRouter()

        model, region = await router.probe_provider()
        assert model == "vertex_ai/gemini-custom-vertex"
        assert region is not None


# ---------------------------------------------------------------------------
# Individual LLM call wrappers (mock _llm_json)
# ---------------------------------------------------------------------------

_SAMPLE_TREE = "Gemfile\napp/controllers/\n  users_controller.rb\napp/models/\n  user.rb\n"

_SAMPLE_BUSINESS_DOMAINS = [
    {"canonical_name": "User Management", "normalised_key": "manag_user", "description": "..."},
    {"canonical_name": "Billing", "normalised_key": "bill", "description": "..."},
]

_SAMPLE_TECHNICAL_DOMAINS = [
    {"name": "API Layer", "description": "...", "patterns": ["app/controllers/"]},
    {"name": "Data Access", "description": "...", "patterns": ["app/models/"]},
]

_SAMPLE_CHUNKER_CONFIG = {
    "language": "Ruby",
    "framework": "Ruby on Rails",
    "ast_chunk_boundaries": [{"node_type": "method", "min_size_chars": 50, "max_size_chars": 1200}],
    "fallback_strategy": "recursive_text",
}

# Resolved provider kwargs shared by all wrapper tests
_GEMINI_KW = {"model": "gemini/gemini-2.5-pro", "vertex_region": None}
_VERTEX_KW = {"model": "vertex_ai/gemini-2.5-pro", "vertex_region": "europe-west1"}


def _make_mock_pool_with_prompt(
    system_prompt: str = "system",
    prompt_text: str = '{{"business_domains": []}}',
) -> MagicMock:
    """Build a minimal asyncpg Pool mock that returns a fake analysis prompt row."""
    pool = MagicMock()
    row = MagicMock()
    row.__getitem__ = lambda self, key: system_prompt if key == "system_prompt" else prompt_text
    pool.fetchrow = AsyncMock(return_value=row)
    return pool


class TestAnalyzeBusinessDomains:
    @pytest.mark.asyncio
    async def test_returns_domain_list(self):
        """analyze_business_domains returns a list when prompt + LLM succeed."""
        mock_llm_result = {"business_domains": _SAMPLE_BUSINESS_DOMAINS}
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value=MagicMock(
                __getitem__=lambda self, k: (
                    "sys" if k == "system_prompt" else "{file_tree}\n{existing_domains}"
                )
            )
        )
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=mock_llm_result,
        ):
            result = await analyze_business_domains(_SAMPLE_TREE, [], "rails", pool, **_GEMINI_KW)
            assert len(result) == 2
            assert result[0]["canonical_name"] == "User Management"

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_prompt_missing(self):
        """Returns [] (not raises) when no prompt row found in DB."""
        pool = MagicMock()
        pool.fetchrow = AsyncMock(return_value=None)
        result = await analyze_business_domains(_SAMPLE_TREE, [], "rails", pool, **_GEMINI_KW)
        assert result == []

    @pytest.mark.asyncio
    async def test_passes_model_and_region_to_llm_json(self):
        mock_result = {"business_domains": []}
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value=MagicMock(
                __getitem__=lambda self, k: (
                    "sys" if k == "system_prompt" else "{file_tree}\n{existing_domains}"
                )
            )
        )
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mock_llm:
            await analyze_business_domains(
                _SAMPLE_TREE,
                [],
                "rails",
                pool,
                model="vertex_ai/gemini-2.5-pro",
                vertex_region="us-central1",
            )
            _, kwargs = mock_llm.call_args
            assert kwargs["model"] == "vertex_ai/gemini-2.5-pro"
            assert kwargs["vertex_region"] == "us-central1"


class TestAnalyzeTechnicalDomains:
    @pytest.mark.asyncio
    async def test_returns_domain_list(self):
        mock_result = {"technical_domains": _SAMPLE_TECHNICAL_DOMAINS}
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            result = await analyze_technical_domains(
                _SAMPLE_TREE, "Ruby", "Ruby on Rails", **_GEMINI_KW
            )
            assert len(result) == 2
            assert result[0]["name"] == "API Layer"

    @pytest.mark.asyncio
    async def test_passes_model_and_region_to_llm_json(self):
        mock_result = {"technical_domains": []}
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mock_llm:
            await analyze_technical_domains(
                _SAMPLE_TREE, "Ruby", "Rails", model="vertex_ai/gemini-2.5-pro", vertex_region="eu"
            )
            _, kwargs = mock_llm.call_args
            assert kwargs["model"] == "vertex_ai/gemini-2.5-pro"
            assert kwargs["vertex_region"] == "eu"


class TestGenerateChunkerConfig:
    @pytest.mark.asyncio
    async def test_returns_config(self):
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=_SAMPLE_CHUNKER_CONFIG,
        ):
            result = await generate_chunker_config(
                _SAMPLE_TREE, "Ruby", "Ruby on Rails", **_GEMINI_KW
            )
            assert result["language"] == "Ruby"
            assert "ast_chunk_boundaries" in result

    @pytest.mark.asyncio
    async def test_passes_model_and_region_to_llm_json(self):
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_llm:
            await generate_chunker_config(
                _SAMPLE_TREE,
                "Ruby",
                "Rails",
                model="vertex_ai/gemini-2.5-pro",
                vertex_region="asia-east1",
            )
            _, kwargs = mock_llm.call_args
            assert kwargs["vertex_region"] == "asia-east1"


class TestGenerateExtractionPrompt:
    @pytest.mark.asyncio
    async def test_returns_filled_prompt(self):
        mock_result = {"filled_prompt": "Extract entities of type Foo and Bar..."}
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            result = await generate_extraction_prompt(
                template="template with {{PLACEHOLDERS}}",
                file_tree=_SAMPLE_TREE,
                business_domains=_SAMPLE_BUSINESS_DOMAINS,
                technical_domains=_SAMPLE_TECHNICAL_DOMAINS,
                language="Ruby",
                framework="Ruby on Rails",
                **_GEMINI_KW,
            )
            assert "Extract entities" in result

    @pytest.mark.asyncio
    async def test_returns_empty_string_on_missing_key(self):
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value={},
        ):
            result = await generate_extraction_prompt(
                template="t",
                file_tree="f",
                business_domains=[],
                technical_domains=[],
                language="Python",
                framework="FastAPI",
                **_GEMINI_KW,
            )
            assert result == ""

    @pytest.mark.asyncio
    async def test_passes_model_and_region_to_llm_json(self):
        with patch(
            "code_preprocessor.project_analyzer._llm_json",
            new_callable=AsyncMock,
            return_value={"filled_prompt": "ok"},
        ) as mock_llm:
            await generate_extraction_prompt(
                template="t",
                file_tree="f",
                business_domains=[],
                technical_domains=[],
                language="Python",
                framework="FastAPI",
                model="vertex_ai/gemini-2.5-pro",
                vertex_region="europe-west4",
            )
            _, kwargs = mock_llm.call_args
            assert kwargs["model"] == "vertex_ai/gemini-2.5-pro"
            assert kwargs["vertex_region"] == "europe-west4"


# ---------------------------------------------------------------------------
# analyze_project (orchestrator)
# ---------------------------------------------------------------------------


class TestAnalyzeProject:
    """Integration-style tests for the orchestrator — all external calls mocked.

    The new two-pass design (Phase 1 refactor):
      Pass 1: identify_project()        — serial, returns {language, framework, framework_key}
      Pass 2: analyze_business_domains() — serial, DB-driven, returns list[dict]
      Parallel: analyze_technical_domains + generate_chunker_config + _load_prompt_for_language
    """

    def _make_pool(self) -> MagicMock:
        pool = MagicMock()
        pool.fetch = AsyncMock(return_value=[])
        pool.fetchrow = AsyncMock(return_value=None)
        pool.execute = AsyncMock()

        # Simulate context manager for acquire()
        conn = MagicMock()
        conn.execute = AsyncMock()
        conn.transaction = MagicMock()
        conn.transaction.return_value.__aenter__ = AsyncMock(return_value=conn)
        conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
        pool.acquire = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        return pool

    def _patch_probe(self, model: str, region: str | None):
        """Return a patch for probe_provider returning a fixed (model, region)."""
        return patch(
            "code_preprocessor.project_analyzer._AnalyzerRouter.probe_provider",
            new_callable=AsyncMock,
            return_value=(model, region),
        )

    def _patch_identify(self, language="Ruby", framework="Ruby on Rails", framework_key="rails"):
        """Patch identify_project to return fixed values."""
        return patch(
            "code_preprocessor.project_analyzer.identify_project",
            new_callable=AsyncMock,
            return_value={
                "language": language,
                "framework": framework,
                "framework_key": framework_key,
            },
        )

    @pytest.mark.asyncio
    async def test_returns_project_profile(self, tmp_path):
        """Happy path — all LLM calls succeed, profile is returned."""
        (tmp_path / "Gemfile").write_text("gem 'rails'")
        (tmp_path / "app").mkdir()
        (tmp_path / "app" / "controllers").mkdir()
        (tmp_path / "app" / "controllers" / "application_controller.rb").write_text(
            "class ApplicationController; end"
        )

        pool = self._make_pool()

        with (
            self._patch_probe("vertex_ai/gemini-2.5-pro", "europe-west1"),
            self._patch_identify(),
            patch(
                "code_preprocessor.project_analyzer.analyze_business_domains",
                new_callable=AsyncMock,
                return_value=_SAMPLE_BUSINESS_DOMAINS,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_technical_domains",
                new_callable=AsyncMock,
                return_value=_SAMPLE_TECHNICAL_DOMAINS,
            ),
            patch(
                "code_preprocessor.project_analyzer.generate_chunker_config",
                new_callable=AsyncMock,
                return_value=_SAMPLE_CHUNKER_CONFIG,
            ),
            patch(
                "code_preprocessor.project_analyzer._load_prompt_for_language",
                new_callable=AsyncMock,
                return_value="filled extraction prompt",
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_project_profile",
                new_callable=AsyncMock,
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_business_domains",
                new_callable=AsyncMock,
            ),
        ):
            profile = await analyze_project(
                project_id="proj-uuid",
                repo_path=str(tmp_path),
                company_id="comp-uuid",
                pool=pool,
            )

        assert isinstance(profile, ProjectProfile)
        assert profile.project_id == "proj-uuid"
        assert profile.company_id == "comp-uuid"
        assert profile.language == "Ruby"
        assert "Rails" in profile.framework
        assert profile.extraction_prompt == "filled extraction prompt"

    @pytest.mark.asyncio
    async def test_probe_called_once_all_calls_use_same_provider(self, tmp_path):
        """Critical: probe runs once; identify_project, analyze_business_domains, technical,
        and chunker all receive the same locked model/region."""
        (tmp_path / "Gemfile").write_text("gem 'rails'")
        (tmp_path / "app").mkdir()

        pool = self._make_pool()
        locked_model = "vertex_ai/gemini-2.5-pro"
        locked_region = "europe-west1"

        received_models: list[str] = []
        received_regions: list[str | None] = []

        async def capture_identify(tree, *, model, vertex_region):
            received_models.append(model)
            received_regions.append(vertex_region)
            return {"language": "Ruby", "framework": "Ruby on Rails", "framework_key": "rails"}

        async def capture_business_domains(tree, domains, fw_key, p, *, model, vertex_region):
            received_models.append(model)
            received_regions.append(vertex_region)
            return _SAMPLE_BUSINESS_DOMAINS

        async def capture_technical_domains(tree, lang, fw, *, model, vertex_region):
            received_models.append(model)
            received_regions.append(vertex_region)
            return _SAMPLE_TECHNICAL_DOMAINS

        async def capture_chunker_config(tree, lang, fw, *, model, vertex_region):
            received_models.append(model)
            received_regions.append(vertex_region)
            return _SAMPLE_CHUNKER_CONFIG

        with (
            self._patch_probe(locked_model, locked_region) as mock_probe,
            patch(
                "code_preprocessor.project_analyzer.identify_project",
                side_effect=capture_identify,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_business_domains",
                side_effect=capture_business_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_technical_domains",
                side_effect=capture_technical_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.generate_chunker_config",
                side_effect=capture_chunker_config,
            ),
            patch(
                "code_preprocessor.project_analyzer._load_prompt_for_language",
                new_callable=AsyncMock,
                return_value="prompt",
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_project_profile",
                new_callable=AsyncMock,
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_business_domains",
                new_callable=AsyncMock,
            ),
        ):
            await analyze_project(
                project_id="p", repo_path=str(tmp_path), company_id="c", pool=pool
            )

        # Probe called exactly once
        assert mock_probe.call_count == 1

        # 4 LLM-consuming calls must all get the same locked provider
        assert len(received_models) == 4
        assert all(m == locked_model for m in received_models), received_models
        assert all(r == locked_region for r in received_regions), received_regions

    @pytest.mark.asyncio
    async def test_identify_and_domains_called_before_parallel(self, tmp_path):
        """Pass 1 (identify_project) and Pass 2 (analyze_business_domains) run before
        the parallel batch (technical + chunker)."""
        (tmp_path / "Gemfile").write_text("gem 'rails'")
        (tmp_path / "app").mkdir()

        pool = self._make_pool()
        call_order: list[str] = []

        async def mock_identify(*a, **kw):
            call_order.append("identify")
            return {"language": "Ruby", "framework": "Ruby on Rails", "framework_key": "rails"}

        async def mock_business_domains(*a, **kw):
            call_order.append("domains")
            return _SAMPLE_BUSINESS_DOMAINS

        async def mock_technical_domains(*a, **kw):
            call_order.append("technical")
            return _SAMPLE_TECHNICAL_DOMAINS

        async def mock_chunker_config(*a, **kw):
            call_order.append("chunker")
            return _SAMPLE_CHUNKER_CONFIG

        with (
            self._patch_probe("vertex_ai/gemini-2.5-pro", "europe-west1"),
            patch(
                "code_preprocessor.project_analyzer.identify_project",
                side_effect=mock_identify,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_business_domains",
                side_effect=mock_business_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_technical_domains",
                side_effect=mock_technical_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.generate_chunker_config",
                side_effect=mock_chunker_config,
            ),
            patch(
                "code_preprocessor.project_analyzer._load_prompt_for_language",
                new_callable=AsyncMock,
                return_value="prompt",
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_project_profile",
                new_callable=AsyncMock,
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_business_domains",
                new_callable=AsyncMock,
            ),
        ):
            await analyze_project(
                project_id="p", repo_path=str(tmp_path), company_id="c", pool=pool
            )

        # identify must come first, then domains, then the parallel pair
        assert call_order[0] == "identify"
        assert call_order[1] == "domains"
        assert set(call_order[2:]) == {"technical", "chunker"}

    @pytest.mark.asyncio
    async def test_invalid_repo_path_raises(self):
        pool = self._make_pool()
        with pytest.raises((ValueError, FileNotFoundError, Exception)):
            await analyze_project(
                project_id="p",
                repo_path="/nonexistent/path",
                company_id="c",
                pool=pool,
            )

    @pytest.mark.asyncio
    async def test_full_language_framework_override_skips_identify(self, tmp_path):
        """When both language AND framework are overridden, identify_project is not called."""
        (tmp_path / "some_file.rb").write_text("class Foo; end")
        pool = self._make_pool()

        with (
            self._patch_probe("vertex_ai/gemini-2.5-pro", "europe-west1"),
            patch(
                "code_preprocessor.project_analyzer.identify_project",
                new_callable=AsyncMock,
            ) as mock_identify,
            patch(
                "code_preprocessor.project_analyzer.analyze_business_domains",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_technical_domains",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "code_preprocessor.project_analyzer.generate_chunker_config",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "code_preprocessor.project_analyzer._load_prompt_for_language",
                new_callable=AsyncMock,
                return_value="",
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_project_profile",
                new_callable=AsyncMock,
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_business_domains",
                new_callable=AsyncMock,
            ),
        ):
            profile = await analyze_project(
                project_id="p",
                repo_path=str(tmp_path),
                company_id="c",
                pool=pool,
                language="Go",
                framework="Go",
            )

        assert profile.language == "Go"
        assert profile.framework == "Go"
        # identify_project should NOT have been called
        assert mock_identify.call_count == 0

    @pytest.mark.asyncio
    async def test_gemini_probe_success_all_calls_use_gemini(self, tmp_path):
        """When probe selects Gemini, all LLM analysis calls use gemini model + None region."""
        (tmp_path / "Gemfile").write_text("gem 'rails'")
        (tmp_path / "app").mkdir()
        pool = self._make_pool()

        received: list[tuple[str, str | None]] = []

        async def capture_identify(tree, *, model, vertex_region):
            received.append((model, vertex_region))
            return {"language": "Ruby", "framework": "Ruby on Rails", "framework_key": "rails"}

        async def capture_domains(*a, model, vertex_region, **kw):
            received.append((model, vertex_region))
            return []

        async def capture_chunker(*a, model, vertex_region, **kw):
            received.append((model, vertex_region))
            return {}

        with (
            self._patch_probe("gemini/gemini-2.5-pro", None),
            patch(
                "code_preprocessor.project_analyzer.identify_project",
                side_effect=capture_identify,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_business_domains",
                side_effect=capture_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.analyze_technical_domains",
                side_effect=capture_domains,
            ),
            patch(
                "code_preprocessor.project_analyzer.generate_chunker_config",
                side_effect=capture_chunker,
            ),
            patch(
                "code_preprocessor.project_analyzer._load_prompt_for_language",
                new_callable=AsyncMock,
                return_value="",
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_project_profile",
                new_callable=AsyncMock,
            ),
            patch(
                "code_preprocessor.project_analyzer._upsert_business_domains",
                new_callable=AsyncMock,
            ),
        ):
            await analyze_project(
                project_id="p", repo_path=str(tmp_path), company_id="c", pool=pool
            )

        assert len(received) == 4
        for model, region in received:
            assert model == "gemini/gemini-2.5-pro"
            assert region is None


# ---------------------------------------------------------------------------
# ProjectProfile.to_db_dict
# ---------------------------------------------------------------------------


class TestProjectProfileToDbDict:
    def test_jsonb_fields_serialised(self):
        profile = ProjectProfile(
            project_id="p",
            company_id="c",
            language="Python",
            framework="FastAPI",
            business_domains=[{"canonical_name": "Foo"}],
            technical_domains=[{"name": "Bar"}],
            chunker_config={"fallback_strategy": "recursive_text"},
            extraction_prompt="extract this",
        )
        d = profile.to_db_dict()
        assert json.loads(d["business_domains"]) == [{"canonical_name": "Foo"}]
        assert json.loads(d["technical_domains"]) == [{"name": "Bar"}]
        assert json.loads(d["chunker_config"])["fallback_strategy"] == "recursive_text"
        assert d["extraction_prompt"] == "extract this"
