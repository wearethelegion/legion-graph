"""Unit tests for ProjectProfileStore — Code Intelligence Pipeline V3.

Tests all store methods with a mocked asyncpg pool.
No real database required.
"""

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from code_preprocessor.storage.project_profile_store import ProjectProfileStore


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_record(**kwargs) -> MagicMock:
    """Build a MagicMock that behaves like an asyncpg.Record (dict-like)."""
    record = MagicMock()
    record.__iter__ = MagicMock(return_value=iter(kwargs.items()))
    record.__getitem__ = MagicMock(side_effect=kwargs.__getitem__)
    record.get = MagicMock(side_effect=kwargs.get)
    # dict(record) uses keys() + __getitem__
    record.keys = MagicMock(return_value=kwargs.keys())
    # Make dict(record) work correctly
    record.__class__ = dict
    # Simplest path: make dict(row) == kwargs
    record._data = kwargs

    class FakeRecord:
        def __init__(self, data):
            self._data = data

        def __iter__(self):
            return iter(self._data.items())

        def __getitem__(self, key):
            return self._data[key]

        def get(self, key, default=None):
            return self._data.get(key, default)

        def keys(self):
            return self._data.keys()

    return FakeRecord(kwargs)


def _ts() -> str:
    """Return a fixed ISO timestamp string."""
    return "2026-04-12T10:00:00+00:00"


def _dt() -> datetime:
    return datetime(2026, 4, 12, 10, 0, 0, tzinfo=timezone.utc)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_pool() -> AsyncMock:
    pool = AsyncMock()
    pool.execute = AsyncMock(return_value="DELETE 0")
    pool.executemany = AsyncMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetchval = AsyncMock(return_value=None)
    return pool


@pytest.fixture
def store(mock_pool) -> ProjectProfileStore:
    return ProjectProfileStore(mock_pool)


@pytest.fixture
def project_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def company_id() -> str:
    return str(uuid.uuid4())


# ── extraction_prompt_templates ───────────────────────────────────────────────


class TestGetPromptTemplate:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, store, mock_pool):
        mock_pool.fetchrow.return_value = None
        result = await store.get_prompt_template(version=1)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_dict_with_iso_timestamp(self, store, mock_pool):
        tid = str(uuid.uuid4())
        mock_pool.fetchrow.return_value = _make_record(
            id=tid,
            template_text="hello {language}",
            version=1,
            created_at=_dt(),
        )
        result = await store.get_prompt_template(version=1)
        assert result is not None
        assert result["version"] == 1
        assert result["template_text"] == "hello {language}"
        assert "T" in result["created_at"]  # ISO format

    @pytest.mark.asyncio
    async def test_queries_by_version(self, store, mock_pool):
        mock_pool.fetchrow.return_value = None
        await store.get_prompt_template(version=3)
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "version = $1" in sql
        assert mock_pool.fetchrow.call_args[0][1] == 3


class TestGetLatestPromptTemplate:
    @pytest.mark.asyncio
    async def test_returns_none_when_table_empty(self, store, mock_pool):
        mock_pool.fetchrow.return_value = None
        result = await store.get_latest_prompt_template()
        assert result is None

    @pytest.mark.asyncio
    async def test_orders_by_version_desc(self, store, mock_pool):
        mock_pool.fetchrow.return_value = None
        await store.get_latest_prompt_template()
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "ORDER BY version DESC" in sql
        assert "LIMIT 1" in sql


class TestCreatePromptTemplate:
    @pytest.mark.asyncio
    async def test_inserts_and_returns_row(self, store, mock_pool):
        tid = str(uuid.uuid4())
        mock_pool.fetchrow.return_value = _make_record(
            id=tid,
            template_text="tmpl",
            version=2,
            created_at=_dt(),
        )
        result = await store.create_prompt_template("tmpl", version=2)
        assert result["id"] == tid
        assert result["version"] == 2

    @pytest.mark.asyncio
    async def test_passes_correct_params(self, store, mock_pool):
        mock_pool.fetchrow.return_value = _make_record(
            id="x", template_text="t", version=5, created_at=_dt()
        )
        await store.create_prompt_template("my template text", version=5)
        args = mock_pool.fetchrow.call_args[0]
        assert args[1] == "my template text"
        assert args[2] == 5


# ── project_profiles ──────────────────────────────────────────────────────────


class TestGetProjectProfile:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, store, mock_pool, project_id):
        mock_pool.fetchrow.return_value = None
        result = await store.get_project_profile(project_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_profile_dict(self, store, mock_pool, project_id):
        pid = str(uuid.uuid4())
        mock_pool.fetchrow.return_value = _make_record(
            id=pid,
            project_id=project_id,
            language="python",
            framework="fastapi",
            chunker_config={"max_tokens": 512},
            extraction_prompt="extract...",
            technical_domains=["api", "database"],
            analysed_at=_dt(),
            created_at=_dt(),
            updated_at=_dt(),
        )
        result = await store.get_project_profile(project_id)
        assert result["project_id"] == project_id
        assert result["language"] == "python"
        assert "T" in result["created_at"]

    @pytest.mark.asyncio
    async def test_queries_by_project_id(self, store, mock_pool, project_id):
        mock_pool.fetchrow.return_value = None
        await store.get_project_profile(project_id)
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "project_id = $1" in sql
        assert mock_pool.fetchrow.call_args[0][1] == project_id


class TestUpsertProjectProfile:
    @pytest.mark.asyncio
    async def test_inserts_on_first_call(self, store, mock_pool, project_id):
        pid = str(uuid.uuid4())
        mock_pool.fetchrow.return_value = _make_record(
            id=pid,
            project_id=project_id,
            language="python",
            framework="fastapi",
            chunker_config=None,
            extraction_prompt=None,
            technical_domains=None,
            analysed_at=_dt(),
            created_at=_dt(),
            updated_at=_dt(),
        )
        result = await store.upsert_project_profile(
            project_id=project_id,
            language="python",
            framework="fastapi",
        )
        assert result["project_id"] == project_id
        assert result["language"] == "python"

    @pytest.mark.asyncio
    async def test_sql_contains_on_conflict(self, store, mock_pool, project_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            project_id=project_id,
            language=None,
            framework=None,
            chunker_config=None,
            extraction_prompt=None,
            technical_domains=None,
            analysed_at=None,
            created_at=_dt(),
            updated_at=_dt(),
        )
        await store.upsert_project_profile(project_id=project_id)
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "uq_pp_project_id" in sql

    @pytest.mark.asyncio
    async def test_serialises_chunker_config_as_json(self, store, mock_pool, project_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            project_id=project_id,
            language=None,
            framework=None,
            chunker_config=None,
            extraction_prompt=None,
            technical_domains=None,
            analysed_at=None,
            created_at=_dt(),
            updated_at=_dt(),
        )
        cfg = {"max_tokens": 256, "overlap": 32}
        await store.upsert_project_profile(project_id=project_id, chunker_config=cfg)
        args = mock_pool.fetchrow.call_args[0]
        # chunker_config is the 4th positional param (index 4)
        chunker_arg = args[4]
        assert json.loads(chunker_arg) == cfg

    @pytest.mark.asyncio
    async def test_serialises_technical_domains_as_json(self, store, mock_pool, project_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            project_id=project_id,
            language=None,
            framework=None,
            chunker_config=None,
            extraction_prompt=None,
            technical_domains=None,
            analysed_at=None,
            created_at=_dt(),
            updated_at=_dt(),
        )
        domains = ["api", "storage", "auth"]
        await store.upsert_project_profile(project_id=project_id, technical_domains=domains)
        args = mock_pool.fetchrow.call_args[0]
        # technical_domains is the 6th positional param (index 6)
        td_arg = args[6]
        assert json.loads(td_arg) == domains


class TestDeleteProjectProfile:
    @pytest.mark.asyncio
    async def test_returns_true_when_deleted(self, store, mock_pool, project_id):
        mock_pool.execute.return_value = "DELETE 1"
        result = await store.delete_project_profile(project_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, store, mock_pool, project_id):
        mock_pool.execute.return_value = "DELETE 0"
        result = await store.delete_project_profile(project_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_deletes_by_project_id(self, store, mock_pool, project_id):
        mock_pool.execute.return_value = "DELETE 1"
        await store.delete_project_profile(project_id)
        sql = mock_pool.execute.call_args[0][0]
        assert "project_profiles" in sql
        assert "project_id = $1" in sql


# ── company_business_domains ──────────────────────────────────────────────────


class TestListCompanyDomains:
    @pytest.mark.asyncio
    async def test_returns_empty_list_when_none(self, store, mock_pool, company_id):
        mock_pool.fetch.return_value = []
        result = await store.list_company_domains(company_id)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(self, store, mock_pool, company_id):
        did = str(uuid.uuid4())
        mock_pool.fetch.return_value = [
            _make_record(
                id=did,
                company_id=company_id,
                canonical_name="Payments",
                normalised_key="payments",
                description="Payment processing domain",
                aliases=["billing", "checkout"],
                created_at=_dt(),
                updated_at=_dt(),
            )
        ]
        result = await store.list_company_domains(company_id)
        assert len(result) == 1
        assert result[0]["canonical_name"] == "Payments"

    @pytest.mark.asyncio
    async def test_filters_by_company_id(self, store, mock_pool, company_id):
        mock_pool.fetch.return_value = []
        await store.list_company_domains(company_id)
        sql = mock_pool.fetch.call_args[0][0]
        assert "company_id = $1" in sql
        assert mock_pool.fetch.call_args[0][1] == company_id

    @pytest.mark.asyncio
    async def test_applies_limit_and_offset(self, store, mock_pool, company_id):
        mock_pool.fetch.return_value = []
        await store.list_company_domains(company_id, limit=10, offset=20)
        args = mock_pool.fetch.call_args[0]
        assert args[2] == 10  # limit
        assert args[3] == 20  # offset


class TestGetCompanyDomain:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, store, mock_pool, company_id):
        mock_pool.fetchrow.return_value = None
        result = await store.get_company_domain(company_id, "payments")
        assert result is None

    @pytest.mark.asyncio
    async def test_queries_by_company_and_key(self, store, mock_pool, company_id):
        mock_pool.fetchrow.return_value = None
        await store.get_company_domain(company_id, "auth")
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "company_id = $1" in sql
        assert "normalised_key = $2" in sql
        args = mock_pool.fetchrow.call_args[0]
        assert args[1] == company_id
        assert args[2] == "auth"


class TestUpsertCompanyDomain:
    @pytest.mark.asyncio
    async def test_inserts_domain(self, store, mock_pool, company_id):
        did = str(uuid.uuid4())
        mock_pool.fetchrow.return_value = _make_record(
            id=did,
            company_id=company_id,
            canonical_name="Payments",
            normalised_key="payments",
            description="Handles all payment operations",
            aliases=["billing"],
            created_at=_dt(),
            updated_at=_dt(),
        )
        result = await store.upsert_company_domain(
            company_id=company_id,
            canonical_name="Payments",
            normalised_key="payments",
            description="Handles all payment operations",
            aliases=["billing"],
        )
        assert result["canonical_name"] == "Payments"
        assert result["company_id"] == company_id

    @pytest.mark.asyncio
    async def test_sql_contains_on_conflict_constraint(self, store, mock_pool, company_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            company_id=company_id,
            canonical_name="Auth",
            normalised_key="auth",
            description=None,
            aliases=[],
            created_at=_dt(),
            updated_at=_dt(),
        )
        await store.upsert_company_domain(
            company_id=company_id,
            canonical_name="Auth",
            normalised_key="auth",
        )
        sql = mock_pool.fetchrow.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "uq_cbd_company_key" in sql

    @pytest.mark.asyncio
    async def test_aliases_defaults_to_empty_list(self, store, mock_pool, company_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            company_id=company_id,
            canonical_name="Auth",
            normalised_key="auth",
            description=None,
            aliases=[],
            created_at=_dt(),
            updated_at=_dt(),
        )
        await store.upsert_company_domain(
            company_id=company_id,
            canonical_name="Auth",
            normalised_key="auth",
        )
        args = mock_pool.fetchrow.call_args[0]
        # aliases is the 5th positional param (index 5)
        aliases_arg = args[5]
        assert json.loads(aliases_arg) == []

    @pytest.mark.asyncio
    async def test_serialises_aliases_as_json(self, store, mock_pool, company_id):
        mock_pool.fetchrow.return_value = _make_record(
            id="x",
            company_id=company_id,
            canonical_name="Billing",
            normalised_key="billing",
            description=None,
            aliases=["payments", "checkout"],
            created_at=_dt(),
            updated_at=_dt(),
        )
        await store.upsert_company_domain(
            company_id=company_id,
            canonical_name="Billing",
            normalised_key="billing",
            aliases=["payments", "checkout"],
        )
        args = mock_pool.fetchrow.call_args[0]
        aliases_arg = args[5]
        assert json.loads(aliases_arg) == ["payments", "checkout"]


class TestDeleteCompanyDomain:
    @pytest.mark.asyncio
    async def test_returns_true_when_deleted(self, store, mock_pool, company_id):
        mock_pool.execute.return_value = "DELETE 1"
        result = await store.delete_company_domain(company_id, "payments")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, store, mock_pool, company_id):
        mock_pool.execute.return_value = "DELETE 0"
        result = await store.delete_company_domain(company_id, "payments")
        assert result is False

    @pytest.mark.asyncio
    async def test_deletes_by_company_and_key(self, store, mock_pool, company_id):
        mock_pool.execute.return_value = "DELETE 1"
        await store.delete_company_domain(company_id, "auth")
        sql = mock_pool.execute.call_args[0][0]
        assert "company_business_domains" in sql
        assert "company_id = $1" in sql
        assert "normalised_key = $2" in sql


class TestBulkUpsertCompanyDomains:
    @pytest.mark.asyncio
    async def test_empty_list_returns_zero_without_db_call(self, store, mock_pool, company_id):
        result = await store.bulk_upsert_company_domains(company_id, [])
        assert result == 0
        mock_pool.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_calls_executemany_with_correct_count(self, store, mock_pool, company_id):
        domains = [
            {"canonical_name": "Payments", "normalised_key": "payments"},
            {"canonical_name": "Auth", "normalised_key": "auth"},
            {"canonical_name": "Notifications", "normalised_key": "notifications"},
        ]
        result = await store.bulk_upsert_company_domains(company_id, domains)
        assert result == 3
        mock_pool.executemany.assert_called_once()

    @pytest.mark.asyncio
    async def test_sql_contains_on_conflict(self, store, mock_pool, company_id):
        domains = [{"canonical_name": "X", "normalised_key": "x"}]
        await store.bulk_upsert_company_domains(company_id, domains)
        sql = mock_pool.executemany.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "uq_cbd_company_key" in sql

    @pytest.mark.asyncio
    async def test_each_record_starts_with_company_id(self, store, mock_pool, company_id):
        domains = [
            {"canonical_name": "A", "normalised_key": "a", "aliases": ["aa"]},
            {"canonical_name": "B", "normalised_key": "b"},
        ]
        await store.bulk_upsert_company_domains(company_id, domains)
        records = mock_pool.executemany.call_args[0][1]
        assert all(r[0] == company_id for r in records)

    @pytest.mark.asyncio
    async def test_aliases_serialised_as_json_per_record(self, store, mock_pool, company_id):
        domains = [
            {"canonical_name": "Payments", "normalised_key": "payments", "aliases": ["billing"]},
        ]
        await store.bulk_upsert_company_domains(company_id, domains)
        records = mock_pool.executemany.call_args[0][1]
        aliases_arg = records[0][4]  # 5th element in tuple
        assert json.loads(aliases_arg) == ["billing"]

    @pytest.mark.asyncio
    async def test_missing_aliases_defaults_to_empty(self, store, mock_pool, company_id):
        domains = [{"canonical_name": "Auth", "normalised_key": "auth"}]
        await store.bulk_upsert_company_domains(company_id, domains)
        records = mock_pool.executemany.call_args[0][1]
        aliases_arg = records[0][4]
        assert json.loads(aliases_arg) == []


# ── ProjectProfileStore.close ─────────────────────────────────────────────────


class TestClose:
    @pytest.mark.asyncio
    async def test_close_is_noop(self, store):
        await store.close()  # Must not raise


# ── ProjectProfileStore class construction ────────────────────────────────────


class TestConstruction:
    def test_stores_pool_reference(self, mock_pool):
        s = ProjectProfileStore(mock_pool)
        assert s._pool is mock_pool

    def test_affected_parses_update_tag(self):
        assert ProjectProfileStore._affected("UPDATE 3") == 3

    def test_affected_parses_delete_tag(self):
        assert ProjectProfileStore._affected("DELETE 1") == 1

    def test_affected_returns_zero_for_empty_tag(self):
        assert ProjectProfileStore._affected("") == 0

    def test_row_to_dict_converts_timestamps(self):
        row = _make_record(
            id="x",
            created_at=_dt(),
            updated_at=_dt(),
            analysed_at=_dt(),
        )
        result = ProjectProfileStore._row_to_dict(row)
        assert result["created_at"] == _ts()
        assert result["updated_at"] == _ts()
        assert result["analysed_at"] == _ts()

    def test_row_to_dict_handles_null_timestamps(self):
        row = _make_record(id="x", created_at=None, updated_at=None, analysed_at=None)
        result = ProjectProfileStore._row_to_dict(row)
        assert result["created_at"] is None
        assert result["updated_at"] is None
        assert result["analysed_at"] is None
