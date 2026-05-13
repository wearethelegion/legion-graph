"""
Company Config Repository
Database operations for company-level configuration.

Configuration is scoped per company with UNIQUE constraint.
get_or_create pattern ensures configs always exist (auto-creates with defaults).
"""

from typing import Optional, Dict, Any
from uuid import uuid4
import json
from loguru import logger

from api.repositories.base_repository import BaseRepository
from api.utils.text import sanitize_text


class CompanyConfigRepository(BaseRepository):
    """Repository for company configuration operations."""

    # Column list for SELECT statements
    _CONFIG_COLUMNS = """
        id, company_id, summariser_enabled, advisor_mode_a_enabled,
        advisor_mode_b_enabled, auditor_enabled, summariser_prompt,
        advisor_prompt, settings, created_at, updated_at
    """

    async def get_by_company(self, company_id: str) -> Optional[Dict[str, Any]]:
        """
        Get company config for a company.

        Args:
            company_id: Company UUID

        Returns:
            Config record or None if not found
        """
        query = f"""
            SELECT {self._CONFIG_COLUMNS}
            FROM company_configs
            WHERE company_id = $1
        """
        row = await self.fetch_one(query, company_id)

        if row:
            # Parse settings JSONB
            result = dict(row)
            if result.get("settings") is not None:
                if isinstance(result["settings"], str):
                    result["settings"] = json.loads(result["settings"])
            else:
                result["settings"] = {}
            return result

        return None

    async def create_default(self, company_id: str) -> Dict[str, Any]:
        """
        Create a new company config with default values.

        Defaults:
        - summariser_enabled: true
        - advisor_mode_a_enabled: false
        - advisor_mode_b_enabled: false
        - auditor_enabled: true
        - prompts: null
        - settings: {}

        Args:
            company_id: Company UUID

        Returns:
            Created config record

        Raises:
            RuntimeError: If creation fails
        """
        config_id = str(uuid4())

        query = f"""
            INSERT INTO company_configs (
                id, company_id, summariser_enabled, advisor_mode_a_enabled,
                advisor_mode_b_enabled, auditor_enabled, summariser_prompt,
                advisor_prompt, settings, created_at, updated_at
            )
            VALUES ($1, $2, true, false, false, true, NULL, NULL, '{{}}'::jsonb, NOW(), NOW())
            RETURNING {self._CONFIG_COLUMNS}
        """

        row = await self.fetch_one(query, config_id, company_id)

        if not row:
            raise RuntimeError("Failed to create company config")

        logger.info(f"Created company config: {row['id']} for company {company_id}")

        # Parse settings JSONB
        result = dict(row)
        if result.get("settings") is not None:
            if isinstance(result["settings"], str):
                result["settings"] = json.loads(result["settings"])
        else:
            result["settings"] = {}

        return result

    async def get_or_create(self, company_id: str) -> Dict[str, Any]:
        """
        Get existing config or create with defaults if none exists.

        Args:
            company_id: Company UUID

        Returns:
            Config record (existing or newly created)
        """
        config = await self.get_by_company(company_id)
        if config:
            return config

        logger.info(f"No company config found for company {company_id}, creating default")
        return await self.create_default(company_id)

    async def update(
        self,
        company_id: str,
        summariser_enabled: Optional[bool] = None,
        advisor_mode_a_enabled: Optional[bool] = None,
        advisor_mode_b_enabled: Optional[bool] = None,
        auditor_enabled: Optional[bool] = None,
        summariser_prompt: Optional[str] = None,
        advisor_prompt: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update company config with COALESCE semantics.

        Only provided fields are updated. None values mean "don't change".
        Sets updated_at to NOW() on every update.

        Args:
            company_id: Company UUID
            summariser_enabled: Optional new value
            advisor_mode_a_enabled: Optional new value
            advisor_mode_b_enabled: Optional new value
            auditor_enabled: Optional new value
            summariser_prompt: Optional new custom prompt
            advisor_prompt: Optional new custom prompt
            settings: Optional new settings dict

        Returns:
            Updated config record or None if not found
        """
        # Convert settings dict to JSON string if provided
        settings_json = None
        if settings is not None:
            settings_json = json.dumps(settings)

        query = f"""
            UPDATE company_configs
            SET summariser_enabled = COALESCE($2, summariser_enabled),
                advisor_mode_a_enabled = COALESCE($3, advisor_mode_a_enabled),
                advisor_mode_b_enabled = COALESCE($4, advisor_mode_b_enabled),
                auditor_enabled = COALESCE($5, auditor_enabled),
                summariser_prompt = COALESCE($6, summariser_prompt),
                advisor_prompt = COALESCE($7, advisor_prompt),
                settings = COALESCE($8::jsonb, settings),
                updated_at = NOW()
            WHERE company_id = $1
            RETURNING {self._CONFIG_COLUMNS}
        """

        row = await self.fetch_one(
            query,
            company_id,
            summariser_enabled,
            advisor_mode_a_enabled,
            advisor_mode_b_enabled,
            auditor_enabled,
            sanitize_text(summariser_prompt) if summariser_prompt is not None else None,
            sanitize_text(advisor_prompt) if advisor_prompt is not None else None,
            settings_json,
        )

        if row:
            logger.info(f"Updated company config for company {company_id}")

            # Parse settings JSONB
            result = dict(row)
            if result.get("settings") is not None:
                if isinstance(result["settings"], str):
                    result["settings"] = json.loads(result["settings"])
            else:
                result["settings"] = {}

            return result

        return None

    async def list_all(self, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """
        List all company configs with pagination.

        Args:
            limit: Maximum number of results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'configs' list
        """
        query = f"""
            SELECT {self._CONFIG_COLUMNS}, COUNT(*) OVER() as total_count
            FROM company_configs
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
        """

        rows = await self.fetch_all(query, limit, offset)

        total = rows[0]["total_count"] if rows else 0

        # Remove total_count from each row and parse settings
        configs = []
        for row in rows:
            config = dict(row)
            config.pop("total_count", None)

            # Parse settings JSONB
            if config.get("settings") is not None:
                if isinstance(config["settings"], str):
                    config["settings"] = json.loads(config["settings"])
            else:
                config["settings"] = {}

            configs.append(config)

        return {"total_count": total, "configs": configs}
