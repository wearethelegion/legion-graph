"""
Company Config Service
Business logic for company-level configuration management.
"""

from typing import Dict, Any
from loguru import logger

from api.repositories.company_config_repository import CompanyConfigRepository
from api.models.company_config import CompanyConfigUpdate


class CompanyConfigService:
    """Service for company configuration operations."""

    def __init__(self, repository: CompanyConfigRepository):
        self.repository = repository

    async def get_config(self, company_id: str) -> Dict[str, Any]:
        """
        Get company config for a company.

        Auto-creates with defaults if none exists.

        Args:
            company_id: Company UUID

        Returns:
            Config record
        """
        return await self.repository.get_or_create(company_id)

    async def update_config(self, company_id: str, data: CompanyConfigUpdate) -> Dict[str, Any]:
        """
        Update company config for a company.

        Ensures config exists, then applies partial update.

        Args:
            company_id: Company UUID
            data: Update data (only provided fields are changed)

        Returns:
            Updated config record

        Raises:
            RuntimeError: If update fails
        """
        # Ensure config exists
        await self.repository.get_or_create(company_id)

        # Apply update with COALESCE semantics
        updated = await self.repository.update(
            company_id,
            summariser_enabled=data.summariser_enabled,
            advisor_mode_a_enabled=data.advisor_mode_a_enabled,
            advisor_mode_b_enabled=data.advisor_mode_b_enabled,
            auditor_enabled=data.auditor_enabled,
            summariser_prompt=data.summariser_prompt,
            advisor_prompt=data.advisor_prompt,
            settings=data.settings,
        )

        if not updated:
            raise RuntimeError(f"Failed to update company config for company {company_id}")

        return updated

    async def list_configs(self, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """
        List all company configs with pagination.

        Args:
            limit: Maximum number of results
            offset: Skip first N results

        Returns:
            Dict with 'total_count' and 'configs' list
        """
        return await self.repository.list_all(limit, offset)
