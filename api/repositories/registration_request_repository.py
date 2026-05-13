"""
Registration Request Repository
Database operations for registration requests.
"""

from typing import Optional, Dict, Any
from uuid import uuid4
from api.repositories.base_repository import BaseRepository
from loguru import logger


class RegistrationRequestRepository(BaseRepository):
    """Repository for registration request operations."""

    # Column list for SELECT statements
    _COLUMNS = """
        id, full_name, email, organisation, organisation_size,
        referral_source, status, created_at, updated_at
    """

    async def create(
        self,
        full_name: str,
        email: str,
        organisation: str,
        organisation_size: str,
        referral_source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new registration request.

        Args:
            full_name: Full name of the requester
            email: Email address
            organisation: Organisation name
            organisation_size: Organisation size range
            referral_source: Optional referral source

        Returns:
            Created registration request record
        """
        request_id = str(uuid4())

        query = """
            INSERT INTO registration_requests (
                id, full_name, email, organisation, organisation_size,
                referral_source, status, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW(), NOW())
            RETURNING id, full_name, email, organisation, organisation_size,
                      referral_source, status, created_at, updated_at
        """

        row = await self.fetch_one(
            query,
            request_id,
            full_name,
            email,
            organisation,
            organisation_size,
            referral_source
        )

        if not row:
            raise RuntimeError("Failed to create registration request")

        logger.info(f"Created registration request: {request_id} ({email})")
        return row

    async def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get registration request by email.

        Args:
            email: Email address to look up

        Returns:
            Registration request record or None if not found
        """
        query = f"""
            SELECT {self._COLUMNS}
            FROM registration_requests
            WHERE email = $1
            ORDER BY created_at DESC
            LIMIT 1
        """
        return await self.fetch_one(query, email)
