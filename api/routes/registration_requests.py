"""
KGRAG API - Registration Requests Routes
Public endpoint for submitting registration requests.

CRITICAL: This is a PUBLIC endpoint — NO authentication required.
"""

from fastapi import APIRouter, HTTPException, status
from loguru import logger

from api.models.registration_request import (
    RegistrationRequestCreate,
    RegistrationRequestResponse,
)
from api.database import get_db_pool
from api.repositories.registration_request_repository import RegistrationRequestRepository

router = APIRouter(prefix="/api/v1/registration-requests", tags=["registration-requests"])


async def get_registration_request_repo() -> RegistrationRequestRepository:
    """Dependency to provide RegistrationRequestRepository instance."""
    pool = await get_db_pool()
    return RegistrationRequestRepository(pool)


@router.post("", response_model=RegistrationRequestResponse, status_code=status.HTTP_201_CREATED)
async def create_registration_request(
    data: RegistrationRequestCreate,
):
    """
    Submit a registration request.

    **Public endpoint — NO authentication required.**

    **Request Body**:
    - full_name: Full name (required)
    - email: Valid email address (required)
    - organisation: Organisation name (required)
    - organisation_size: Size range — 1-5, 6-20, 21-50, 51-200, 200+ (required)
    - referral_source: How did you hear about us (optional)

    **Response**: 201 Created with RegistrationRequestResponse
    """
    logger.info(f"Registration request received from {data.email}")

    try:
        repo = await get_registration_request_repo()

        row = await repo.create(
            full_name=data.full_name,
            email=data.email,
            organisation=data.organisation,
            organisation_size=data.organisation_size,
            referral_source=data.referral_source,
        )

        logger.info(f"Registration request created: {row['id']} ({data.email})")

        return RegistrationRequestResponse(
            id=str(row["id"]),
            full_name=row["full_name"],
            email=row["email"],
            organisation=row["organisation"],
            organisation_size=row["organisation_size"],
            referral_source=row.get("referral_source"),
            status=row["status"],
            created_at=row["created_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to create registration request: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create registration request: {str(e)}"
        )
