"""
KGRAG API - Company Roles Routes
RESTful endpoints for company role management.

Per architecture plan (entry 733d587e-3a7b-48a0-88e0-d48f9298cfd3):
- GET    /companies/{company_id}/roles          - List roles
- POST   /companies/{company_id}/roles          - Create role
- GET    /companies/{company_id}/roles/{id}     - Get role
- PUT    /companies/{company_id}/roles/{id}     - Update role
- DELETE /companies/{company_id}/roles/{id}     - Delete role
- POST   /companies/{company_id}/roles/seed     - Re-seed defaults (admin)
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Optional
from loguru import logger

from api.services.company_role_service import CompanyRoleService
from api.models.company_role import (
    CompanyRoleCreate,
    CompanyRoleUpdate,
    CompanyRoleResponse,
    CompanyRoleListResponse,
)
from api.auth import CurrentUser, get_current_user, validate_company_access
from api.database import get_db_pool
from api.repositories.company_role_repository import CompanyRoleRepository

router = APIRouter(prefix="/api/v1/companies/{company_id}/roles", tags=["company-roles"])


async def get_role_service() -> CompanyRoleService:
    """Dependency to provide CompanyRoleService instance."""
    pool = await get_db_pool()
    role_repo = CompanyRoleRepository(pool)
    return CompanyRoleService(role_repo)


@router.get("", response_model=CompanyRoleListResponse)
async def list_roles(
    company_id: str,
    active_only: bool = Query(True, description="Only return active roles"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    List all roles for a company.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Query Parameters**:
    - active_only: Only return active roles (default: true)
    - limit: Maximum number of results (default: 100, max: 500)
    - offset: Number of results to skip (default: 0)

    **Response**: 200 OK with CompanyRoleListResponse
    """
    logger.info(f"Listing roles for company {company_id} (active_only={active_only})")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        result = await role_service.list_roles(
            company_id=company_id,
            active_only=active_only,
            limit=limit,
            offset=offset
        )

        role_responses = [
            CompanyRoleResponse(
                id=str(role["id"]),
                company_id=str(role["company_id"]),
                name=role["name"],
                display_name=role["display_name"],
                description=role.get("description"),
                color=role.get("color"),
                icon=role.get("icon"),
                is_system=role.get("is_system", False),
                is_active=role.get("is_active", True),
                sort_order=role.get("sort_order", 0),
                created_at=role["created_at"],
                updated_at=role["updated_at"]
            )
            for role in result["roles"]
        ]

        logger.info(f"Found {len(role_responses)} roles for company {company_id}")

        return CompanyRoleListResponse(
            roles=role_responses,
            total=result["total"]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to list roles: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list roles: {str(e)}"
        )


@router.post("", response_model=CompanyRoleResponse, status_code=status.HTTP_201_CREATED)
async def create_role(
    company_id: str,
    data: CompanyRoleCreate,
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    Create a new company role.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body**:
    - name: Role identifier (required, lowercase, hyphen-separated)
    - display_name: Human-readable name (required)
    - description: Role description (optional)
    - color: Hex color for UI (optional, e.g., '#10B981')
    - icon: Icon identifier (optional)
    - sort_order: UI ordering (optional, default: 0)

    **Response**: 201 Created with CompanyRoleResponse
    """
    logger.info(f"Creating role '{data.name}' for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        role = await role_service.create_role(
            company_id=company_id,
            name=data.name,
            display_name=data.display_name,
            description=data.description,
            color=data.color,
            icon=data.icon,
            sort_order=data.sort_order
        )

        logger.info(f"Created role {role['id']} ({data.name})")

        return CompanyRoleResponse(
            id=str(role["id"]),
            company_id=str(role["company_id"]),
            name=role["name"],
            display_name=role["display_name"],
            description=role.get("description"),
            color=role.get("color"),
            icon=role.get("icon"),
            is_system=role.get("is_system", False),
            is_active=role.get("is_active", True),
            sort_order=role.get("sort_order", 0),
            created_at=role["created_at"],
            updated_at=role["updated_at"]
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for role creation: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to create role: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create role: {str(e)}"
        )


@router.get("/{role_id}", response_model=CompanyRoleResponse)
async def get_role(
    company_id: str,
    role_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    Get a specific company role.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Response**: 200 OK with CompanyRoleResponse
    """
    logger.info(f"Getting role {role_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        role = await role_service.get_role(role_id)

        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role {role_id} not found"
            )

        # Validate role belongs to company
        if str(role["company_id"]) != str(company_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role {role_id} not found in company {company_id}"
            )

        return CompanyRoleResponse(
            id=str(role["id"]),
            company_id=str(role["company_id"]),
            name=role["name"],
            display_name=role["display_name"],
            description=role.get("description"),
            color=role.get("color"),
            icon=role.get("icon"),
            is_system=role.get("is_system", False),
            is_active=role.get("is_active", True),
            sort_order=role.get("sort_order", 0),
            created_at=role["created_at"],
            updated_at=role["updated_at"]
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid role_id or company_id format"
        )
    except Exception as e:
        logger.error("Failed to get role: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get role: {str(e)}"
        )


@router.put("/{role_id}", response_model=CompanyRoleResponse)
async def update_role(
    company_id: str,
    role_id: str,
    data: CompanyRoleUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    Update an existing company role.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Request Body** (all fields optional):
    - display_name: New human-readable name
    - description: New description
    - color: New hex color
    - icon: New icon identifier
    - is_active: Toggle active status
    - sort_order: New UI ordering

    **Note**: name and is_system are immutable.

    **Response**: 200 OK with CompanyRoleResponse
    """
    logger.info(f"Updating role {role_id} for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        role = await role_service.update_role(
            role_id=role_id,
            company_id=company_id,
            display_name=data.display_name,
            description=data.description,
            color=data.color,
            icon=data.icon,
            is_active=data.is_active,
            sort_order=data.sort_order
        )

        logger.info(f"Updated role {role_id}")

        return CompanyRoleResponse(
            id=str(role["id"]),
            company_id=str(role["company_id"]),
            name=role["name"],
            display_name=role["display_name"],
            description=role.get("description"),
            color=role.get("color"),
            icon=role.get("icon"),
            is_system=role.get("is_system", False),
            is_active=role.get("is_active", True),
            sort_order=role.get("sort_order", 0),
            created_at=role["created_at"],
            updated_at=role["updated_at"]
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid input for role update: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(e)}"
        )
    except Exception as e:
        logger.error("Failed to update role: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update role: {str(e)}"
        )


@router.delete("/{role_id}", response_model=dict)
async def delete_role(
    company_id: str,
    role_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    Delete a company role.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Restrictions**:
    - System roles cannot be deleted (deactivate instead)
    - Roles with assigned agents cannot be deleted (reassign or deactivate)

    **Response**: 200 OK with {"message": "Role deleted successfully"}
    """
    logger.info(f"Deleting role {role_id} from company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        await role_service.delete_role(role_id, company_id)

        logger.info(f"Deleted role {role_id}")

        return {"message": "Role deleted successfully"}

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid UUID format: {}", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid role_id or company_id format"
        )
    except Exception as e:
        logger.error("Failed to delete role: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete role: {str(e)}"
        )


@router.post("/seed", response_model=dict)
async def seed_default_roles(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    role_service: CompanyRoleService = Depends(get_role_service)
):
    """
    Re-seed default roles for a company.

    **Authentication Required**: User must be authenticated and have access to the company.

    Idempotent - skips roles that already exist.

    **Response**: 200 OK with {"created": [list of created role names], "total_created": int}
    """
    logger.info(f"Seeding default roles for company {company_id}")

    # Validate company access
    validate_company_access(current_user, company_id)

    try:
        created_roles = await role_service.ensure_defaults_exist(company_id)

        created_names = [role["name"] for role in created_roles]
        logger.info(f"Seeded {len(created_roles)} roles for company {company_id}")

        return {
            "created": created_names,
            "total_created": len(created_roles)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to seed roles: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to seed roles: {str(e)}"
        )
