"""
Shared utilities for gRPC servicers.
Contains company_id resolution and other common servicer helpers.
"""

from typing import Optional, Protocol
import grpc
from loguru import logger


class HasCompanyAndProject(Protocol):
    """Structural type for any proto request that carries company/project fields."""

    @property
    def company_id(self) -> str: ...

    @property
    def project_id(self) -> str: ...


class CompanyResolutionError(Exception):
    """Internal signal: company_id could not be resolved. Carries gRPC status details."""

    def __init__(self, code: grpc.StatusCode, message: str, error_code: str):
        self.grpc_code = code
        self.message = message
        self.error_code = error_code
        super().__init__(message)


async def resolve_company_id(
    request: HasCompanyAndProject,
    current_user,
    project_repo,
) -> str:
    """
    Resolve and authorise company_id from a gRPC request.

    Resolution priority:
      1. request.company_id provided → validate membership → return it
      2. request.project_id provided → project_repo.get_company_id() → validate → return it
      3. Neither provided → raise CompanyResolutionError(INVALID_ARGUMENT)

    Args:
        request: Any proto request with .company_id and .project_id string fields.
        current_user: Authenticated user from gRPC context.
        project_repo: ProjectRepository instance (injected by caller).

    Returns:
        Validated company_id string.

    Raises:
        CompanyResolutionError: On any resolution failure. Caller should propagate
                                as a proto error response using .grpc_code, .message,
                                .error_code fields.
    """
    logger.debug(
        f"resolve_company_id ENTER | request.company_id={request.company_id!r} "
        f"request.project_id={request.project_id!r} | "
        f"user.companies={current_user.companies if current_user else None}"
    )

    if not current_user or not current_user.companies:
        logger.debug("resolve_company_id: no current_user or no companies → PERMISSION_DENIED")
        raise CompanyResolutionError(
            grpc.StatusCode.PERMISSION_DENIED,
            "No company membership associated with this session",
            "PERMISSION_DENIED",
        )

    company_id: Optional[str] = None

    if request.company_id:
        # Path 1: explicit company_id in request — validate membership
        company_id = request.company_id
        if company_id not in current_user.companies:
            logger.warning(
                f"resolve_company_id: company {company_id} not in "
                f"user {current_user.email} companies {current_user.companies}"
            )
            raise CompanyResolutionError(
                grpc.StatusCode.PERMISSION_DENIED,
                "Permission denied",
                "PERMISSION_DENIED",
            )
        logger.debug(f"resolve_company_id: PATH 1 (explicit company_id) → resolved={company_id!r}")
        return company_id

    if request.project_id:
        # Path 2: derive company_id from project
        resolved = await project_repo.get_company_id(request.project_id)
        if not resolved:
            logger.debug(
                f"resolve_company_id: PATH 2 project {request.project_id!r} not found → NOT_FOUND"
            )
            raise CompanyResolutionError(
                grpc.StatusCode.NOT_FOUND,
                f"Project {request.project_id} not found",
                "NOT_FOUND",
            )
        if resolved not in current_user.companies:
            logger.warning(
                f"resolve_company_id: project {request.project_id} belongs to "
                f"company {resolved} not in user {current_user.email} "
                f"companies {current_user.companies}"
            )
            raise CompanyResolutionError(
                grpc.StatusCode.NOT_FOUND,
                f"Project {request.project_id} not found",
                "NOT_FOUND",
            )
        logger.debug(
            f"resolve_company_id: PATH 2 (project→company) project={request.project_id!r} → resolved={resolved!r}"
        )
        return resolved

    # Path 3: neither present
    logger.debug("resolve_company_id: PATH 3 neither company_id nor project_id → INVALID_ARGUMENT")
    raise CompanyResolutionError(
        grpc.StatusCode.INVALID_ARGUMENT,
        "company_id or project_id is required",
        "INVALID_ARGUMENT",
    )
