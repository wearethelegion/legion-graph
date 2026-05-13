"""
Auth Service gRPC Servicer
Implements authentication and authorization RPCs.
"""

import os
import grpc
import httpx
from loguru import logger
from typing import Optional

from grpc_server.protos.loader import auth_pb2, auth_pb2_grpc
from api.auth import verify_token, CurrentUser
from api.repositories.project_repository import ProjectRepository
from api.database import get_db_pool
from grpc_server.utils.auth import get_current_user_from_context


class AuthServicer(auth_pb2_grpc.AuthServiceServicer):
    """
    AuthService gRPC implementation.

    Delegates to existing business logic:
    - verify_token from api.auth
    - ProjectRepository for project operations
    """

    def __init__(self, db_pool=None):
        """
        Initialize AuthServicer.

        Args:
            db_pool: asyncpg connection pool (optional, will fetch if not provided)
        """
        self.db_pool = db_pool
        self.auth_service_url = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")

    async def _get_db_pool(self):
        """Get database pool (lazy load if not provided)."""
        if self.db_pool is None:
            self.db_pool = await get_db_pool()
        return self.db_pool

    async def Authenticate(
        self, request: auth_pb2.AuthRequest, context: grpc.aio.ServicerContext
    ) -> auth_pb2.AuthResponse:
        """
        Authenticate user with email/password.

        Note: This is a stub implementation. In production, this would:
        1. Validate credentials against auth service
        2. Generate JWT tokens
        3. Return tokens to client

        For now, we'll validate the token if provided in metadata.

        Args:
            request: AuthRequest with email and password
            context: gRPC context

        Returns:
            AuthResponse with status and tokens
        """
        logger.info(f"Authenticate request for email: {request.email}")

        # Extract token from metadata if present (for token refresh flows)
        metadata = dict(context.invocation_metadata())
        auth_header = metadata.get("authorization", "")

        if auth_header:
            # Token-based auth (refresh token flow)
            parts = auth_header.split(" ")
            if len(parts) == 2 and parts[0].lower() == "bearer":
                token = parts[1]
                payload = await verify_token(token)

                if payload:
                    return auth_pb2.AuthResponse(
                        status="success",
                        access_token=token,  # Return same token (simplified)
                        refresh_token="",
                        token_type="bearer",
                        expires_in=3600,
                        user_email=payload["email"],
                        message="Token validated successfully",
                    )

        # Password-based authentication - delegate to REST auth service
        if not request.email or not request.password:
            return auth_pb2.AuthResponse(
                status="error",
                access_token="",
                refresh_token="",
                token_type="",
                expires_in=0,
                user_email="",
                message="Email and password required",
            )

        # Call REST auth service to validate credentials and get tokens
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.auth_service_url}/login",
                    json={"email": request.email, "password": request.password},
                )

                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"Authentication successful for user: {request.email}")

                    return auth_pb2.AuthResponse(
                        status="success",
                        access_token=data["access_token"],
                        refresh_token=data.get("refresh_token", ""),
                        token_type=data.get("token_type", "bearer"),
                        expires_in=data.get("expires_in", 3600),
                        user_email=request.email,
                        message="Authentication successful",
                    )
                else:
                    error_detail = response.json().get("detail", "Authentication failed")
                    logger.warning(
                        f"Authentication failed for user {request.email}: {error_detail}"
                    )

                    return auth_pb2.AuthResponse(
                        status="error",
                        access_token="",
                        refresh_token="",
                        token_type="",
                        expires_in=0,
                        user_email="",
                        message=f"Authentication failed: {error_detail}",
                    )

        except httpx.TimeoutException:
            logger.error(f"Auth service timeout for user: {request.email}")
            return auth_pb2.AuthResponse(
                status="error",
                access_token="",
                refresh_token="",
                token_type="",
                expires_in=0,
                user_email="",
                message="Authentication service unavailable (timeout)",
            )
        except httpx.RequestError as e:
            logger.error(f"Auth service connection error for user {request.email}: {e}")
            return auth_pb2.AuthResponse(
                status="error",
                access_token="",
                refresh_token="",
                token_type="",
                expires_in=0,
                user_email="",
                message=f"Authentication service unavailable: {str(e)}",
            )
        except Exception as e:
            logger.error(f"Unexpected error during authentication for user {request.email}: {e}")
            return auth_pb2.AuthResponse(
                status="error",
                access_token="",
                refresh_token="",
                token_type="",
                expires_in=0,
                user_email="",
                message=f"Authentication failed: {str(e)}",
            )

    async def GetProjects(
        self, request: auth_pb2.GetProjectsRequest, context: grpc.aio.ServicerContext
    ) -> auth_pb2.GetProjectsResponse:
        """
        Get projects accessible by authenticated user.

        Requires valid JWT token in metadata.

        Args:
            request: GetProjectsRequest (user_token is deprecated, use metadata)
            context: gRPC context

        Returns:
            GetProjectsResponse with list of projects
        """
        # Get current user from context (set by AuthenticationInterceptor)
        current_user = get_current_user_from_context(context)

        if not current_user:
            # Try to extract and validate token manually
            metadata = dict(context.invocation_metadata())
            auth_header = metadata.get("authorization", "")

            if not auth_header:
                return auth_pb2.GetProjectsResponse(
                    status="error",
                    message="Authentication required - missing authorization header",
                    projects_count=0,
                    projects=[],
                )

            parts = auth_header.split(" ")
            if len(parts) != 2 or parts[0].lower() != "bearer":
                return auth_pb2.GetProjectsResponse(
                    status="error",
                    message="Invalid authorization header format",
                    projects_count=0,
                    projects=[],
                )

            token = parts[1]
            payload = await verify_token(token)

            if not payload:
                return auth_pb2.GetProjectsResponse(
                    status="error",
                    message="Invalid or expired token",
                    projects_count=0,
                    projects=[],
                )

            current_user = CurrentUser(
                user_id=payload["user_id"],
                email=payload["email"],
                roles=payload["roles"],
                is_superuser=payload["is_superuser"],
                companies=payload["companies"],
            )

        logger.info(f"GetProjects request for user: {current_user.email}")

        try:
            # Get database pool
            pool = await self._get_db_pool()
            project_repo = ProjectRepository(pool)

            # Fetch projects based on user access
            if current_user.is_superuser:
                # Superuser sees all projects
                projects = await project_repo.get_all()
            else:
                # Regular user sees projects from their companies
                projects = []
                for company_id in current_user.companies:
                    company_projects = await project_repo.get_by_company(company_id)
                    projects.extend(company_projects)

            # Convert to protobuf messages
            project_items = []
            for proj in projects:
                project_items.append(
                    auth_pb2.ProjectItem(
                        id=proj["id"],
                        company_id=proj["company_id"],
                        name=proj["name"],
                        description=proj.get("description", ""),
                        company_name=proj.get("company_name", ""),
                        cognee_enabled=bool(proj.get("cognee_enabled", False)),
                    )
                )

            return auth_pb2.GetProjectsResponse(
                status="success",
                message=f"Retrieved {len(project_items)} projects",
                projects_count=len(project_items),
                projects=project_items,
                user_email=current_user.email,
            )

        except Exception as e:
            logger.error(f"Error fetching projects: {e}")
            return auth_pb2.GetProjectsResponse(
                status="error",
                message=f"Failed to fetch projects: {str(e)}",
                projects_count=0,
                projects=[],
            )
