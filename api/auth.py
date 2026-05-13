"""
KGRAG API Authentication Middleware
Validates JWT tokens against Auth Service.
"""

from fastapi import HTTPException, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict, Any, List
import httpx
import os
from loguru import logger

security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)

# Auth service configuration
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")


class CurrentUser:
    """Current authenticated user information."""

    def __init__(
        self,
        user_id: str,
        email: str,
        roles: list[str],
        is_superuser: bool = False,
        companies: List[str] = None
    ):
        self.user_id = user_id
        self.email = email
        self.roles = roles
        self.is_superuser = is_superuser
        self.companies = companies or []

    def has_role(self, role: str) -> bool:
        """Check if user has specific role."""
        return self.is_superuser or role in self.roles

    def has_any_role(self, *roles: str) -> bool:
        """Check if user has any of the specified roles."""
        return self.is_superuser or any(role in self.roles for role in roles)

    def __repr__(self) -> str:
        return f"<User {self.email} (roles: {', '.join(self.roles)})>"


async def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify JWT token with auth service.

    Args:
        token: JWT access token

    Returns:
        User payload if valid, None if invalid
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{AUTH_SERVICE_URL}/verify",
                json={"token": token}
            )

            if response.status_code != 200:
                logger.warning("Auth service returned {}", response.status_code)
                return None

            data = response.json()

            if not data.get("valid"):
                return None

            return {
                "user_id": data.get("user_id"),
                "email": data.get("email"),
                "roles": data.get("roles", []),
                "is_superuser": data.get("is_superuser", False),
                "companies": data.get("companies", [])
            }

    except httpx.TimeoutException:
        logger.error("Auth service timeout")
        return None
    except httpx.ConnectError:
        logger.error("Cannot connect to auth service at {}", AUTH_SERVICE_URL)
        return None
    except Exception as e:
        logger.error("Token verification error: {}", e)
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> CurrentUser:
    """
    Get current authenticated user (required).

    Raises HTTPException if token is invalid or missing.
    Use this dependency for protected endpoints.

    Example:
        @app.get("/protected")
        async def protected_route(user: CurrentUser = Depends(get_current_user)):
            return {"message": f"Hello {user.email}"}
    """
    token = credentials.credentials

    payload = await verify_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"}
        )

    return CurrentUser(
        user_id=payload["user_id"],
        email=payload["email"],
        roles=payload["roles"],
        is_superuser=payload["is_superuser"],
        companies=payload["companies"]
    )


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_optional)
) -> Optional[CurrentUser]:
    """
    Get current authenticated user (optional).

    Returns None if no token provided or token is invalid.
    Use this for endpoints that have different behavior for authenticated vs anonymous users.

    Example:
        @app.get("/data")
        async def get_data(user: Optional[CurrentUser] = Depends(get_current_user_optional)):
            if user:
                return {"message": f"Personalized for {user.email}"}
            return {"message": "Public data"}
    """
    if not credentials:
        return None

    token = credentials.credentials
    payload = await verify_token(token)

    if not payload:
        return None

    return CurrentUser(
        user_id=payload["user_id"],
        email=payload["email"],
        roles=payload["roles"],
        is_superuser=payload["is_superuser"],
        companies=payload["companies"]
    )


def validate_company_access(current_user: CurrentUser, company_id: str) -> None:
    """
    Validate user has access to company.

    Args:
        current_user: The current authenticated user
        company_id: The company ID to check access for

    Raises:
        HTTPException: If user doesn't have access to the company (403)
    """
    if current_user.is_superuser:
        return  # Super admin bypasses all checks

    if company_id not in current_user.companies:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this company"
        )


def require_role(*required_roles: str):
    """
    Dependency factory for role-based access control.

    Args:
        *required_roles: One or more role names required

    Returns:
        Dependency function that checks user roles

    Example:
        @app.delete("/admin/users/{user_id}")
        async def delete_user(
            user_id: str,
            user: CurrentUser = Depends(require_role("admin"))
        ):
            # Only users with 'admin' role can access this
            pass
    """
    async def check_roles(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not user.has_any_role(*required_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required role(s): {', '.join(required_roles)}"
            )
        return user

    return check_roles


def require_superuser():
    """
    Dependency for superuser-only endpoints.

    Example:
        @app.post("/admin/reset")
        async def reset_system(user: CurrentUser = Depends(require_superuser())):
            # Only superusers can access this
            pass
    """
    async def check_superuser(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not user.is_superuser:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Superuser access required"
            )
        return user

    return check_superuser
