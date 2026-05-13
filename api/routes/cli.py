"""
CLI Distribution Routes
Endpoints for CLI package version metadata, download, and superuser upload.
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional

import asyncpg
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse
from loguru import logger

from api.auth import (
    CurrentUser,
    get_current_user,
    get_current_user_optional,
    require_superuser,
    verify_token,
)
from api.database import get_db_pool

# ---------------------------------------------------------------------------
# Storage configuration — mirrors DOCUMENT_STORAGE_PATH pattern
# ---------------------------------------------------------------------------
CLI_STORAGE_PATH = Path(os.getenv("CLI_STORAGE_PATH", "data/cli"))

router = APIRouter(prefix="/api/v1/cli", tags=["cli"])


# ---------------------------------------------------------------------------
# GET /cli/version — authenticated
# ---------------------------------------------------------------------------


@router.get("/version", summary="Get current CLI version metadata")
async def get_cli_version(
    current_user: CurrentUser = Depends(get_current_user),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> Dict[str, Any]:
    """
    Returns the current active CLI version metadata.

    Returns ``{"version": null, "available": false}`` if no version has been uploaded yet.

    **Authentication Required**
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT version, filename, file_size, uploaded_at
            FROM cli_versions
            WHERE is_active = TRUE
            ORDER BY uploaded_at DESC
            LIMIT 1
            """
        )

    if not row:
        return {"available": False, "version": None}

    return {
        "available": True,
        "version": row["version"],
        "filename": row["filename"],
        "file_size": row["file_size"],
        "uploaded_at": row["uploaded_at"].isoformat(),
    }


# ---------------------------------------------------------------------------
# GET /cli/download — authenticated
# ---------------------------------------------------------------------------


@router.get("/download", summary="Download the current CLI package")
async def download_cli(
    token: Optional[str] = Query(None, description="JWT token (for browser anchor downloads)"),
    pool: asyncpg.Pool = Depends(get_db_pool),
    current_user: Optional[CurrentUser] = Depends(get_current_user_optional),
):
    """
    Streams the current active CLI ``.pkg`` file as a file download.

    Accepts authentication via:
    - ``Authorization: Bearer <token>`` header (standard)
    - ``?token=<token>`` query parameter (for browser anchor-based downloads)

    Returns 404 if no version has been uploaded yet.

    **Authentication Required**
    """
    # If header auth failed, fall back to query param token
    token_user_id: Optional[str] = None
    if current_user is None:
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )
        payload = await verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token_user_id = payload.get("sub")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT version, filename, file_path
            FROM cli_versions
            WHERE is_active = TRUE
            ORDER BY uploaded_at DESC
            LIMIT 1
            """
        )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No CLI version available for download."
        )

    file_path = Path(row["file_path"])
    if not file_path.exists():
        logger.error("CLI file missing from disk: {}", file_path)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="CLI file not found on server."
        )

    download_filename = f"kgrag-{row['version']}.pkg"
    user_id_for_log = current_user.user_id if current_user is not None else token_user_id
    logger.info(f"User {user_id_for_log} downloading CLI v{row['version']} ({download_filename})")

    return FileResponse(
        path=file_path,
        filename=download_filename,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{download_filename}"'},
    )


# ---------------------------------------------------------------------------
# POST /cli/upload — superuser only
# ---------------------------------------------------------------------------


@router.post(
    "/upload",
    status_code=status.HTTP_201_CREATED,
    summary="Upload a new CLI package version (superuser only)",
)
async def upload_cli(
    file: UploadFile = File(..., description=".pkg file to upload"),
    version: str = Form(..., description="Version string e.g. '1.2.3'"),
    current_user: CurrentUser = Depends(require_superuser()),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> Dict[str, Any]:
    """
    Upload a new CLI ``.pkg`` package.

    - Deactivates all previous versions.
    - Stores the file under ``data/cli/`` (configurable via ``CLI_STORAGE_PATH``).
    - Persists version metadata to the ``cli_versions`` table.

    **Authentication Required: Superuser Only**
    """
    # Validate file extension
    if not (file.filename or "").lower().endswith(".pkg"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Only .pkg files are accepted."
        )

    # Validate version string (semver-ish: major.minor.patch prefix)
    if not re.match(r"^\d+\.\d+\.\d+", version):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Version must follow semver format (e.g. '1.2.3').",
        )

    # Prepare storage directory — mirrors data/documents/{company_id}/ pattern
    CLI_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    secure_filename = f"kgrag-{version}.pkg"
    file_path = CLI_STORAGE_PATH / secure_filename

    # Write file to disk in 8 KB chunks (same pattern as documents.py)
    file_size = 0
    try:
        with open(file_path, "wb") as f:
            while chunk := await file.read(8192):
                f.write(chunk)
                file_size += len(chunk)
        logger.info(f"Saved CLI package: {file_path} ({file_size} bytes)")
    except Exception as e:
        logger.error("Failed to save CLI file: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save CLI file: {str(e)}",
        )

    # Persist to database
    try:
        async with pool.acquire() as conn:
            # Deactivate all previous versions
            await conn.execute("UPDATE cli_versions SET is_active = FALSE")

            # Insert new version record
            row = await conn.fetchrow(
                """
                INSERT INTO cli_versions
                    (version, filename, file_path, file_size, uploaded_by, is_active)
                VALUES ($1, $2, $3, $4, $5, TRUE)
                RETURNING id::text, version, filename, file_size, uploaded_at
                """,
                version,
                secure_filename,
                str(file_path),
                file_size,
                current_user.user_id,
            )
    except Exception as e:
        logger.error("Failed to persist CLI version metadata: {}", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save version metadata: {str(e)}",
        )

    logger.info(
        f"Superuser {current_user.user_id} uploaded CLI v{version} "
        f"({secure_filename}, {file_size} bytes)"
    )

    return {
        "id": row["id"],
        "version": row["version"],
        "filename": row["filename"],
        "file_size": row["file_size"],
        "uploaded_at": row["uploaded_at"].isoformat(),
    }
