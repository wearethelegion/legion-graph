# FileValidator Integration Example

This document shows how to integrate the `FileValidator` utility into a FastAPI endpoint for skill package uploads.

## Complete Example: Skill Package Upload Endpoint

```python
"""
Skill Package Upload Endpoint
Demonstrates FileValidator integration for ZIP validation and extraction.
"""

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, status
from typing import Dict
from loguru import logger

from api.utils import FileValidator
from api.auth import CurrentUser, get_current_user

router = APIRouter(prefix="/api/v1/skills", tags=["skills"])


@router.post("/upload", status_code=status.HTTP_201_CREATED)
async def upload_skill_package(
    file: UploadFile = File(..., description="Skill package ZIP file"),
    current_user: CurrentUser = Depends(get_current_user)
):
    """
    Upload and validate a skill package ZIP archive.

    **Authentication Required**: User must be authenticated.

    **Request:**
    - Content-Type: multipart/form-data
    - file: ZIP archive containing skill package

    **Validation Rules:**
    - File must be a .zip archive
    - Maximum size: 100MB
    - Must contain SKILL.md at root level
    - No executable files allowed

    **Response:** 201 Created
    ```json
    {
        "skill_id": "uuid",
        "filename": "my-skill.zip",
        "files_count": 5,
        "skill_name": "My Skill",
        "message": "Skill package uploaded successfully"
    }
    ```
    """
    validator = FileValidator()

    # Step 1: Validate ZIP archive
    logger.info(f"Validating skill package: {file.filename}")

    is_valid, error_msg = await validator.validate_zip_archive(file)
    if not is_valid:
        logger.warning(f"Invalid skill package from user {current_user.user_id}: {error_msg}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid skill package: {error_msg}"
        )

    # Step 2: Extract ZIP contents to memory
    logger.info(f"Extracting skill package: {file.filename}")

    try:
        files = await validator.extract_zip_contents(file)
    except Exception as e:
        logger.error(f"Failed to extract skill package: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to extract skill package: {str(e)}"
        )

    # Step 3: Parse SKILL.md
    if "SKILL.md" not in files:
        # This should never happen after validation, but defensive check
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SKILL.md not found in package"
        )

    try:
        skill_definition = files["SKILL.md"].decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SKILL.md must be valid UTF-8 text"
        )

    # Step 4: Extract skill metadata from SKILL.md
    # (Simplified - real implementation would use proper markdown parser)
    skill_name = extract_skill_name(skill_definition)

    # Step 5: Store skill package
    # Example: Save to database, object storage, etc.
    skill_id = await store_skill_package(
        user_id=current_user.user_id,
        filename=file.filename,
        skill_definition=skill_definition,
        files=files
    )

    logger.info(
        f"Skill package uploaded successfully: {skill_id} "
        f"(user: {current_user.user_id}, files: {len(files)})"
    )

    return {
        "skill_id": skill_id,
        "filename": file.filename,
        "files_count": len(files),
        "skill_name": skill_name,
        "message": "Skill package uploaded successfully"
    }


def extract_skill_name(skill_md: str) -> str:
    """
    Extract skill name from SKILL.md content.

    Args:
        skill_md: SKILL.md content

    Returns:
        Skill name (first H1 header or "Unnamed Skill")
    """
    for line in skill_md.split('\n'):
        line = line.strip()
        if line.startswith('# '):
            return line[2:].strip()

    return "Unnamed Skill"


async def store_skill_package(
    user_id: str,
    filename: str,
    skill_definition: str,
    files: Dict[str, bytes]
) -> str:
    """
    Store skill package in database and object storage.

    Args:
        user_id: ID of user uploading the skill
        filename: Original filename
        skill_definition: SKILL.md content
        files: Extracted files from ZIP

    Returns:
        Generated skill ID
    """
    import uuid

    # TODO: Implement actual storage logic
    # - Insert skill record into PostgreSQL
    # - Upload files to object storage (S3, MinIO, etc.)
    # - Create Neo4j nodes for skill metadata
    # - Index skill content in vector store

    skill_id = str(uuid.uuid4())

    # Example database insertion (pseudo-code):
    # await db.execute(
    #     """
    #     INSERT INTO skills (id, user_id, filename, skill_definition, files_count)
    #     VALUES ($1, $2, $3, $4, $5)
    #     """,
    #     skill_id, user_id, filename, skill_definition, len(files)
    # )

    return skill_id
```

## Error Handling Examples

### Invalid File Extension

```python
# Request: Upload skill.tar.gz
# Response: 400 Bad Request
{
    "detail": "Invalid skill package: File must be a ZIP archive"
}
```

### Missing SKILL.md

```python
# Request: Upload ZIP without SKILL.md
# Response: 400 Bad Request
{
    "detail": "Invalid skill package: ZIP must contain SKILL.md at root level"
}
```

### Executable Files Detected

```python
# Request: Upload ZIP with malware.exe
# Response: 400 Bad Request
{
    "detail": "Invalid skill package: Executable files not allowed: malware.exe"
}
```

### File Too Large

```python
# Request: Upload 150MB ZIP
# Response: 400 Bad Request
{
    "detail": "Invalid skill package: ZIP archive too large (max 100MB)"
}
```

## Testing the Endpoint

### Using cURL

```bash
# Upload valid skill package
curl -X POST http://localhost:8000/api/v1/skills/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@my-skill.zip"

# Expected response:
{
    "skill_id": "123e4567-e89b-12d3-a456-426614174000",
    "filename": "my-skill.zip",
    "files_count": 5,
    "skill_name": "My Amazing Skill",
    "message": "Skill package uploaded successfully"
}
```

### Using Python requests

```python
import requests

url = "http://localhost:8000/api/v1/skills/upload"
headers = {"Authorization": f"Bearer {token}"}

with open("my-skill.zip", "rb") as f:
    files = {"file": ("my-skill.zip", f, "application/zip")}
    response = requests.post(url, headers=headers, files=files)

print(response.json())
```

### Using httpx (async)

```python
import httpx

async def upload_skill():
    url = "http://localhost:8000/api/v1/skills/upload"
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient() as client:
        with open("my-skill.zip", "rb") as f:
            files = {"file": ("my-skill.zip", f, "application/zip")}
            response = await client.post(url, headers=headers, files=files)

    return response.json()
```

## Security Considerations

### Implemented by FileValidator

1. **File Type Validation**: Only `.zip` files accepted
2. **Size Limits**: Maximum 100MB to prevent DoS
3. **Executable Detection**: Blocks `.exe`, `.dll`, `.so`, `.dylib`, `.bat`
4. **Memory-Only Operations**: No temp file vulnerabilities
5. **SKILL.md Required**: Ensures package structure validity

### Additional Security Measures

For production deployment, consider:

```python
# 1. Virus scanning (integrate ClamAV or similar)
async def scan_for_viruses(file: UploadFile) -> bool:
    """Scan uploaded file for viruses."""
    # Integrate with antivirus service
    pass

# 2. Content inspection
async def validate_skill_definition(skill_md: str) -> bool:
    """Validate SKILL.md content against schema."""
    # Parse and validate SKILL.md structure
    pass

# 3. Rate limiting per user
@router.post("/upload")
@limiter.limit("10/hour")  # Max 10 uploads per hour per user
async def upload_skill_package(...):
    pass

# 4. Quarantine suspicious packages
async def quarantine_if_suspicious(skill_id: str, files: Dict[str, bytes]):
    """Flag packages for manual review if suspicious patterns detected."""
    pass
```

## Performance Optimization

### For Large Files

```python
# Use streaming for large file processing
async def process_large_skill_package(file: UploadFile):
    """Process large skill packages in chunks."""

    validator = FileValidator()

    # Validate first
    is_valid, error_msg = await validator.validate_zip_archive(file)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_msg)

    # Extract and process in chunks
    files = await validator.extract_zip_contents(file)

    # Process files asynchronously
    tasks = []
    for filepath, content in files.items():
        task = process_file_async(filepath, content)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    return results
```

### Caching Validation Results

```python
from functools import lru_cache
import hashlib

# Cache validation results by file hash
validation_cache = {}

async def validate_with_cache(file: UploadFile) -> tuple[bool, str]:
    """Validate with caching for duplicate uploads."""

    # Calculate file hash
    content = await file.read()
    await file.seek(0)
    file_hash = hashlib.sha256(content).hexdigest()

    # Check cache
    if file_hash in validation_cache:
        return validation_cache[file_hash]

    # Validate
    validator = FileValidator()
    result = await validator.validate_zip_archive(file)

    # Cache result
    validation_cache[file_hash] = result
    return result
```

## Monitoring and Logging

```python
from loguru import logger
import time

@router.post("/upload")
async def upload_skill_package(
    file: UploadFile = File(...),
    current_user: CurrentUser = Depends(get_current_user)
):
    start_time = time.time()

    try:
        # Log upload attempt
        logger.info(
            f"Skill upload started: user={current_user.user_id}, "
            f"file={file.filename}, size={file.file.tell()}"
        )

        # Validate and process...
        validator = FileValidator()
        is_valid, error_msg = await validator.validate_zip_archive(file)

        if not is_valid:
            logger.warning(
                f"Validation failed: user={current_user.user_id}, "
                f"file={file.filename}, error={error_msg}"
            )
            raise HTTPException(status_code=400, detail=error_msg)

        files = await validator.extract_zip_contents(file)

        # Log success
        elapsed = time.time() - start_time
        logger.info(
            f"Skill upload completed: user={current_user.user_id}, "
            f"file={file.filename}, files={len(files)}, "
            f"elapsed={elapsed:.2f}s"
        )

        return {"message": "Success"}

    except HTTPException:
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f"Skill upload failed: user={current_user.user_id}, "
            f"file={file.filename}, error={str(e)}, elapsed={elapsed:.2f}s"
        )
        raise HTTPException(status_code=500, detail=str(e))
```
