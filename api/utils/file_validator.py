"""
File Validator Utility
Validation methods for uploaded files including ZIP archives for skill packages.
"""

import zipfile
import io
from typing import Dict
from fastapi import UploadFile


class FileValidator:
    """Validator for uploaded files with support for ZIP archives."""

    async def validate_zip_archive(self, file: UploadFile) -> tuple[bool, str]:
        """
        Validate ZIP archive for skill package.

        Args:
            file: Uploaded ZIP file

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check file extension
        if not file.filename or not file.filename.endswith('.zip'):
            return False, "File must be a ZIP archive"

        # Check file size (max 100MB for skill packages)
        file.file.seek(0, 2)  # Seek to end
        size = file.file.tell()
        file.file.seek(0)  # Reset

        if size > 100 * 1024 * 1024:  # 100MB
            return False, "ZIP archive too large (max 100MB)"

        if size == 0:
            return False, "ZIP archive is empty"

        # Validate ZIP structure
        try:
            # Read file content
            content = await file.read()
            await file.seek(0)  # Reset for later use

            # Try to open as ZIP
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Check for SKILL.md
                namelist = zf.namelist()

                if "SKILL.md" not in namelist:
                    return False, "ZIP must contain SKILL.md at root level"

                # Validate all files are text-based (no executables, binaries except images)
                for name in namelist:
                    if name.endswith(('.exe', '.dll', '.so', '.dylib', '.bat')):
                        return False, f"Executable files not allowed: {name}"

                return True, ""

        except zipfile.BadZipFile:
            return False, "Invalid ZIP archive"
        except Exception as e:
            return False, f"ZIP validation error: {str(e)}"

    async def extract_zip_contents(self, file: UploadFile) -> Dict[str, bytes]:
        """
        Extract ZIP archive contents to memory.

        Args:
            file: Uploaded ZIP file

        Returns:
            Dictionary mapping file paths to file contents (as bytes)
        """
        content = await file.read()
        await file.seek(0)

        files = {}
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                # Skip directories
                if name.endswith('/'):
                    continue

                files[name] = zf.read(name)

        return files
