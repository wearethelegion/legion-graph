# API Utilities

Shared utility classes and functions for the KGRAG API.

## FileValidator

Validation utility for uploaded files, with specialized support for ZIP archives used in skill packages.

### Usage

#### Basic ZIP Validation

```python
from fastapi import UploadFile
from api.utils import FileValidator

validator = FileValidator()

# Validate uploaded ZIP file
is_valid, error_msg = await validator.validate_zip_archive(file)

if is_valid:
    # File is valid, proceed with processing
    pass
else:
    # Handle validation error
    raise HTTPException(status_code=400, detail=error_msg)
```

#### ZIP Extraction

```python
from api.utils import FileValidator

validator = FileValidator()

# First validate
is_valid, error_msg = await validator.validate_zip_archive(file)
if not is_valid:
    raise HTTPException(status_code=400, detail=error_msg)

# Extract contents to memory
files = await validator.extract_zip_contents(file)

# Process extracted files
for filepath, content in files.items():
    if filepath == "SKILL.md":
        # Process SKILL.md
        skill_definition = content.decode('utf-8')
    elif filepath.endswith('.py'):
        # Process Python files
        code = content.decode('utf-8')
```

#### Example: Skill Package Upload Endpoint

```python
from fastapi import APIRouter, UploadFile, File, HTTPException
from api.utils import FileValidator

router = APIRouter()

@router.post("/skills/upload")
async def upload_skill_package(
    file: UploadFile = File(...),
    current_user: CurrentUser = Depends(get_current_user)
):
    """Upload and validate skill package."""

    validator = FileValidator()

    # Validate ZIP archive
    is_valid, error_msg = await validator.validate_zip_archive(file)
    if not is_valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid skill package: {error_msg}"
        )

    # Extract contents
    files = await validator.extract_zip_contents(file)

    # Process SKILL.md
    if "SKILL.md" not in files:
        raise HTTPException(
            status_code=400,
            detail="SKILL.md not found in package"
        )

    skill_definition = files["SKILL.md"].decode('utf-8')

    # Store and process skill package...

    return {"message": "Skill package uploaded successfully"}
```

### Validation Rules

#### ZIP Archive Validation

**File Extension:**
- Must be `.zip`

**Size Limits:**
- Maximum: 100MB
- Minimum: Non-empty

**Required Files:**
- `SKILL.md` must be present at root level (case-sensitive)

**Prohibited Files:**
- `.exe` (Windows executables)
- `.dll` (Windows libraries)
- `.so` (Unix shared objects)
- `.dylib` (macOS dynamic libraries)
- `.bat` (Batch scripts)

**Allowed Content:**
- Text files (`.md`, `.py`, `.js`, `.txt`, etc.)
- Image files (`.png`, `.jpg`, `.svg`, etc.)
- Configuration files (`.json`, `.yaml`, `.toml`, etc.)
- Any other non-executable files

### Methods

#### `validate_zip_archive(file: UploadFile) -> tuple[bool, str]`

Validates a ZIP archive for skill package requirements.

**Parameters:**
- `file`: Uploaded ZIP file (FastAPI UploadFile)

**Returns:**
- `tuple[bool, str]`: `(is_valid, error_message)`
  - `is_valid`: `True` if validation passes, `False` otherwise
  - `error_message`: Empty string if valid, error description if invalid

**Validation Checks:**
1. File extension is `.zip`
2. File size is between 1 byte and 100MB
3. File is a valid ZIP archive
4. `SKILL.md` exists at root level
5. No executable files present

#### `extract_zip_contents(file: UploadFile) -> Dict[str, bytes]`

Extracts ZIP archive contents to memory (no filesystem operations).

**Parameters:**
- `file`: Uploaded ZIP file (FastAPI UploadFile)

**Returns:**
- `Dict[str, bytes]`: Dictionary mapping file paths to file contents
  - Keys: File paths within ZIP (e.g., `"SKILL.md"`, `"src/main.py"`)
  - Values: File contents as bytes

**Behavior:**
- Directory entries are skipped
- File pointer is reset to beginning after extraction
- All contents loaded into memory (no temp files)

### Design Principles

**SOLID Compliance:**
- **Single Responsibility**: Each method has one clear purpose
- **Open/Closed**: Extensible for new validation types without modifying existing code
- **Interface Segregation**: Clean, focused interface

**Security:**
- No arbitrary code execution
- Validates file types before processing
- Memory-only operations (no temp file vulnerabilities)
- Size limits prevent DoS attacks

**Performance:**
- In-memory operations for speed
- File pointer reset for reusability
- Efficient ZIP streaming

### Testing

Comprehensive test coverage in `tests/test_file_validator.py`:

```bash
# Run all FileValidator tests
pytest tests/test_file_validator.py -v

# Run specific test class
pytest tests/test_file_validator.py::TestFileValidatorZipValidation -v

# Run with coverage
pytest tests/test_file_validator.py --cov=api.utils.file_validator --cov-report=term-missing
```

**Test Coverage:**
- Valid ZIP archives
- Invalid file extensions
- Size limit enforcement
- Missing required files
- Executable file detection
- Corrupted ZIP handling
- Nested directory structures
- Binary content preservation
- Edge cases and error scenarios
