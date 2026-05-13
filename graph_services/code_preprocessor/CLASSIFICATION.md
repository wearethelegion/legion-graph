# Project File Classification

Enriches ingested code files with semantic metadata via LLM classification.

## Overview

The project classifier sends file paths to an LLM (Gemini Flash Lite) and receives back structured metadata for each file:

- **business_tags**: Domain concepts (e.g. `["users", "appointments", "authentication"]`)
- **technical_tags**: Technical patterns (e.g. `["controller", "service", "orm_model"]`)
- **layer**: Architectural layer (`controller`, `model`, `service`, `repository`, `test`, etc.)
- **framework_role**: Framework-specific role (`rails_controller`, `react_component`, `fastapi_router`)
- **description**: One-sentence summary of the file's purpose

## Database Schema

Classifications are stored in `code_processing.repository_file_versions`:

```sql
ALTER TABLE code_processing.repository_file_versions
ADD COLUMN business_tags TEXT[],
ADD COLUMN technical_tags TEXT[],
ADD COLUMN layer TEXT,
ADD COLUMN framework_role TEXT,
ADD COLUMN description TEXT;
```

Migration: `066_add_file_classification_columns.sql`

## Pipeline Integration

Runs automatically during Kafka ingestion, after file processing and before embedding:

```
1. Process files → enrich_and_store_file()
2. Store project tree → store_project_tree()
3. Classify files → classify_and_store()  ← THIS STEP
4. Embed chunks → batch_embed_chunks()
5. Mark ingestion complete
```

## Usage

### Automatic (via Kafka consumer)

Classification runs automatically when:
- GEMINI_API_KEY is set
- Files exist in DB with `language IS NOT NULL`
- Files have not been classified yet (`business_tags IS NULL`)

### Manual Testing

```bash
# Set API key
export GEMINI_API_KEY=your_key_here

# Test classification
python code_inteligence_preprocessor/test_classifier.py oscar-vet/vet_backend develop
```

### Programmatic

```python
import asyncpg
from code_inteligence_preprocessor.project_classifier import classify_and_store
from code_inteligence_preprocessor.file_tree import build_tree

pool = await asyncpg.create_pool("postgresql://...")
tree = build_tree("/path/to/repo")

classified_count = await classify_and_store(
    pool,
    repository="org/repo",
    branch="main",
    file_tree_context=tree
)
```

## Performance

- **Batch size**: 500 files per LLM call
- **Parallelism**: Multiple batches processed concurrently
- **Model**: `gemini/gemini-2.5-flash-lite` (fast, cheap)
- **Context**: First 3000 chars of file tree included for better accuracy
- **Idempotency**: Only classifies files where `business_tags IS NULL`

## Example Output

```
app/controllers/appointments_controller.rb
  Business: appointments, scheduling
  Technical: controller, http_handler
  controller / rails_controller
  → Handles HTTP requests for appointment CRUD operations

app/models/user.rb
  Business: users, authentication
  Technical: orm_model, validation
  model / rails_model
  → Defines user entity with authentication and profile data

spec/models/user_spec.rb
  Business: users
  Technical: test, unit_test
  test / rspec_model_test
  → Unit tests for User model validations and behavior
```

## Error Handling

- Missing API key → skip classification, log warning
- LLM timeout → retry batch, log error
- Invalid response → skip affected files, log warning
- DB constraint violation → skip file, log warning

All errors are gracefully degraded — ingestion continues even if classification fails.

## Future Enhancements

- Cache classifications by file content hash
- Use file content snippets for deeper classification
- Add confidence scores to tags
- Support custom classification rules per project
- Classify architectural patterns (layered, hexagonal, microservices)
