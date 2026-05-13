"""Integration tests for the enrichment pipeline.

Tests the full enrichment pipeline: file tree → skeleton extraction → chunking → Postgres storage.
Uses REAL cloned repo and REAL Postgres database.
"""

import asyncio
import os
import uuid
from pathlib import Path

import asyncpg
import pytest

from code_preprocessor.file_tree import build_tree, get_file_list
from code_preprocessor.skeleton_extractor import extract_skeleton, EXTENSION_TO_LANGUAGE
from code_preprocessor.chunker import chunk_file
from code_preprocessor.enrichment import (
    enrich_and_store_file,
    store_project_tree,
    simple_chunk,
)

# Test configuration
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth"
)
REPO_ROOT = "rag_storage/repos/oscar-vet__vet_backend"


@pytest.fixture
async def db_pool():
    """Create database connection pool for tests."""
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    yield pool
    await pool.close()


@pytest.fixture
def test_ids():
    """Generate unique IDs for test data."""
    return {
        "ingestion_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "company_id": str(uuid.uuid4()),
    }


@pytest.fixture
def repo_path():
    """Get repo path and skip if it doesn't exist."""
    path = Path(REPO_ROOT)
    if not path.exists():
        pytest.skip(f"Test repository not found at {REPO_ROOT}")
    return path


# ============================================================================
# Test 1: File Tree Generation
# ============================================================================


def test_file_tree_builds_correctly(repo_path):
    """Test that build_tree() produces a valid tree for the cloned repo."""
    tree = build_tree(str(repo_path))

    # Assert tree is non-empty
    assert tree, "Tree should be non-empty"
    assert len(tree) > 100, "Tree should contain substantial content"

    # Assert it contains known Rails paths (tree uses indentation, so just check for the names)
    assert "controllers/" in tree or "controllers" in tree, "Should contain controllers directory"
    assert "models/" in tree or "models" in tree, "Should contain models directory"
    assert "Gemfile" in tree, "Should contain Gemfile"

    # Assert it does NOT contain .git
    assert ".git/" not in tree and ".git" not in tree.split("\n")[0], (
        "Should not contain .git directory"
    )

    # Assert basic structure
    lines = tree.split("\n")
    assert len(lines) > 10, "Tree should have multiple lines"

    print(f"\n✓ File tree generated successfully ({len(lines)} lines)")
    print(f"First 10 lines:\n{chr(10).join(lines[:10])}")


# ============================================================================
# Test 2: Skeleton Extraction (Ruby)
# ============================================================================


def test_skeleton_extraction_ruby(repo_path):
    """Test skeleton extraction on a Ruby controller file."""
    # Find a controller file
    controller_path = repo_path / "app/controllers/api/v1/appointments_controller.rb"
    if not controller_path.exists():
        # Try another controller
        controller_path = repo_path / "app/controllers/application_controller.rb"

    if not controller_path.exists():
        pytest.skip("No Ruby controller file found")

    content = controller_path.read_text()
    skeleton = extract_skeleton(str(controller_path), content)

    # Assert skeleton was extracted
    assert skeleton is not None, "Skeleton should be extracted"
    assert skeleton.get("language") == "ruby", "Language should be ruby"

    declarations = skeleton.get("declarations", [])
    assert len(declarations) > 0, "Should have at least one declaration"

    # Declarations are strings like "class ApplicationController" or "def method_name"
    # Check for class or method keywords
    declaration_texts = [str(d) for d in declarations]
    has_class = any("class " in d for d in declaration_texts)
    has_method = any("def " in d for d in declaration_texts)
    assert has_class or has_method, "Should contain class or method declarations"

    print(f"\n✓ Extracted {len(declarations)} declarations from {controller_path.name}")
    print(f"Sample declarations: {declaration_texts[:5]}")


# ============================================================================
# Test 3: Code File Chunking (Ruby)
# ============================================================================


def test_chunking_code_file(repo_path):
    """Test chunking of a Ruby controller file."""
    controller_path = repo_path / "app/controllers/api/v1/appointments_controller.rb"
    if not controller_path.exists():
        controller_path = repo_path / "app/controllers/application_controller.rb"

    if not controller_path.exists():
        pytest.skip("No Ruby controller file found")

    content = controller_path.read_text()
    chunks = chunk_file(str(controller_path), content)

    # Assert chunks were created
    assert len(chunks) > 0, "Should produce at least one chunk"

    # Assert all chunks are non-empty and have line numbers
    for idx, (chunk, start_line, end_line) in enumerate(chunks):
        assert chunk, f"Chunk {idx} should be non-empty"
        assert len(chunk.strip()) > 0, f"Chunk {idx} should have content"
        assert start_line >= 1, f"Chunk {idx} start_line should be ≥ 1"
        assert end_line >= start_line, f"Chunk {idx} end_line should be ≥ start_line"

    # Assert reasonable chunk sizes (with tolerance for overage)
    for idx, (chunk, start_line, end_line) in enumerate(chunks):
        assert len(chunk) <= 1500, f"Chunk {idx} should not exceed 1500 chars (got {len(chunk)})"

    # Assert no significant data loss
    joined = "".join(c for c, _, _ in chunks)
    original_len = len(content)
    joined_len = len(joined)
    loss_pct = abs(original_len - joined_len) / original_len * 100
    assert loss_pct < 5, f"Data loss should be < 5% (got {loss_pct:.1f}%)"

    print(f"\n✓ Chunked {controller_path.name} into {len(chunks)} chunks")
    print(f"Chunk sizes: {[len(c) for c, _, _ in chunks]}")


# ============================================================================
# Test 4: YAML File Chunking
# ============================================================================


def test_chunking_yaml_file(repo_path):
    """Test chunking of a YAML configuration file."""
    yaml_path = repo_path / "config/sidekiq.yml"
    if not yaml_path.exists():
        # Try another YAML file
        yaml_files = list(repo_path.glob("config/*.yml"))
        if not yaml_files:
            pytest.skip("No YAML file found")
        yaml_path = yaml_files[0]

    content = yaml_path.read_text()
    chunks = chunk_file(str(yaml_path), content)

    # Assert chunks were created
    assert len(chunks) > 0, "Should produce at least one chunk"

    # Assert reasonable chunk sizes
    for idx, (chunk, start_line, end_line) in enumerate(chunks):
        assert len(chunk) <= 1500, f"Chunk {idx} should not exceed 1500 chars (got {len(chunk)})"

    # Assert no significant data loss
    joined = "".join(c for c, _, _ in chunks)
    original_len = len(content)
    joined_len = len(joined)
    loss_pct = abs(original_len - joined_len) / original_len * 100
    assert loss_pct < 5, f"Data loss should be < 5% (got {loss_pct:.1f}%)"

    print(f"\n✓ Chunked {yaml_path.name} into {len(chunks)} chunks")


# ============================================================================
# Test 5: Enrich and Store to Postgres (Integration)
# ============================================================================


@pytest.mark.asyncio
async def test_enrich_and_store_to_postgres(db_pool, test_ids, repo_path):
    """Test full enrichment and storage pipeline with real Postgres."""
    ingestion_id = test_ids["ingestion_id"]
    project_id = test_ids["project_id"]
    company_id = test_ids["company_id"]
    file_version_id = None  # Initialize for cleanup

    try:
        # 1. Create test ingestion record
        await db_pool.execute(
            """
            INSERT INTO code_processing.ingestion_batches
                (ingestion_id, project_id, company_id, repository, branch, status)
            VALUES ($1, $2, $3, $4, $5, 'running')
            """,
            ingestion_id,
            project_id,
            company_id,
            "oscar-vet/vet_backend",
            "main",
        )

        # 2. Pick a Ruby file
        ruby_file = repo_path / "app/controllers/application_controller.rb"
        if not ruby_file.exists():
            pytest.skip("Test Ruby file not found")

        content = ruby_file.read_text()
        document_id = f"oscar-vet__vet_backend/{ruby_file.relative_to(repo_path)}"
        file_version_id = str(uuid.uuid4())

        # 3. Create file version record
        await db_pool.execute(
            """
            INSERT INTO code_processing.repository_file_versions
                (id, document_id, repository, branch, file_path, content_hash, 
                 version, change_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            file_version_id,
            document_id,
            "oscar-vet/vet_backend",
            "main",
            str(ruby_file.relative_to(repo_path)),
            "test_hash",
            1,
            "added",
        )

        # 4. Call enrich_and_store_file
        chunk_count = await enrich_and_store_file(
            db_pool,
            document_id,
            str(ruby_file),
            content,
            ingestion_id,
            project_id,
            company_id,
        )

        assert chunk_count > 0, "Should create at least one chunk"

        # 5. Query repository_file_versions - verify skeleton
        version_row = await db_pool.fetchrow(
            "SELECT language, file_skeleton FROM code_processing.repository_file_versions WHERE id = $1",
            file_version_id,
        )

        assert version_row is not None, "File version should exist"
        assert version_row["language"] == "ruby", "Language should be set to ruby"
        assert version_row["file_skeleton"] is not None, "Skeleton should be populated"

        # 6. Query file_chunks - verify chunks exist
        chunk_rows = await db_pool.fetch(
            """
            SELECT chunk_text, chunk_index, total_chunks, status
              FROM code_processing.file_chunks
             WHERE file_version_id = $1
             ORDER BY chunk_index
            """,
            file_version_id,
        )

        assert len(chunk_rows) == chunk_count, (
            f"Should have {chunk_count} chunks (got {len(chunk_rows)})"
        )

        # 7. Verify chunk properties
        for row in chunk_rows:
            assert row["chunk_text"], "Chunk text should be non-empty"
            assert row["status"] == "pending", "Chunks should be pending"
            assert row["total_chunks"] == chunk_count, "total_chunks should match"

        # 8. Verify chunk indices are sequential
        indices = [row["chunk_index"] for row in chunk_rows]
        assert indices == list(range(chunk_count)), "Chunk indices should be sequential"

        print(f"\n✓ Successfully enriched and stored {ruby_file.name}")
        print(f"  Language: {version_row['language']}")
        print(f"  Chunks: {chunk_count}")
        print(
            f"  Skeleton declarations: {len(version_row['file_skeleton']) if version_row['file_skeleton'] else 0}"
        )

    finally:
        # Cleanup
        await db_pool.execute(
            "DELETE FROM code_processing.file_chunks WHERE ingestion_id = $1",
            ingestion_id,
        )
        if file_version_id:
            await db_pool.execute(
                "DELETE FROM code_processing.repository_file_versions WHERE id = $1",
                file_version_id,
            )
        await db_pool.execute(
            "DELETE FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )


# ============================================================================
# Test 6: Store Project Tree to Postgres
# ============================================================================


@pytest.mark.asyncio
async def test_store_project_tree_to_postgres(db_pool, test_ids, repo_path):
    """Test storing project tree to ingestion_batches table."""
    ingestion_id = test_ids["ingestion_id"]
    project_id = test_ids["project_id"]
    company_id = test_ids["company_id"]

    try:
        # 1. Create ingestion record
        await db_pool.execute(
            """
            INSERT INTO code_processing.ingestion_batches
                (ingestion_id, project_id, company_id, repository, branch, status)
            VALUES ($1, $2, $3, $4, $5, 'running')
            """,
            ingestion_id,
            project_id,
            company_id,
            "oscar-vet/vet_backend",
            "main",
        )

        # 2. Build tree
        tree_string = build_tree(str(repo_path))
        assert tree_string, "Tree should be non-empty"

        # 3. Store tree
        await store_project_tree(db_pool, ingestion_id, tree_string)

        # 4. Query and verify
        row = await db_pool.fetchrow(
            "SELECT project_tree FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )

        assert row is not None, "Ingestion record should exist"
        assert row["project_tree"] == tree_string, "Stored tree should match original"

        print(f"\n✓ Successfully stored project tree ({len(tree_string)} chars)")

    finally:
        # Cleanup
        await db_pool.execute(
            "DELETE FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )


# ============================================================================
# Test 7: Full Pipeline - Multiple Files (Main Integration Test)
# ============================================================================


@pytest.mark.asyncio
async def test_full_pipeline_multiple_files(db_pool, test_ids, repo_path):
    """Test the full enrichment pipeline with multiple files of different types."""
    ingestion_id = test_ids["ingestion_id"]
    project_id = test_ids["project_id"]
    company_id = test_ids["company_id"]
    file_version_ids = []  # Initialize for cleanup

    try:
        # 1. Create ingestion record
        await db_pool.execute(
            """
            INSERT INTO code_processing.ingestion_batches
                (ingestion_id, project_id, company_id, repository, branch, status)
            VALUES ($1, $2, $3, $4, $5, 'running')
            """,
            ingestion_id,
            project_id,
            company_id,
            "oscar-vet/vet_backend",
            "main",
        )

        # 2. Select test files (mix of types)
        test_files = []

        # Ruby controller
        controller = repo_path / "app/controllers/application_controller.rb"
        if controller.exists():
            test_files.append(controller)

        # Ruby model
        model = repo_path / "app/models/all_clients.rb"
        if model.exists():
            test_files.append(model)

        # Another controller
        appointments = repo_path / "app/controllers/api/v1/appointments_controller.rb"
        if appointments.exists():
            test_files.append(appointments)

        # YAML config
        yaml_file = repo_path / "config/sidekiq.yml"
        if yaml_file.exists():
            test_files.append(yaml_file)

        # Gemfile
        gemfile = repo_path / "Gemfile"
        if gemfile.exists():
            test_files.append(gemfile)

        # Skip if we don't have enough files
        if len(test_files) < 3:
            pytest.skip(f"Need at least 3 test files (found {len(test_files)})")

        # Limit to 5 files
        test_files = test_files[:5]
        file_version_ids = []

        # 3. Enrich and store each file
        for file_path in test_files:
            content = file_path.read_text()
            document_id = f"oscar-vet__vet_backend/{file_path.relative_to(repo_path)}"
            file_version_id = str(uuid.uuid4())
            file_version_ids.append(file_version_id)

            # Create file version record
            await db_pool.execute(
                """
                INSERT INTO code_processing.repository_file_versions
                    (id, document_id, repository, branch, file_path, content_hash,
                     version, change_type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                file_version_id,
                document_id,
                "oscar-vet/vet_backend",
                "main",
                str(file_path.relative_to(repo_path)),
                f"hash_{file_version_id}",
                1,
                "added",
            )

            # Enrich and store
            chunk_count = await enrich_and_store_file(
                db_pool,
                document_id,
                str(file_path),
                content,
                ingestion_id,
                project_id,
                company_id,
            )

            print(f"  Processed {file_path.name}: {chunk_count} chunks")

        # 4. Store project tree
        tree_string = build_tree(str(repo_path))
        await store_project_tree(db_pool, ingestion_id, tree_string)

        # 5. Verify all file versions have language and skeleton
        version_rows = await db_pool.fetch(
            """
            SELECT id, language, file_skeleton, file_path
              FROM code_processing.repository_file_versions
             WHERE id = ANY($1::uuid[])
            """,
            file_version_ids,
        )

        assert len(version_rows) == len(test_files), f"Should have {len(test_files)} file versions"

        for row in version_rows:
            file_path = row["file_path"]
            # Ruby files should have language and skeleton
            if file_path.endswith(".rb"):
                assert row["language"] == "ruby", f"{file_path} should have language=ruby"
                # Note: skeleton might be empty for very simple files, so just check it exists
                assert row["file_skeleton"] is not None, f"{file_path} should have skeleton"

        # 6. Verify all files have chunks
        chunk_counts = await db_pool.fetch(
            """
            SELECT file_version_id, COUNT(*) as chunk_count
              FROM code_processing.file_chunks
             WHERE file_version_id = ANY($1::uuid[])
             GROUP BY file_version_id
            """,
            file_version_ids,
        )

        assert len(chunk_counts) == len(test_files), "All files should have chunks"

        total_chunks = sum(row["chunk_count"] for row in chunk_counts)
        assert total_chunks >= len(test_files), "Should have at least one chunk per file"
        assert total_chunks <= len(test_files) * 50, (
            "Should have reasonable chunk count (not too many)"
        )

        # 7. Verify ingestion has project tree
        ingestion_row = await db_pool.fetchrow(
            "SELECT project_tree FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )

        assert ingestion_row is not None, "Ingestion should exist"
        assert ingestion_row["project_tree"] is not None, "Project tree should be stored"
        assert len(ingestion_row["project_tree"]) > 100, "Project tree should have content"

        print(f"\n✓ Successfully processed {len(test_files)} files")
        print(f"  Total chunks: {total_chunks}")
        print(f"  Project tree: {len(ingestion_row['project_tree'])} chars")

    finally:
        # Cleanup
        await db_pool.execute(
            "DELETE FROM code_processing.file_chunks WHERE ingestion_id = $1",
            ingestion_id,
        )
        if file_version_ids:
            await db_pool.execute(
                "DELETE FROM code_processing.repository_file_versions WHERE id = ANY($1::uuid[])",
                file_version_ids,
            )
        await db_pool.execute(
            "DELETE FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )
