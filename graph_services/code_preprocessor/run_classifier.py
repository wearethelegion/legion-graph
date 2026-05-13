#!/usr/bin/env python3
"""Test the project classifier against real DB data.

Usage:
    python test_classifier.py <repository> <branch>

Example:
    python test_classifier.py oscar-vet/vet_backend develop
"""

import asyncio
import os
import sys
from pathlib import Path

import asyncpg

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from code_preprocessor.project_classifier import classify_and_store
from code_preprocessor.file_tree import build_tree, resolve_repo_path


async def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    repository = sys.argv[1]
    branch = sys.argv[2]

    # Check API key
    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY environment variable not set")
        sys.exit(1)

    # Connect to DB
    pool = await asyncpg.create_pool(
        "postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth",
        min_size=1,
        max_size=3,
    )

    try:
        # Check existing files
        count = await pool.fetchval(
            "SELECT COUNT(*) FROM code_processing.repository_file_versions "
            "WHERE repository = $1 AND branch = $2 AND language IS NOT NULL",
            repository,
            branch,
        )

        if count == 0:
            print(f"No files found for {repository}@{branch}")
            print("Run the ingestion pipeline first.")
            sys.exit(1)

        print(f"Found {count} files in database for {repository}@{branch}")

        # Build file tree
        repo_path = resolve_repo_path(repository)
        if not repo_path.exists():
            print(f"ERROR: Repository not found at {repo_path}")
            sys.exit(1)

        print(f"Building file tree from {repo_path}...")
        tree = build_tree(str(repo_path))
        print(f"  Tree size: {len(tree)} chars, {tree.count(chr(10))} lines")

        # Classify
        print(f"\nClassifying files...")
        classified = await classify_and_store(pool, repository, branch, file_tree_context=tree)

        print(f"✓ Classified {classified} files\n")

        # Show results grouped by layer
        rows = await pool.fetch(
            """
            SELECT layer, framework_role, COUNT(*) as cnt 
            FROM code_processing.repository_file_versions 
            WHERE repository = $1 AND branch = $2 AND business_tags IS NOT NULL
            GROUP BY layer, framework_role 
            ORDER BY layer, cnt DESC
            """,
            repository,
            branch,
        )

        print("Classification summary:")
        print("-" * 60)
        current_layer = None
        for r in rows:
            if r["layer"] != current_layer:
                current_layer = r["layer"]
                print(f"\n{current_layer.upper()}:")
            print(f"  {r['framework_role']:30s} {r['cnt']:4d} files")

        # Show sample files
        print("\n" + "=" * 60)
        print("Sample classified files:\n")

        samples = await pool.fetch(
            """
            SELECT file_path, business_tags, technical_tags, layer, framework_role, 
                   left(description, 70) as desc
            FROM code_processing.repository_file_versions 
            WHERE repository = $1 AND branch = $2 AND business_tags IS NOT NULL
            ORDER BY layer, file_path 
            LIMIT 10
            """,
            repository,
            branch,
        )

        for s in samples:
            print(f"{s['file_path']}")
            print(f"  Business: {', '.join(s['business_tags'])}")
            print(f"  Technical: {', '.join(s['technical_tags'])}")
            print(f"  {s['layer']} / {s['framework_role']}")
            print(f"  → {s['desc']}")
            print()

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
