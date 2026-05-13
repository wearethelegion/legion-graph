"""Test project-level analysis + per-file classification.

1. Runs analyze_project() for oscar-vet/vet_backend@develop
2. Pretty-prints the full project analysis JSON
3. Runs classify_files() for the 5 demo files (language IS NOT NULL)
4. Queries and prints the 5 files with their tags
"""

import asyncio, json, logging, os, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Load .env from project root
_env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

import asyncpg  # noqa: E402
from code_preprocessor.project_classifier import (  # noqa: E402
    analyze_project,
    classify_files,
)

DB_DSN = "postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth"
REPO = "oscar-vet/vet_backend"
BRANCH = "develop"


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    assert pool is not None

    # --- Step 1: Project analysis ---
    print("\n" + "=" * 60)
    print("  PROJECT ANALYSIS")
    print("=" * 60)

    t0 = time.time()
    analysis = await analyze_project(pool, REPO, BRANCH)
    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed:.1f}s")
    print(json.dumps(analysis, indent=2))

    # --- Step 2: Reset tags on demo files so we can re-classify ---
    await pool.execute(
        """UPDATE code_processing.repository_file_versions
        SET business_tags = NULL, technical_tags = NULL,
            layer = NULL, framework_role = NULL, description = NULL
        WHERE repository = $1 AND branch = $2 AND language IS NOT NULL""",
        REPO,
        BRANCH,
    )

    # --- Step 3: Classify the 5 demo files ---
    print("\n" + "=" * 60)
    print("  FILE CLASSIFICATION")
    print("=" * 60)

    t0 = time.time()
    count = await classify_files(pool, REPO, BRANCH)
    elapsed = time.time() - t0
    print(f"\nclassify_files returned: {count} files in {elapsed:.1f}s")

    # --- Step 4: Show results ---
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)

    rows = await pool.fetch(
        """SELECT file_path, language, business_tags, technical_tags,
                  layer, framework_role, description
        FROM code_processing.repository_file_versions
        WHERE repository = $1 AND branch = $2 AND language IS NOT NULL
        ORDER BY file_path""",
        REPO,
        BRANCH,
    )
    for r in rows:
        print(f"\n  {r['file_path']}  (lang={r['language']})")
        print(f"    business_tags:  {r['business_tags']}")
        print(f"    technical_tags: {r['technical_tags']}")
        print(f"    layer:          {r['layer']}")
        print(f"    framework_role: {r['framework_role']}")
        print(f"    description:    {r['description']}")

    still_null = sum(1 for r in rows if r["business_tags"] is None)
    if still_null:
        print(f"\n⚠️  {still_null} file(s) still have NULL business_tags!")
    else:
        print(f"\n✅ All {len(rows)} files classified successfully.")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
