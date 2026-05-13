"""Test script for classify_and_store — runs against local Postgres + Gemini."""

import asyncio
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import asyncpg  # noqa: E402

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

from code_preprocessor.project_classifier import classify_and_store  # noqa: E402

DB_DSN = "postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth"
REPO = "oscar-vet/vet_backend"
BRANCH = "develop"


async def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=3)
    assert pool is not None

    # --- Pre-run: show current state ---
    print("\n=== PRE-RUN STATE ===")
    rows = await pool.fetch(
        """
        SELECT file_path, language, business_tags, technical_tags, layer, framework_role
        FROM code_processing.repository_file_versions
        WHERE repository = $1 AND branch = $2
          AND language IS NOT NULL AND business_tags IS NULL
        ORDER BY file_path
        """,
        REPO,
        BRANCH,
    )
    print(f"Files needing classification: {len(rows)}")
    for r in rows:
        print(f"  {r['file_path']}  (lang={r['language']})")

    if not rows:
        print("Nothing to classify — exiting.")
        await pool.close()
        return

    # --- Run classify_and_store ---
    print("\n=== RUNNING classify_and_store ===")
    count = await classify_and_store(pool, REPO, BRANCH)
    print(f"\nclassify_and_store returned: {count}")

    # --- Post-run: show results ---
    print("\n=== POST-RUN STATE ===")
    results = await pool.fetch(
        """
        SELECT file_path, business_tags, technical_tags, layer, framework_role, description
        FROM code_processing.repository_file_versions
        WHERE repository = $1 AND branch = $2
          AND file_path = ANY($3::text[])
        ORDER BY file_path
        """,
        REPO,
        BRANCH,
        [r["file_path"] for r in rows],
    )

    for r in results:
        print(f"\n  {r['file_path']}")
        print(f"    business_tags:  {r['business_tags']}")
        print(f"    technical_tags: {r['technical_tags']}")
        print(f"    layer:          {r['layer']}")
        print(f"    framework_role: {r['framework_role']}")
        print(f"    description:    {r['description']}")

    still_null = sum(1 for r in results if r["business_tags"] is None)
    if still_null:
        print(f"\n⚠️  {still_null} file(s) still have NULL business_tags!")
    else:
        print(f"\n✅ All {len(results)} files classified successfully.")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
