"""Test BFS directory classification.

Clears existing classifications for oscar-vet/vet_backend, runs classify_directories()
(which builds the tree from DB and classifies level-by-level), then prints stats,
coverage, and sample JOIN results.
"""

import asyncio
import logging
import os
import sys
import time

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
from code_preprocessor.project_classifier import classify_directories  # noqa: E402

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

    # --- Run BFS directory classification ---
    print("\n" + "=" * 60)
    print("  BFS DIRECTORY CLASSIFICATION")
    print("=" * 60)

    t0 = time.time()
    count = await classify_directories(pool, REPO, BRANCH)
    elapsed = time.time() - t0
    print(f"\nclassify_directories returned: {count} dirs in {elapsed:.1f}s")

    # --- Stats: classifications per level ---
    print("\n=== CLASSIFICATIONS BY DEPTH ===")
    depth_rows = await pool.fetch(
        """
        SELECT
          array_length(string_to_array(dir_path, '/'), 1) AS depth,
          COUNT(*) AS cnt
        FROM code_processing.directory_classifications
        WHERE repository = $1 AND branch = $2
        GROUP BY 1 ORDER BY 1
        """,
        REPO,
        BRANCH,
    )
    for r in depth_rows:
        print(f"  Depth {r['depth']}: {r['cnt']} dirs")
    print(f"  Total: {sum(r['cnt'] for r in depth_rows)} dirs")

    # --- Show all classified directories ---
    print("\n=== ALL DIRECTORY CLASSIFICATIONS ===")
    rows = await pool.fetch(
        """
        SELECT dir_path, business_tags, technical_tags, layer, description
        FROM code_processing.directory_classifications
        WHERE repository = $1 AND branch = $2
        ORDER BY dir_path
        """,
        REPO,
        BRANCH,
    )
    for r in rows:
        print(f"\n  {r['dir_path']}/")
        print(f"    business:  {r['business_tags']}")
        print(f"    technical: {r['technical_tags']}")
        print(f"    layer:     {r['layer']}")
        print(f"    desc:      {r['description']}")

    print(f"\nTotal: {len(rows)} directories classified")

    # --- Coverage: files matched to classified directories ---
    print("\n=== COVERAGE ===")
    coverage = await pool.fetchrow(
        """
        SELECT
          COUNT(*) AS total,
          COUNT(d.dir_path) AS matched
        FROM code_processing.repository_file_versions f
        LEFT JOIN code_processing.directory_classifications d
          ON f.repository = d.repository AND f.branch = d.branch
          AND d.dir_path = regexp_replace(f.file_path, '/[^/]+$', '')
        WHERE f.repository = $1 AND f.branch = $2
        """,
        REPO,
        BRANCH,
    )
    total, matched = coverage["total"], coverage["matched"]
    pct = (matched / total * 100) if total else 0
    print(f"  Total files:  {total}")
    print(f"  Matched:      {matched}")
    print(f"  Coverage:     {pct:.1f}%")
    target = "PASS" if pct >= 80 else "FAIL"
    print(f"  Target ≥80%:  {target}")

    # --- Sample JOIN results (10 files with inherited tags) ---
    print("\n=== SAMPLE: Files with Inherited Tags (10) ===")
    join_rows = await pool.fetch(
        """
        SELECT f.file_path, d.business_tags, d.technical_tags, d.layer
        FROM code_processing.repository_file_versions f
        LEFT JOIN code_processing.directory_classifications d
          ON f.repository = d.repository AND f.branch = d.branch
          AND d.dir_path = regexp_replace(f.file_path, '/[^/]+$', '')
        WHERE f.repository = $1 AND f.branch = $2
          AND d.dir_path IS NOT NULL
        ORDER BY f.file_path
        LIMIT 10
        """,
        REPO,
        BRANCH,
    )
    for r in join_rows:
        print(f"  {r['file_path']}")
        print(
            f"    → business={r['business_tags']}  technical={r['technical_tags']}  layer={r['layer']}"
        )

    # --- Unmatched files sample ---
    print("\n=== UNMATCHED FILES (sample 10) ===")
    unmatched = await pool.fetch(
        """
        SELECT f.file_path, regexp_replace(f.file_path, '/[^/]+$', '') AS dir
        FROM code_processing.repository_file_versions f
        LEFT JOIN code_processing.directory_classifications d
          ON f.repository = d.repository AND f.branch = d.branch
          AND d.dir_path = regexp_replace(f.file_path, '/[^/]+$', '')
        WHERE f.repository = $1 AND f.branch = $2
          AND d.dir_path IS NULL
        LIMIT 10
        """,
        REPO,
        BRANCH,
    )
    for r in unmatched:
        print(f"  {r['file_path']}  (dir: {r['dir']})")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
