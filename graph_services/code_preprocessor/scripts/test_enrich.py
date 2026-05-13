"""Quick test: enrich chunks, then re-embed with enriched content.

Usage:
    python -m code_preprocessor.scripts.test_enrich [ingestion_id]
"""

import asyncio
import logging
import os
import sys

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth"
)
DEFAULT_INGESTION = "demo-33d40d93"


async def main():
    ingestion_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INGESTION
    logger.info("Testing enrichment for ingestion_id=%s", ingestion_id)

    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    try:
        # Step 1: Enrich chunks
        from code_preprocessor.enrichment import enrich_chunks

        enriched = await enrich_chunks(pool, ingestion_id)
        logger.info("Enriched %d chunks", enriched)

        if enriched == 0:
            logger.warning("No chunks enriched — check ingestion_id and data")
            return

        # Step 2: Print first enriched chunk
        row = await pool.fetchrow(
            "SELECT c.chunk_text, c.chunk_index, f.file_path "
            "FROM code_processing.file_chunks c "
            "JOIN code_processing.repository_file_versions f ON c.file_version_id = f.id "
            "WHERE c.ingestion_id = $1 ORDER BY f.file_path, c.chunk_index LIMIT 1",
            ingestion_id,
        )
        if row:
            print(f"\n{'=' * 60}")
            print(f"First enriched chunk — {row['file_path']} (chunk {row['chunk_index']})")
            print(f"{'=' * 60}")
            print(row["chunk_text"])
            print(f"{'=' * 60}\n")

        # Step 3: Clear embeddings so re-embed works on enriched content
        cleared = await pool.execute(
            "UPDATE code_processing.file_chunks SET embedding = NULL WHERE ingestion_id = $1",
            ingestion_id,
        )
        logger.info("Cleared embeddings: %s", cleared)

        # Step 4: Re-embed with enriched content
        from code_preprocessor.enrichment import batch_embed_chunks

        embedded = await batch_embed_chunks(pool, ingestion_id)
        logger.info("Re-embedded %d chunks with enriched content", embedded)

        # Step 5: Verify embedding dimensions
        emb_row = await pool.fetchrow(
            "SELECT id, array_length(embedding, 1) as dims "
            "FROM code_processing.file_chunks "
            "WHERE ingestion_id = $1 AND embedding IS NOT NULL LIMIT 1",
            ingestion_id,
        )
        if emb_row:
            logger.info("Embedding dimensions: %d", emb_row["dims"])
        else:
            logger.warning("No embeddings found — check GEMINI_API_KEY")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
