"""
Ingestion Progress Repository
Postgres access for code_processing.pipeline_chunks and pipeline_counters.
"""

from typing import Optional, Dict, Any

from api.repositories.base_repository import BaseRepository


class IngestionProgressRepository(BaseRepository):
    """Repository for postgres-backed ingestion summaries and progress."""

    async def list_project_ingestions(
        self,
        project_id: str,
        status_filter: Optional[str],
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        query = """
            WITH chunk_stats AS (
                SELECT
                    ingestion_id,
                    COUNT(*)::int AS total_chunks,
                    MIN(created_at) AS started_at,
                    MIN(repository) AS repository,
                    MIN(branch) AS branch
                FROM code_processing.pipeline_chunks
                WHERE project_id = $1
                GROUP BY ingestion_id
            ),
            counter_stats AS (
                SELECT
                    ingestion_id,
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'chunks_received' THEN counter_value ELSE 0 END), 0)::int AS chunks_received,
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'entities_extracted' THEN counter_value ELSE 0 END), 0)::int AS entities_extracted,
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'edges_extracted' THEN counter_value ELSE 0 END), 0)::int AS edges_extracted,
                    COALESCE(SUM(CASE WHEN service_name = 'summarization' AND counter_name = 'summaries_produced' THEN counter_value ELSE 0 END), 0)::int AS summaries_produced,
                    COALESCE(SUM(CASE WHEN service_name = 'embedding' AND counter_name = 'embeddings_computed' THEN counter_value ELSE 0 END), 0)::int AS embeddings_computed,
                    COALESCE(SUM(CASE WHEN service_name = 'neo4j_storage' AND counter_name = 'chunks_processed' THEN counter_value ELSE 0 END), 0)::int AS chunks_processed,
                    MAX(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::int AS has_failed,
                    MAX(CASE WHEN service_name = 'entity_extraction' THEN 1 ELSE 0 END)::int AS has_entity_extraction,
                    MAX(CASE WHEN service_name = 'summarization' THEN 1 ELSE 0 END)::int AS has_summarization,
                    MAX(CASE WHEN service_name = 'embedding' THEN 1 ELSE 0 END)::int AS has_embedding,
                    MAX(CASE WHEN service_name = 'neo4j_storage' THEN 1 ELSE 0 END)::int AS has_neo4j_storage,
                    COALESCE(BOOL_AND(status = 'completed'), FALSE) AS all_completed,
                    MAX(CASE WHEN service_name = 'neo4j_storage' AND status = 'completed' THEN updated_at END) AS completed_at
                FROM code_processing.pipeline_counters
                WHERE ingestion_id IN (SELECT ingestion_id FROM chunk_stats)
                GROUP BY ingestion_id
            ),
            ingestions AS (
                SELECT
                    c.ingestion_id::text AS ingestion_id,
                    c.repository,
                    c.branch,
                    c.started_at,
                    c.total_chunks,
                    CASE
                        WHEN COALESCE(s.has_failed, 0) = 1 THEN 'failed'
                        WHEN c.total_chunks > 0
                            AND COALESCE(s.has_entity_extraction, 0) = 1
                            AND COALESCE(s.has_summarization, 0) = 1
                            AND COALESCE(s.has_embedding, 0) = 1
                            AND COALESCE(s.has_neo4j_storage, 0) = 1
                            AND s.all_completed
                            AND COALESCE(s.chunks_processed, 0) >= COALESCE(s.chunks_received, 0)
                        THEN 'completed'
                        ELSE 'running'
                    END AS status,
                    CASE
                        WHEN c.total_chunks > 0
                            AND COALESCE(s.has_entity_extraction, 0) = 1
                            AND COALESCE(s.has_summarization, 0) = 1
                            AND COALESCE(s.has_embedding, 0) = 1
                            AND COALESCE(s.has_neo4j_storage, 0) = 1
                            AND s.all_completed
                            AND COALESCE(s.chunks_processed, 0) >= COALESCE(s.chunks_received, 0)
                        THEN s.completed_at
                        ELSE NULL
                    END AS completed_at
                FROM chunk_stats c
                LEFT JOIN counter_stats s ON s.ingestion_id = c.ingestion_id
            ),
            filtered AS (
                SELECT *
                FROM ingestions
                WHERE ($2::text IS NULL OR status = $2)
            ),
            paged AS (
                SELECT *
                FROM filtered
                ORDER BY started_at DESC NULLS LAST
                LIMIT $3 OFFSET $4
            )
            SELECT
                COALESCE((SELECT COUNT(*) FROM filtered), 0)::int AS total_count,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'ingestion_id', ingestion_id,
                            'repository', repository,
                            'branch', branch,
                            'status', status,
                            'started_at', started_at,
                            'completed_at', completed_at,
                            'total_chunks', total_chunks
                        )
                        ORDER BY started_at DESC NULLS LAST
                    ),
                    '[]'::json
                ) AS ingestions
            FROM paged
        """

        row = await self.fetch_one(query, project_id, status_filter, limit, offset)
        return row or {"total_count": 0, "ingestions": []}

    async def get_ingestion_progress(self, ingestion_id: str) -> Optional[Dict[str, Any]]:
        query = """
            WITH counter_totals AS (
                SELECT
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'chunks_received' THEN counter_value ELSE 0 END), 0)::int AS extracted,
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'entities_extracted' THEN counter_value ELSE 0 END), 0)::int AS entities_extracted,
                    COALESCE(SUM(CASE WHEN service_name = 'entity_extraction' AND counter_name = 'edges_extracted' THEN counter_value ELSE 0 END), 0)::int AS edges_extracted,
                    COALESCE(SUM(CASE WHEN service_name = 'summarization' AND counter_name = 'summaries_produced' THEN counter_value ELSE 0 END), 0)::int AS summarised,
                    COALESCE(SUM(CASE WHEN service_name = 'embedding' AND counter_name = 'embeddings_computed' THEN counter_value ELSE 0 END), 0)::int AS embedded,
                    COALESCE(SUM(CASE WHEN service_name = 'neo4j_storage' AND counter_name = 'chunks_processed' THEN counter_value ELSE 0 END), 0)::int AS stored_neo4j,
                    MAX(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::int AS has_failed,
                    MAX(CASE WHEN service_name = 'entity_extraction' THEN 1 ELSE 0 END)::int AS has_entity_extraction,
                    MAX(CASE WHEN service_name = 'summarization' THEN 1 ELSE 0 END)::int AS has_summarization,
                    MAX(CASE WHEN service_name = 'embedding' THEN 1 ELSE 0 END)::int AS has_embedding,
                    MAX(CASE WHEN service_name = 'neo4j_storage' THEN 1 ELSE 0 END)::int AS has_neo4j_storage,
                    COALESCE(BOOL_AND(status = 'completed'), FALSE) AS all_completed,
                    COALESCE(MAX(updated_at), NOW()) AS updated_at
                FROM code_processing.pipeline_counters
                WHERE ingestion_id = $1
            )
            SELECT
                $1::text AS ingestion_id,
                CASE
                    WHEN has_failed = 1 THEN 'failed'
                    WHEN has_entity_extraction = 1
                        AND has_summarization = 1
                        AND has_embedding = 1
                        AND has_neo4j_storage = 1
                        AND all_completed
                        AND stored_neo4j >= extracted
                    THEN 'completed'
                    ELSE 'running'
                END AS status,
                updated_at,
                extracted,
                entities_extracted,
                edges_extracted,
                summarised,
                embedded,
                stored_neo4j,
                has_failed,
                has_entity_extraction,
                has_summarization,
                has_embedding,
                has_neo4j_storage,
                all_completed
            FROM counter_totals
        """

        return await self.fetch_one(query, ingestion_id)

    async def get_ingestion_total_chunks(self, ingestion_id: str) -> int:
        query = """
            SELECT COUNT(*)::int AS total_chunks
            FROM code_processing.pipeline_chunks
            WHERE ingestion_id = $1
        """

        row = await self.fetch_one(query, ingestion_id)
        if not row:
            return 0
        return row["total_chunks"] or 0

    async def get_ingestion_project_id(self, ingestion_id: str) -> Optional[str]:
        query = """
            SELECT project_id::text
            FROM code_processing.pipeline_chunks
            WHERE ingestion_id = $1
            LIMIT 1
        """

        return await self.fetch_val(query, ingestion_id)
