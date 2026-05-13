"""
Reciprocal Rank Fusion (RRF) algorithm for combining search results.
Merges vector search and graph traversal results optimally.
"""

from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from loguru import logger

# Lazy import for cross-encoder to avoid loading model at import time
_cross_encoder_model = None


class RRFusion:
    """
    Reciprocal Rank Fusion for combining multiple ranked lists.

    RRF formula: RRF(d) = Σ(1 / (k + rank(d)))
    where k is a constant (typically 60) and rank(d) is the document's rank in each list.
    """

    def __init__(self, k: int = 60):
        """
        Initialize RRF fusion.

        Args:
            k: RRF constant (higher = more weight to top results)
        """
        self.k = k
        logger.debug(f"Initialized RRFusion with k={k}")

    def fuse(
        self,
        result_lists: List[List[Dict[str, Any]]],
        weights: Optional[List[float]] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fuse multiple result lists using RRF.

        Args:
            result_lists: List of result lists, each containing dicts with 'id' field
            weights: Optional weights for each list (default: equal weights)
            limit: Maximum results to return

        Returns:
            Fused and ranked results
        """
        if not result_lists:
            return []

        # Default to equal weights
        if weights is None:
            weights = [1.0] * len(result_lists)
        elif len(weights) != len(result_lists):
            raise ValueError(f"Weights count ({len(weights)}) != lists count ({len(result_lists)})")

        # Calculate RRF scores
        rrf_scores = defaultdict(float)
        item_data = {}

        for list_idx, (results, weight) in enumerate(zip(result_lists, weights)):
            for rank, item in enumerate(results, start=1):
                # Use 'id' as the key for merging
                item_id = item.get("id")
                if not item_id:
                    logger.warning(f"Item missing 'id' field: {item}")
                    continue

                # Calculate RRF score for this item
                rrf_score = weight * (1.0 / (self.k + rank))
                rrf_scores[item_id] += rrf_score

                # Store item data (last one wins if duplicates)
                if item_id not in item_data:
                    item_data[item_id] = item.copy()
                    item_data[item_id]["sources"] = []

                # Track which sources this item came from
                item_data[item_id]["sources"].append({
                    "list": list_idx,
                    "rank": rank,
                    "original_score": item.get("score", 0.0)
                })

        # Sort by RRF score
        sorted_items = sorted(
            rrf_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]

        # Build final results
        results = []
        for item_id, rrf_score in sorted_items:
            result = item_data[item_id].copy()
            result["rrf_score"] = rrf_score
            result["fusion_rank"] = len(results) + 1
            results.append(result)

        logger.debug(f"Fused {len(result_lists)} lists into {len(results)} results")
        return results

    def fuse_vector_graph(
        self,
        vector_results: List[Dict[str, Any]],
        graph_results: List[Dict[str, Any]],
        vector_weight: float = 1.0,
        graph_weight: float = 1.0,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Specialized fusion for vector search and graph traversal.

        Args:
            vector_results: Results from vector search
            graph_results: Results from graph traversal
            vector_weight: Weight for vector results
            graph_weight: Weight for graph results
            limit: Maximum results to return

        Returns:
            Fused results with both vector and graph context
        """
        # Ensure both have consistent ID fields
        for item in vector_results:
            if "id" not in item and "text" in item:
                # Generate ID from text hash if missing
                import hashlib
                item["id"] = hashlib.md5(item.get("text", "").encode()).hexdigest()[:8]
            item["source_type"] = "vector"

        for item in graph_results:
            if "id" not in item and "name" in item:
                item["id"] = item["name"]
            item["source_type"] = "graph"

        # Fuse using RRF
        fused = self.fuse(
            [vector_results, graph_results],
            weights=[vector_weight, graph_weight],
            limit=limit
        )

        # Add fusion metadata
        for result in fused:
            sources = result.get("sources", [])
            result["from_vector"] = any(s["list"] == 0 for s in sources)
            result["from_graph"] = any(s["list"] == 1 for s in sources)
            result["fusion_type"] = "hybrid" if result["from_vector"] and result["from_graph"] else (
                "vector" if result["from_vector"] else "graph"
            )

        return fused


class DiversityReranker:
    """
    Rerank results to promote diversity while maintaining relevance.
    Implements Maximal Marginal Relevance (MMR).
    """

    def __init__(self, lambda_param: float = 0.5):
        """
        Initialize diversity reranker.

        Args:
            lambda_param: Balance between relevance (1.0) and diversity (0.0)
        """
        self.lambda_param = lambda_param

    def rerank(
        self,
        results: List[Dict[str, Any]],
        similarity_key: str = "text",
        relevance_key: str = "rrf_score",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Rerank results for diversity using MMR.

        Args:
            results: Initial ranked results
            similarity_key: Key to use for similarity comparison
            relevance_key: Key containing relevance scores
            limit: Maximum results to return

        Returns:
            Reranked results balancing relevance and diversity
        """
        if not results:
            return []

        # Start with the most relevant item
        selected = [results[0]]
        remaining = results[1:]

        while remaining and len(selected) < limit:
            best_score = -float('inf')
            best_idx = -1

            for idx, candidate in enumerate(remaining):
                # Relevance score
                relevance = candidate.get(relevance_key, 0.0)

                # Compute maximum similarity to already selected items
                max_sim = 0.0
                for selected_item in selected:
                    sim = self._compute_similarity(
                        candidate.get(similarity_key, ""),
                        selected_item.get(similarity_key, "")
                    )
                    max_sim = max(max_sim, sim)

                # MMR score
                mmr_score = self.lambda_param * relevance - (1 - self.lambda_param) * max_sim

                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = idx

            if best_idx >= 0:
                selected.append(remaining.pop(best_idx))
            else:
                break

        return selected

    def _compute_similarity(self, text1: str, text2: str) -> float:
        """
        Compute similarity between two texts.
        Simple Jaccard similarity for now, can be improved.

        Args:
            text1: First text
            text2: Second text

        Returns:
            Similarity score (0-1)
        """
        if not text1 or not text2:
            return 0.0

        # Simple word-based Jaccard similarity
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2

        return len(intersection) / len(union) if union else 0.0


class CrossEncoderReranker:
    """
    Production-grade reranking using cross-encoder models.

    Cross-encoders provide superior semantic relevance scoring by
    jointly encoding query and document together, achieving 20-30%
    better accuracy compared to bi-encoder similarity alone.
    """

    def __init__(self, model_name: str = 'cross-encoder/ms-marco-MiniLM-L-6-v2'):
        """
        Initialize cross-encoder reranker.

        Args:
            model_name: HuggingFace cross-encoder model
                       Default: ms-marco-MiniLM-L-6-v2 (fast, accurate)
        """
        self.model_name = model_name
        self._model = None  # Lazy loading
        logger.debug(f"Initialized CrossEncoderReranker with model={model_name}")

    def _load_model(self):
        """Lazy load cross-encoder model (only on first use)."""
        if self._model is None:
            try:
                from sentence_transformers import CrossEncoder
                logger.info(f"Loading cross-encoder model: {self.model_name}")
                self._model = CrossEncoder(self.model_name)
                logger.info("Cross-encoder model loaded successfully")
            except ImportError:
                logger.error(
                    "sentence-transformers not installed. "
                    "Install with: pip install sentence-transformers"
                )
                raise
            except Exception as e:
                logger.error(f"Failed to load cross-encoder model: {e}")
                raise

    def rerank(
        self,
        query: str,
        results: List[Dict[str, Any]],
        limit: int = 10,
        text_key: str = "text",
        score_key: str = "rrf_score",
        ce_weight: float = 0.7,
        original_weight: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Rerank results using cross-encoder for better semantic relevance.

        Args:
            query: Search query
            results: List of results with text field
            limit: Number of top results to return
            text_key: Key containing text content to rerank
            score_key: Key containing original scores (RRF/similarity)
            ce_weight: Weight for cross-encoder score (0-1)
            original_weight: Weight for original score (0-1)

        Returns:
            Reranked results with ce_score and final_score fields
        """
        if not results:
            logger.debug("No results to rerank")
            return []

        if not query or not query.strip():
            logger.warning("Empty query provided, returning original results")
            return results[:limit]

        # Load model on first use
        if self._model is None:
            self._load_model()

        # Create query-document pairs for cross-encoder
        pairs = []
        valid_indices = []

        for idx, result in enumerate(results):
            text = result.get(text_key, "")
            if text and text.strip():
                pairs.append([query, text])
                valid_indices.append(idx)
            else:
                logger.warning(f"Result at index {idx} has empty '{text_key}' field")

        if not pairs:
            logger.warning("No valid text content found in results")
            return results[:limit]

        # Get cross-encoder scores
        try:
            import time
            start = time.time()
            ce_scores = self._model.predict(pairs)
            elapsed = time.time() - start
            logger.debug(f"Cross-encoder scored {len(pairs)} pairs in {elapsed:.3f}s")
        except Exception as e:
            logger.error(f"Cross-encoder prediction failed: {e}, falling back to original scores")
            return results[:limit]

        # Normalize cross-encoder scores to 0-1 range for combination
        if len(ce_scores) > 0:
            min_ce = float(min(ce_scores))
            max_ce = float(max(ce_scores))
            ce_range = max_ce - min_ce

            if ce_range > 0:
                ce_scores_normalized = [(float(s) - min_ce) / ce_range for s in ce_scores]
            else:
                ce_scores_normalized = [0.5] * len(ce_scores)  # All same score
        else:
            ce_scores_normalized = []

        # Add scores to results and calculate final scores
        for i, idx in enumerate(valid_indices):
            result = results[idx]
            ce_score_normalized = ce_scores_normalized[i]

            result['ce_score'] = float(ce_scores[i])  # Original CE score
            result['ce_score_normalized'] = ce_score_normalized

            # Get original score (RRF, similarity, etc.)
            original_score = result.get(score_key, result.get('score', 0.0))

            # Normalize original score to 0-1 if needed
            if original_score > 1.0:
                # Assume it's already in reasonable range, just scale down
                original_score_normalized = min(original_score / 10.0, 1.0)
            else:
                original_score_normalized = original_score

            # Weighted combination
            result['final_score'] = (
                ce_weight * ce_score_normalized +
                original_weight * original_score_normalized
            )

            # Preserve original scores for debugging
            if score_key not in result:
                result[score_key] = original_score

        # Sort by final score (descending)
        reranked = sorted(
            results,
            key=lambda x: x.get('final_score', 0.0),
            reverse=True
        )

        logger.info(
            f"Reranked {len(results)} results, returning top {limit} "
            f"(CE weight: {ce_weight}, Original weight: {original_weight})"
        )

        return reranked[:limit]


# Convenience functions
def fuse_results(
    result_lists: List[List[Dict[str, Any]]],
    k: int = 60,
    weights: Optional[List[float]] = None,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Convenience function for RRF fusion."""
    fusion = RRFusion(k=k)
    return fusion.fuse(result_lists, weights, limit)


def fuse_hybrid(
    vector_results: List[Dict[str, Any]],
    graph_results: List[Dict[str, Any]],
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Convenience function for hybrid fusion."""
    fusion = RRFusion()
    return fusion.fuse_vector_graph(vector_results, graph_results, limit=limit)


def diversify_results(
    results: List[Dict[str, Any]],
    lambda_param: float = 0.5,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Convenience function for diversity reranking."""
    reranker = DiversityReranker(lambda_param=lambda_param)
    return reranker.rerank(results, limit=limit)