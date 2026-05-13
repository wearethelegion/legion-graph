"""
Embedding generation using Google Gemini.
Provides a unified interface for generating embeddings with batch processing.
"""

from typing import List, Optional, Union
import google.generativeai as genai
from loguru import logger
from kgrag.config import config


class GeminiEmbedder:
    """Generate embeddings using Google Gemini with batch processing support."""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        Initialize Gemini embedder.

        Args:
            api_key: Gemini API key (default from config)
            model: Model name (default from config)
        """
        self.api_key = api_key or config.GEMINI_API_KEY
        self.model = model or config.GEMINI_MODEL

        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not provided")

        # Configure Gemini
        genai.configure(api_key=self.api_key)
        logger.info(f"Initialized GeminiEmbedder with model: {self.model}")

    def embed_text(
        self,
        text: Union[str, List[str]],
        task_type: str = "RETRIEVAL_DOCUMENT",
        batch_size: int = 100
    ) -> List[List[float]]:
        """
        Generate embeddings for text with batch processing.

        Args:
            text: Single text or list of texts
            task_type: Task type for embedding (RETRIEVAL_DOCUMENT, RETRIEVAL_QUERY, etc.)
            batch_size: Number of texts to process per API call (max 100 for Gemini)

        Returns:
            List of embedding vectors
        """
        # Ensure we have a list
        texts = [text] if isinstance(text, str) else text

        if not texts:
            return []

        # Limit batch size to Gemini's maximum
        batch_size = min(batch_size, 100)

        embeddings = []
        total_texts = len(texts)

        # Process in batches
        for i in range(0, total_texts, batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = self._process_batch(batch, task_type)
            embeddings.extend(batch_embeddings)

            if len(batch) > 1:
                logger.debug(f"Processed batch {i//batch_size + 1}: {len(batch)} texts ({i+len(batch)}/{total_texts})")

        return embeddings

    def _process_batch(self, batch: List[str], task_type: str) -> List[List[float]]:
        """
        Process a single batch of texts.

        Args:
            batch: List of texts to embed
            task_type: Task type for embedding

        Returns:
            List of embedding vectors
        """
        # Filter empty texts and track their positions
        non_empty_texts = []
        empty_positions = []

        for idx, t in enumerate(batch):
            if not t or not t.strip():
                empty_positions.append(idx)
            else:
                non_empty_texts.append(t)

        # If all texts are empty, return zero vectors
        if not non_empty_texts:
            logger.warning(f"Batch contains only empty texts ({len(batch)} texts)")
            return [[0.0] * config.GEMINI_EMBEDDING_DIM for _ in batch]

        try:
            # Try batch processing first
            if len(non_empty_texts) > 1:
                embeddings = self._batch_embed(non_empty_texts, task_type)
            else:
                # Single text
                embeddings = self._batch_embed(non_empty_texts[0], task_type)

            # Re-insert zero vectors for empty texts
            if empty_positions:
                result = []
                non_empty_idx = 0
                for idx in range(len(batch)):
                    if idx in empty_positions:
                        result.append([0.0] * config.GEMINI_EMBEDDING_DIM)
                    else:
                        result.append(embeddings[non_empty_idx])
                        non_empty_idx += 1
                return result

            return embeddings

        except Exception as e:
            logger.warning(f"Batch processing failed ({len(non_empty_texts)} texts): {e}. Falling back to sequential.")
            return self._sequential_embed(batch, task_type)

    def _batch_embed(self, texts: Union[str, List[str]], task_type: str) -> List[List[float]]:
        """
        Call Gemini API for batch embedding.

        Args:
            texts: Single text or list of texts
            task_type: Task type for embedding

        Returns:
            List of embedding vectors

        Raises:
            Exception: If API call fails
        """
        result = genai.embed_content(
            model=self.model,
            content=texts,
            task_type=task_type,
            output_dimensionality=config.GEMINI_EMBEDDING_DIM
        )

        # Handle different return formats
        # Single text: {'embedding': [...]}
        # Batch: {'embeddings': [[...], [...]]} or {'embedding': [[...], [...]]}
        if 'embeddings' in result:
            embeddings = result['embeddings']
            logger.debug(f"Batch API returned {len(embeddings)} embeddings")
            return embeddings
        elif 'embedding' in result:
            embedding = result['embedding']
            # Check if it's a list of embeddings or single embedding
            if isinstance(embedding[0], list):
                logger.debug(f"Batch API returned {len(embedding)} embeddings (nested)")
                return embedding
            else:
                # Single embedding
                logger.debug("Single embedding returned")
                return [embedding]
        else:
            raise ValueError(f"Unexpected API response format: {result.keys()}")

    def _sequential_embed(self, batch: List[str], task_type: str) -> List[List[float]]:
        """
        Fallback to sequential processing for a batch.

        Args:
            batch: List of texts to embed
            task_type: Task type for embedding

        Returns:
            List of embedding vectors
        """
        embeddings = []
        for t in batch:
            if not t or not t.strip():
                embeddings.append([0.0] * config.GEMINI_EMBEDDING_DIM)
                continue

            try:
                result = genai.embed_content(
                    model=self.model,
                    content=t,
                    task_type=task_type,
                    output_dimensionality=config.GEMINI_EMBEDDING_DIM
                )
                embeddings.append(result['embedding'])
            except Exception as e:
                logger.error(f"Failed to generate embedding for text (len={len(t)}): {e}")
                # Return zero vector on error
                embeddings.append([0.0] * config.GEMINI_EMBEDDING_DIM)

        return embeddings

    def embed_query(self, query: str) -> List[float]:
        """
        Generate embedding for a search query.

        Uses RETRIEVAL_QUERY task type to match the Chonkie library convention
        and optimize for search query embedding (asymmetric retrieval).

        Args:
            query: Search query text

        Returns:
            Embedding vector
        """
        result = self.embed_text(query, task_type="RETRIEVAL_QUERY")
        return result[0] if result else [0.0] * config.GEMINI_EMBEDDING_DIM

    def embed_documents(self, documents: List[str], batch_size: int = 100) -> List[List[float]]:
        """
        Generate embeddings for multiple documents with batch processing.

        Args:
            documents: List of document texts
            batch_size: Number of documents to process per API call (max 100)

        Returns:
            List of embedding vectors
        """
        return self.embed_text(documents, task_type="RETRIEVAL_DOCUMENT", batch_size=batch_size)


class OpenAIEmbedder:
    """Generate embeddings using OpenAI (for comparison/testing)."""

    def __init__(self, api_key: Optional[str] = None, model: str = "text-embedding-3-small"):
        """
        Initialize OpenAI embedder.

        Args:
            api_key: OpenAI API key (default from config)
            model: Model name
        """
        self.api_key = api_key or config.OPENAI_API_KEY
        self.model = model

        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not provided")

        from openai import OpenAI
        self.client = OpenAI(api_key=self.api_key)
        logger.info(f"Initialized OpenAIEmbedder with model: {self.model}")

    def embed_text(self, text: Union[str, List[str]]) -> List[List[float]]:
        """
        Generate embeddings for text.

        Args:
            text: Single text or list of texts

        Returns:
            List of embedding vectors
        """
        texts = [text] if isinstance(text, str) else text

        if not texts:
            return []

        try:
            response = self.client.embeddings.create(
                input=texts,
                model=self.model
            )
            embeddings = [item.embedding for item in response.data]
            logger.debug(f"Generated {len(embeddings)} OpenAI embeddings")
            return embeddings
        except Exception as e:
            logger.error(f"Failed to generate OpenAI embeddings: {e}")
            # Return zero vectors on error
            return [[0.0] * 1536 for _ in texts]  # OpenAI default dimension

    def embed_query(self, query: str) -> List[float]:
        """Generate embedding for a search query."""
        result = self.embed_text(query)
        return result[0] if result else [0.0] * 1536

    def embed_documents(self, documents: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple documents."""
        return self.embed_text(documents)


# Factory function to get embedder
def get_embedder(provider: str = "gemini", **kwargs) -> Union[GeminiEmbedder, OpenAIEmbedder]:
    """
    Get an embedder instance.

    Args:
        provider: Embedding provider ("gemini" or "openai")
        **kwargs: Additional arguments for the embedder

    Returns:
        Embedder instance
    """
    if provider.lower() == "gemini":
        return GeminiEmbedder(**kwargs)
    elif provider.lower() == "openai":
        return OpenAIEmbedder(**kwargs)
    else:
        raise ValueError(f"Unknown provider: {provider}")
