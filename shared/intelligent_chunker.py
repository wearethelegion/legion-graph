"""
Intelligent Code Chunker with Nested Symbol Support

Implements size-based chunking strategy using AST nested symbols for optimal
chunk granularity and semantic preservation.

Author: Code Intelligence Team
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from shared.code_extraction import extract_by_lines
from shared.data_models import CodeChunk
from shared.metadata_inheritance import create_nested_chunk_metadata

logger = logging.getLogger(__name__)


@dataclass
class ChunkingStrategy:
    """Configuration for chunking strategy based on size"""

    # Size thresholds (in characters)
    SMALL_CHUNK_THRESHOLD = 800      # Keep whole, don't split
    MEDIUM_CHUNK_THRESHOLD = 2000    # Single method chunk
    LARGE_CHUNK_THRESHOLD = 2000     # Split by nested symbols

    # Line thresholds for nested splitting
    LARGE_NESTED_LINES = 50          # Re-analyze nested if > 50 lines


class IntelligentChunker:
    """
    Creates semantic chunks with intelligent size-based splitting.

    Uses nested symbol information from AST parsers to:
    1. Keep small methods whole (optimal size)
    2. Create single chunks for medium methods
    3. Split large methods by nested symbols (nested functions, try/catch)
    """

    def __init__(self):
        self.strategy = ChunkingStrategy()
        logger.info("🔧 IntelligentChunker initialized with size-based strategy")

    def chunk_method(
        self,
        method_info: Dict[str, Any],
        method_code: str,
        parent_metadata: Dict[str, Any],
        nested_symbols: List[Dict[str, Any]],
        document_id: str,
        file_path: str,
        original_code: str
    ) -> List[CodeChunk]:
        """
        Create chunks for a method using intelligent size-based strategy.

        Args:
            method_info: Method metadata from LLM analysis
            method_code: Extracted method code
            parent_metadata: Metadata to inherit (file-level context)
            nested_symbols: Nested functions/blocks from AST parser
            document_id: Document ID for chunk identification
            file_path: File path for context
            original_code: Full source code for extraction

        Returns:
            List of CodeChunk objects (1 for small/medium, N for large with nested)
        """
        method_size = len(method_code)
        method_name = method_info.get('name', 'unknown')
        start_line = method_info.get('start_line', 0)

        logger.debug(
            f"📏 Chunking {method_name}: {method_size} chars, "
            f"{len(nested_symbols)} nested symbols"
        )

        # Strategy 1: Small method - keep whole
        if method_size < self.strategy.SMALL_CHUNK_THRESHOLD:
            logger.debug(f"✅ Small method {method_name} - single chunk")
            return [self._create_single_chunk(
                method_info, method_code, parent_metadata,
                nested_symbols, document_id
            )]

        # Strategy 2: Medium method - single chunk
        if method_size < self.strategy.MEDIUM_CHUNK_THRESHOLD:
            logger.debug(f"✅ Medium method {method_name} - single chunk")
            return [self._create_single_chunk(
                method_info, method_code, parent_metadata,
                nested_symbols, document_id
            )]

        # Strategy 3: Large method - split by nested symbols if available
        if nested_symbols:
            logger.debug(
                f"📦 Large method {method_name} - splitting by {len(nested_symbols)} nested symbols"
            )
            return self._split_by_nested_symbols(
                method_info, method_code, parent_metadata,
                nested_symbols, document_id, original_code
            )
        else:
            # No nested symbols - keep as single large chunk
            logger.debug(
                f"⚠️ Large method {method_name} without nested symbols - single chunk"
            )
            return [self._create_single_chunk(
                method_info, method_code, parent_metadata,
                nested_symbols, document_id
            )]

    def _create_single_chunk(
        self,
        method_info: Dict[str, Any],
        method_code: str,
        parent_metadata: Dict[str, Any],
        nested_symbols: List[Dict[str, Any]],
        document_id: str
    ) -> CodeChunk:
        """Create a single chunk for entire method"""
        method_name = method_info.get('name', 'unknown')
        start_line = method_info.get('start_line', 0)
        end_line = method_info.get('end_line', 0)

        # Build metadata from parent + method info
        metadata = {
            **parent_metadata,
            'chunk_type': 'method',
            'name': method_name,
            'start_line': start_line,
            'end_line': end_line,
            'has_nested_symbols': len(nested_symbols) > 0,
            'nested_count': len(nested_symbols),
        }

        # Add method-specific metadata
        for field in ['signature', 'business_operation', 'complexity',
                     'parameters', 'return_type', 'visibility']:
            if field in method_info:
                metadata[field] = method_info[field]

        # Generate chunk ID
        chunk_id = f"{document_id}:method:{method_name}:{start_line}"

        return CodeChunk(
            chunk_id=chunk_id,
            content=method_code,
            metadata=metadata,
            relationships={}  # Will be populated by chunker
        )

    def _split_by_nested_symbols(
        self,
        method_info: Dict[str, Any],
        method_code: str,
        parent_metadata: Dict[str, Any],
        nested_symbols: List[Dict[str, Any]],
        document_id: str,
        original_code: str
    ) -> List[CodeChunk]:
        """
        Split large method into chunks by nested symbols.

        Creates:
        1. Parent method chunk (main logic)
        2. Nested function chunks (with inherited metadata)
        """
        chunks = []
        method_name = method_info.get('name', 'unknown')
        method_start = method_info.get('start_line', 0)
        method_end = method_info.get('end_line', 0)

        # Build parent metadata for inheritance
        method_metadata = {
            **parent_metadata,
            'chunk_type': 'method',
            'name': method_name,
            'start_line': method_start,
            'end_line': method_end,
            'business_operation': method_info.get('business_operation', ''),
            'complexity': method_info.get('complexity', 'unknown'),
            'quality_score': parent_metadata.get('quality_score', 0.0),
            'contextual_keywords': method_info.get('contextual_keywords', []),
        }

        # Generate parent chunk ID
        parent_chunk_id = f"{document_id}:method:{method_name}:{method_start}"
        method_metadata['chunk_id'] = parent_chunk_id

        # Create chunks for each nested symbol
        for idx, nested_symbol in enumerate(nested_symbols):
            nested_kind = nested_symbol.get('kind', 'unknown')
            nested_name = nested_symbol.get('name', 'unknown')
            nested_span = nested_symbol.get('span', {})
            nested_start = nested_span.get('start_line', 0)
            nested_end = nested_span.get('end_line', 0)

            # Extract nested code
            nested_code = extract_by_lines(original_code, nested_start, nested_end)

            if not nested_code:
                logger.warning(
                    f"Failed to extract nested {nested_kind} {nested_name} "
                    f"at lines {nested_start}-{nested_end}"
                )
                continue

            # Calculate size
            nested_size = nested_end - nested_start + 1

            # Decide: include in parent or create separate chunk
            if nested_size < 10:
                # Very small nested - include in parent chunk
                logger.debug(
                    f"Tiny nested {nested_kind} {nested_name} ({nested_size} lines) - "
                    f"including in parent"
                )
                continue

            # Create separate chunk with inherited metadata
            inherited_metadata = create_nested_chunk_metadata(
                parent_metadata=method_metadata,
                nested_symbol=nested_symbol,
                chunk_code=nested_code
            )

            # Generate chunk ID for nested
            chunk_id = f"{parent_chunk_id}:nested:{nested_kind}:{nested_name}:{nested_start}"

            chunks.append(CodeChunk(
                chunk_id=chunk_id,
                content=nested_code,
                metadata=inherited_metadata,
                relationships={
                    'parent': {
                        'chunk_id': parent_chunk_id,
                        'type': 'method',
                        'name': method_name
                    }
                }
            ))

            logger.debug(
                f"✅ Created nested chunk: {nested_kind} {nested_name} "
                f"({nested_size} lines) with inherited metadata"
            )

        # If we created nested chunks, also create parent chunk (without nested code)
        if chunks:
            # Parent chunk contains main method logic (we keep full code for now)
            # In future, could extract only non-nested portions
            parent_chunk = CodeChunk(
                chunk_id=parent_chunk_id,
                content=method_code,
                metadata=method_metadata,
                relationships={
                    'contains_nested': [
                        {
                            'chunk_id': chunk.chunk_id,
                            'type': chunk.metadata['chunk_type'],
                            'name': chunk.metadata['name']
                        }
                        for chunk in chunks
                    ]
                }
            )

            # Insert parent at beginning
            chunks.insert(0, parent_chunk)

            logger.info(
                f"📦 Split {method_name} into {len(chunks)} chunks "
                f"(1 parent + {len(chunks)-1} nested)"
            )
        else:
            # No nested chunks created - return single chunk
            chunks = [self._create_single_chunk(
                method_info, method_code, parent_metadata,
                nested_symbols, document_id
            )]

        return chunks

    def should_create_nested_chunk(
        self,
        nested_symbol: Dict[str, Any]
    ) -> bool:
        """
        Determine if nested symbol should be separate chunk.

        Criteria:
        - Size > 10 lines
        - Type is nested_function (not just try/catch)
        """
        span = nested_symbol.get('span', {})
        lines = span.get('end_line', 0) - span.get('start_line', 0) + 1
        kind = nested_symbol.get('kind', '')

        # Only chunk nested functions that are substantial
        if kind == 'nested_function' and lines >= 10:
            return True

        # Try/catch blocks: only if very large (>30 lines)
        if kind in ['try_catch', 'try_except', 'rescue_block'] and lines >= 30:
            return True

        return False
