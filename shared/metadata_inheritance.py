"""
Metadata Inheritance Strategy for Nested Code Chunks

Implements intelligent metadata inheritance where nested functions/blocks
inherit business context from their parent method, based on the principle:
"If method creates appointments, inner functions also create appointments"

Author: Code Intelligence Team
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class InheritanceConfig:
    """Configuration for what metadata to inherit vs re-analyze"""

    # Structural metadata - always inherit (free)
    INHERIT_FIELDS = [
        'file_path',
        'language',
        'parent_symbol',
        'main_entity_name',
        'main_entity_type',
        'name_space',
        'general_purpose',
        'repo_name',
        'document_id',
        'quality_score',
        'complexity',  # Inherit from parent - nested usually same complexity
    ]

    # Business context - inherit with modifications
    DERIVE_FIELDS = [
        'business_operation',  # Inherit but can specialize
        'contextual_keywords',  # Inherit core + add specific
    ]

    # Technical metadata - re-analyze if needed
    REANALYZE_FIELDS = [
        'parameters',  # Nested functions may have different params
        'return_type',  # Different return types
        'error_handling',  # May have specific error handling
    ]


class MetadataInheritor:
    """Handles intelligent metadata inheritance for nested code chunks"""

    def __init__(self):
        self.config = InheritanceConfig()

    def inherit_from_parent(
        self,
        parent_metadata: Dict[str, Any],
        nested_symbol: Dict[str, Any],
        chunk_code: str
    ) -> Dict[str, Any]:
        """
        Create metadata for nested chunk by inheriting from parent.

        Args:
            parent_metadata: Full metadata from parent method/function
            nested_symbol: AST info about nested symbol (from parser)
            chunk_code: Extracted code for this nested chunk

        Returns:
            Complete metadata dict for nested chunk
        """
        inherited = {}

        # Phase 1: Direct inheritance (free)
        for field in self.config.INHERIT_FIELDS:
            if field in parent_metadata:
                inherited[field] = parent_metadata[field]

        # Phase 2: Derived fields (business context)
        inherited.update(self._derive_business_context(
            parent_metadata,
            nested_symbol,
            chunk_code
        ))

        # Phase 3: Nested-specific metadata
        inherited.update({
            'chunk_type': nested_symbol['kind'],
            'name': nested_symbol['name'],
            'start_line': nested_symbol['span']['start_line'],
            'end_line': nested_symbol['span']['end_line'],
            'parent_chunk_id': parent_metadata.get('chunk_id'),
            'is_nested': True,
            'nesting_level': parent_metadata.get('nesting_level', 0) + 1,
        })

        return inherited

    def _derive_business_context(
        self,
        parent_metadata: Dict[str, Any],
        nested_symbol: Dict[str, Any],
        chunk_code: str
    ) -> Dict[str, Any]:
        """
        Derive business context from parent with intelligent modifications.

        Key insight: Nested functions serve the parent's business purpose.
        Example: If parent "creates appointments", nested function also
        "creates appointments" (specifically: validates, formats, etc.)
        """
        derived = {}

        # Business operation: Inherit parent's operation + add specificity
        parent_operation = parent_metadata.get('business_operation', '')
        nested_name = nested_symbol['name']

        # Derive specific operation from name + parent context
        if parent_operation:
            # Keep parent context, add nested function specificity
            if nested_symbol['kind'] == 'nested_function':
                derived['business_operation'] = (
                    f"{parent_operation} - {self._infer_nested_purpose(nested_name, chunk_code)}"
                )
            elif nested_symbol['kind'] in ['try_catch', 'try_except', 'rescue_block']:
                derived['business_operation'] = (
                    f"{parent_operation} - error handling"
                )
            else:
                derived['business_operation'] = parent_operation
        else:
            derived['business_operation'] = self._infer_nested_purpose(nested_name, chunk_code)

        # Contextual keywords: Inherit parent + add from nested code
        parent_keywords = parent_metadata.get('contextual_keywords', [])
        nested_keywords = self._extract_keywords_from_code(chunk_code)

        # Merge: parent keywords (high priority) + nested keywords (lower priority)
        all_keywords = []
        if isinstance(parent_keywords, list):
            for kw in parent_keywords:
                if isinstance(kw, dict):
                    all_keywords.append(kw)
                elif isinstance(kw, str):
                    all_keywords.append({'keyword': kw, 'probability': 0.8})

        # Add nested keywords with lower probability
        for kw in nested_keywords[:3]:  # Top 3 keywords from nested code
            all_keywords.append({'keyword': kw, 'probability': 0.6})

        derived['contextual_keywords'] = all_keywords[:10]  # Keep top 10

        return derived

    def _infer_nested_purpose(self, name: str, code: str) -> str:
        """Infer purpose of nested function from name and code patterns"""
        name_lower = name.lower()

        # Common patterns
        if 'validate' in name_lower or 'check' in name_lower:
            return "validation logic"
        elif 'format' in name_lower or 'transform' in name_lower:
            return "data formatting"
        elif 'handle' in name_lower:
            return "event/data handling"
        elif 'fetch' in name_lower or 'get' in name_lower or 'load' in name_lower:
            return "data retrieval"
        elif 'save' in name_lower or 'update' in name_lower or 'create' in name_lower:
            return "data persistence"
        elif 'filter' in name_lower or 'find' in name_lower or 'search' in name_lower:
            return "data filtering/search"
        elif 'calculate' in name_lower or 'compute' in name_lower:
            return "computation logic"
        else:
            # Fallback: generic helper
            return "helper logic"

    def _extract_keywords_from_code(self, code: str) -> List[str]:
        """Extract relevant keywords from code (simple pattern matching)"""
        keywords = []

        # Look for common patterns
        patterns = [
            'appointment', 'event', 'user', 'calendar', 'schedule',
            'payment', 'order', 'customer', 'product', 'cart',
            'email', 'notification', 'message', 'alert',
            'auth', 'login', 'session', 'token', 'permission',
            'database', 'query', 'transaction', 'record',
            'api', 'request', 'response', 'fetch', 'http',
            'validation', 'error', 'exception', 'retry',
        ]

        code_lower = code.lower()
        for pattern in patterns:
            if pattern in code_lower:
                keywords.append(pattern)

        return keywords[:5]  # Top 5 most relevant

    def should_reanalyze(self, nested_symbol: Dict[str, Any]) -> bool:
        """
        Determine if nested chunk needs LLM re-analysis.

        Current strategy: Only re-analyze if nested function is large (>50 lines)
        Small nested functions inherit everything.
        """
        span = nested_symbol['span']
        lines = span['end_line'] - span['start_line'] + 1

        # Large nested functions (>50 lines) might need re-analysis
        if lines > 50 and nested_symbol['kind'] == 'nested_function':
            return True

        # Everything else: inheritance is sufficient
        return False


# Convenience function for use in chunker
def create_nested_chunk_metadata(
    parent_metadata: Dict[str, Any],
    nested_symbol: Dict[str, Any],
    chunk_code: str
) -> Dict[str, Any]:
    """
    Helper function to create metadata for nested chunk.

    Usage in chunker:
        nested_metadata = create_nested_chunk_metadata(
            parent_metadata=method_metadata,
            nested_symbol=ast_nested_symbol,
            chunk_code=extracted_code
        )
    """
    inheritor = MetadataInheritor()
    return inheritor.inherit_from_parent(
        parent_metadata,
        nested_symbol,
        chunk_code
    )
