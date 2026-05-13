"""
Shared Data Models

Common data structures used across multiple services.

Author: Code Intelligence Team
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any


@dataclass
class CodeChunk:
    """Represents a semantic code chunk for vector + graph storage"""
    chunk_id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, List[Any]] = field(default_factory=dict)
