"""JSON Schema and Pydantic models for code analysis.

Defines the structure for Gemini's structured output and validates results.
"""

from typing import Any
from pydantic import BaseModel, Field


# ============================================================================
# JSON Schema for Gemini Structured Output
# ============================================================================

CODE_ANALYSIS_SCHEMA = {
    "type": "object",
    "required": ["file_metadata", "entities", "relationships", "embeddings_metadata"],
    "properties": {
        "file_metadata": {
            "type": "object",
            "required": ["file_path", "language"],
            "properties": {
                "file_path": {"type": "string"},
                "language": {
                    "type": "string",
                    "enum": [
                        "python", "typescript", "ruby", "javascript",
                        "go", "java", "kotlin", "rust", "cpp", "c",
                        "csharp", "php", "swift"
                    ]
                },
                "summary": {
                    "type": "string",
                    "description": "2-3 sentence summary of what this file does"
                },
                "primary_purpose": {
                    "type": "string",
                    "enum": ["api", "service", "model", "util", "config", "test"]
                },
                "complexity": {
                    "type": "string",
                    "enum": ["simple", "moderate", "complex"]
                },
                "loc": {
                    "type": "integer",
                    "description": "Lines of code"
                },
                "imports": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "exports": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "key_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Design patterns detected"
                }
            }
        },
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type", "name", "line_start", "line_end"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique identifier: file_path:entity_name"
                    },
                    "type": {
                        "type": "string",
                        "enum": [
                            "class", "function", "method", "variable",
                            "interface", "type_alias"
                        ]
                    },
                    "name": {"type": "string"},
                    "signature": {
                        "type": "string",
                        "description": "Full signature with types"
                    },
                    "parameters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                                "default_value": {"type": "string"},
                                "optional": {"type": "boolean"}
                            }
                        }
                    },
                    "return_type": {"type": "string"},
                    "docstring": {"type": "string"},
                    "code_snippet": {
                        "type": "string",
                        "description": "The actual code"
                    },
                    "line_start": {"type": "integer"},
                    "line_end": {"type": "integer"},
                    "complexity": {
                        "type": "string",
                        "enum": ["simple", "moderate", "complex"]
                    },
                    "is_async": {"type": "boolean"},
                    "is_static": {"type": "boolean"},
                    "is_private": {"type": "boolean"},
                    "is_exported": {"type": "boolean"},
                    "decorators": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "implements_pattern": {
                        "type": "string",
                        "description": "Design pattern if detected"
                    },
                    "semantic_purpose": {
                        "type": "string",
                        "description": "What this entity does in plain English"
                    }
                }
            }
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["source_id", "target_id", "type"],
                "properties": {
                    "source_id": {"type": "string"},
                    "target_id": {"type": "string"},
                    "type": {
                        "type": "string",
                        "enum": [
                            "calls", "imports", "inherits", "implements",
                            "uses", "returns", "accepts", "overrides", "contains"
                        ]
                    },
                    "line_number": {"type": "integer"},
                    "context": {
                        "type": "string",
                        "description": "Why/how this relationship exists"
                    }
                }
            }
        },
        "embeddings_metadata": {
            "type": "object",
            "description": "Metadata for generating embeddings",
            "properties": {
                "file_embedding_text": {
                    "type": "string",
                    "description": "Text to embed for file-level search"
                },
                "entity_embeddings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "entity_id": {"type": "string"},
                            "signature_text": {
                                "type": "string",
                                "description": "For signature embedding"
                            },
                            "docstring_text": {
                                "type": "string",
                                "description": "For docstring embedding"
                            },
                            "semantic_text": {
                                "type": "string",
                                "description": "For full semantic embedding"
                            }
                        }
                    }
                }
            }
        }
    }
}


# ============================================================================
# Pydantic Models for Type Safety
# ============================================================================

class Parameter(BaseModel):
    """Function/method parameter."""
    name: str
    type: str = ""
    default_value: str | None = None
    optional: bool = False


class Entity(BaseModel):
    """Code entity (class, function, method, etc.)."""
    id: str
    type: str  # class, function, method, variable, interface, type_alias
    name: str
    signature: str = ""
    parameters: list[Parameter] = Field(default_factory=list)
    return_type: str = ""
    docstring: str = ""
    code_snippet: str = ""
    line_start: int
    line_end: int
    complexity: str = "moderate"  # simple, moderate, complex
    is_async: bool = False
    is_static: bool = False
    is_private: bool = False
    is_exported: bool = False
    decorators: list[str] = Field(default_factory=list)
    implements_pattern: str | None = None
    semantic_purpose: str = ""


class Relationship(BaseModel):
    """Relationship between entities."""
    source_id: str
    target_id: str
    type: str  # calls, imports, inherits, implements, uses, returns, accepts, overrides, contains
    line_number: int | None = None
    context: str = ""


class EmbeddingMetadata(BaseModel):
    """Metadata for entity embedding."""
    entity_id: str
    signature_text: str = ""
    docstring_text: str = ""
    semantic_text: str = ""


class FileMetadata(BaseModel):
    """File-level metadata."""
    file_path: str
    name: str = ""  # Filename only (e.g., "code_service.py")
    directory_path: str = ""  # Parent directory (e.g., "api/services")
    language: str
    summary: str
    primary_purpose: str = "util"  # api, service, model, util, config, test
    complexity: str = "moderate"
    loc: int = 0
    imports: list[str] = Field(default_factory=list)
    exports: list[str] = Field(default_factory=list)
    key_patterns: list[str] = Field(default_factory=list)


class EmbeddingsMetadata(BaseModel):
    """Embeddings metadata container."""
    file_embedding_text: str = ""
    entity_embeddings: list[EmbeddingMetadata] = Field(default_factory=list)


class CodeAnalysisResult(BaseModel):
    """Complete code analysis result."""
    file_metadata: FileMetadata
    entities: list[Entity] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    embeddings_metadata: EmbeddingsMetadata


class StorageStats(BaseModel):
    """Statistics from storage operations."""
    neo4j_nodes_created: int = 0
    neo4j_relationships_created: int = 0
    qdrant_points_created: int = 0
    duration_seconds: float = 0.0


class AnalysisReport(BaseModel):
    """Overall analysis report."""
    total_files: int
    successful: int
    failed: int
    skipped: int
    storage_stats: StorageStats
    duration_seconds: float
    errors: list[dict[str, Any]] = Field(default_factory=list)
