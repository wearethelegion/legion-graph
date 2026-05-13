"""
Centralized ID Generation for Code Intelligence Platform.

Provides consistent identifier generation across all storage systems:
- MongoDB (Document Storage)
- Neo4j (Graph Database)
- Qdrant (Vector Database)

All IDs are hash32 (32-character hexadecimal) for strong collision resistance (128 bits).
"""

import hashlib
from typing import Optional


class IDGenerator:
    """
    Centralized ID generation ensuring consistency across all databases.
    
    ID Format Standards:
    - document_id: hash32(workspace + repository + branch + file_path + content_hash)
    - chunk_id: hash32(document_id + hash32(chunk_content))
    - entity_id: hash32(document_id + entity_type + entity_name + context)
    """
    
    @staticmethod
    def _hash32(content: str) -> str:
        """
        Generate 32-character hexadecimal hash.
        
        Args:
            content: String to hash
            
        Returns:
            32-character hex string (16 bytes = 128 bits)
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:32]
    
    @staticmethod
    def generate_document_id(
        workspace: str,
        repository: str,
        branch: str,
        file_path: str,
        content_hash: str
    ) -> str:
        """
        Generate consistent document_id across all systems.
        
        MATCHES EXISTING IMPLEMENTATION:
        document_id = hashlib.sha256(f"{workspace}:{repository}:{branch}:{file_path}:{content_hash}".encode()).hexdigest()[:32]
        
        Used by:
        - MongoDB: Primary document identifier (_id field)
        - Neo4j: File node identifier (id property)
        - Qdrant: Document-level vector point ID
        
        Args:
            workspace: Workspace name (e.g., "code_intel", "default")
            repository: Repository name (e.g., "oscar-vet/vet_backend")
            branch: Branch name (e.g., "develop", "main")
            file_path: Full file path (e.g., "app/controllers/api/v1/clinic/documents_controller.rb")
            content_hash: SHA256 hash of file content (64 characters)
            
        Returns:
            32-character hash string (128 bits)
            
        Example:
            >>> IDGenerator.generate_document_id(
            ...     "default",
            ...     "oscar-vet/vet_backend",
            ...     "develop",
            ...     "app/controllers/api/v1/clinic/documents_controller.rb",
            ...     "748edf79067019782cdc5c2aea918436c6c449646a9677818be50b372ef6fb8b"
            ... )
            'b8c1e35d8062728841eefe1647b555cc'
        """
        composite = f"{workspace}:{repository}:{branch}:{file_path}:{content_hash}"
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def generate_chunk_id(
        document_id: str,
        chunk_content: str
    ) -> str:
        """
        Generate chunk_id based on document + content hash.
        
        Used by:
        - Neo4j: Method/chunk node identifier
        - Qdrant: Chunk-level vector point ID
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            chunk_content: Full chunk source code or content
            
        Returns:
            32-character hash string (128 bits)
            
        Example:
            >>> IDGenerator.generate_chunk_id(
            ...     "b8c1e35d8062728841eefe1647b555cc",
            ...     "def initialize\n  @module_name = DOCUMENTS\nend"
            ... )
            'c9d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6'
        """
        # Hash the chunk content for uniqueness
        chunk_hash = IDGenerator._hash32(chunk_content)
        
        # Combine document_id with chunk content hash
        composite = f"{document_id}:{chunk_hash}"
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def generate_entity_id(
        document_id: str,
        entity_type: str,
        entity_name: str,
        additional_context: Optional[str] = None
    ) -> str:
        """
        Generate entity_id for graph nodes (fields, symbols, meta-programming).
        
        Used by:
        - Neo4j: Field, Symbol, MetaProgramming, Variable node identifiers
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            entity_type: Type of entity (field, symbol, meta, variable, etc.)
            entity_name: Name of the entity
            additional_context: Optional scope/line number for disambiguation
            
        Returns:
            32-character hash string (128 bits)
            
        Example:
            >>> IDGenerator.generate_entity_id(
            ...     "b8c1e35d8062728841eefe1647b555cc",
            ...     "field",
            ...     "@clinic",
            ...     "scope:class"
            ... )
            'd4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9'
        """
        composite = f"{document_id}:{entity_type}:{entity_name}"
        if additional_context:
            composite += f":{additional_context}"
        
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def generate_method_id(
        document_id: str,
        method_name: str,
        start_line: int,
        parent_class: Optional[str] = None
    ) -> str:
        """
        Generate method_id for graph Method nodes.
        
        Convenience wrapper around generate_entity_id for methods.
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            method_name: Name of the method
            start_line: Starting line number (for disambiguation)
            parent_class: Optional parent class name
            
        Returns:
            32-character hash string (128 bits)
        """
        context = f"line:{start_line}"
        if parent_class:
            context = f"{parent_class}:{context}"
        
        return IDGenerator.generate_entity_id(
            document_id,
            "method",
            method_name,
            context
        )
    
    @staticmethod
    def generate_field_id(
        document_id: str,
        field_name: str,
        scope: str = "class"
    ) -> str:
        """
        Generate field_id for graph Field nodes.
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            field_name: Name of the field/variable
            scope: Scope of the field (class, instance, method)
            
        Returns:
            32-character hash string (128 bits)
        """
        return IDGenerator.generate_entity_id(
            document_id,
            "field",
            field_name,
            f"scope:{scope}"
        )
    
    @staticmethod
    def generate_symbol_id(
        document_id: str,
        symbol_type: str,
        symbol_name: str
    ) -> str:
        """
        Generate symbol_id for graph Symbol nodes (constants, modules).
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            symbol_type: Type of symbol (constant, module, enum, etc.)
            symbol_name: Name of the symbol
            
        Returns:
            32-character hash string (128 bits)
        """
        return IDGenerator.generate_entity_id(
            document_id,
            "symbol",
            symbol_name,
            f"type:{symbol_type}"
        )
    
    @staticmethod
    def generate_meta_programming_id(
        document_id: str,
        signature: str,
        line: int
    ) -> str:
        """
        Generate ID for MetaProgramming nodes.
        
        Args:
            document_id: Parent document identifier (hash32 - 32 chars)
            signature: Meta-programming signature (e.g., "include Utils::Documentable")
            line: Line number where meta-programming appears
            
        Returns:
            32-character hash string (128 bits)
        """
        return IDGenerator.generate_entity_id(
            document_id,
            "meta",
            signature,
            f"line:{line}"
        )
    
    @staticmethod
    def generate_dependency_id(
        dependency_name: str,
        source_type: str
    ) -> str:
        """
        Generate ID for ExternalDependency nodes.
        
        Note: Dependencies are shared across documents, so document_id not included.
        
        Args:
            dependency_name: Name of the dependency (e.g., "ApplicationController")
            source_type: Type of dependency (class, module, gem, etc.)
            
        Returns:
            32-character hash string (128 bits)
        """
        composite = f"dep:{dependency_name}:{source_type}"
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def generate_keyword_id(keyword_name: str) -> str:
        """
        Generate ID for Keyword nodes.
        
        Note: Keywords are shared across documents, so document_id not included.
        
        Args:
            keyword_name: The keyword (e.g., "API", "Controller")
            
        Returns:
            32-character hash string (128 bits)
        """
        composite = f"keyword:{keyword_name}"
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def generate_path_segment_id(segment_name: str) -> str:
        """
        Generate ID for PathSegment nodes.
        
        Note: Path segments are shared across files, so document_id not included.
        
        Args:
            segment_name: The path segment (e.g., "app", "controllers")
            
        Returns:
            32-character hash string (128 bits)
        """
        composite = f"path:{segment_name}"
        return IDGenerator._hash32(composite)
    
    @staticmethod
    def extract_document_reference(entity_id: str, document_id: str) -> dict:
        """
        Create document reference metadata for any entity.
        
        All entities should include this metadata to maintain traceability
        back to the original document in MongoDB.
        
        Args:
            entity_id: The entity's unique identifier
            document_id: The parent document identifier
            
        Returns:
            Dict with document reference metadata
            
        Example:
            >>> IDGenerator.extract_document_reference(
            ...     "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9",
            ...     "b8c1e35d8062728841eefe1647b555cc"
            ... )
            {'document_id': 'b8c1e35d8062728841eefe1647b555cc', 'entity_id': 'd4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9'}
        """
        return {
            'document_id': document_id,
            'entity_id': entity_id
        }


# Convenience functions for common operations
def doc_id(workspace: str, repository: str, branch: str, file_path: str, content_hash: str) -> str:
    """Shorthand for generate_document_id."""
    return IDGenerator.generate_document_id(workspace, repository, branch, file_path, content_hash)


def chunk_id(document_id: str, chunk_content: str) -> str:
    """Shorthand for generate_chunk_id."""
    return IDGenerator.generate_chunk_id(document_id, chunk_content)


def entity_id(document_id: str, entity_type: str, entity_name: str, context: str = None) -> str:
    """Shorthand for generate_entity_id."""
    return IDGenerator.generate_entity_id(document_id, entity_type, entity_name, context)


__all__ = [
    'IDGenerator',
    'doc_id',
    'chunk_id',
    'entity_id',
]
