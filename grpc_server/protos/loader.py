"""
Lazy proto loader to prevent duplicate descriptor registration.

This module provides a single point of entry for all proto imports,
ensuring each proto module is loaded exactly once to avoid the
"duplicate file name" error in protobuf's descriptor pool.

Phase 1 STRIP: Only the 5 kept servicers are loaded here.
  AuthService, CodeService, CodeSearchService, DocumentSearchService, IngestionService.
"""

# Import in dependency order: kgrag_common first, then others
import grpc_server.protos.kgrag_common_pb2 as kgrag_common_pb2
import grpc_server.protos.auth_pb2 as auth_pb2
import grpc_server.protos.auth_pb2_grpc as auth_pb2_grpc
import grpc_server.protos.code_pb2 as code_pb2
import grpc_server.protos.code_pb2_grpc as code_pb2_grpc
import grpc_server.protos.code_search_pb2 as code_search_pb2
import grpc_server.protos.code_search_pb2_grpc as code_search_pb2_grpc
import grpc_server.protos.document_search_pb2 as document_search_pb2
import grpc_server.protos.document_search_pb2_grpc as document_search_pb2_grpc
import grpc_server.protos.ingestion_pb2 as ingestion_pb2
import grpc_server.protos.ingestion_pb2_grpc as ingestion_pb2_grpc

__all__ = [
    "kgrag_common_pb2",
    "auth_pb2",
    "auth_pb2_grpc",
    "code_pb2",
    "code_pb2_grpc",
    "code_search_pb2",
    "code_search_pb2_grpc",
    "document_search_pb2",
    "document_search_pb2_grpc",
    "ingestion_pb2",
    "ingestion_pb2_grpc",
]
