"""
KGRAG gRPC Server
Production-grade gRPC service for knowledge management.
"""

# NOTE: Do not import servicers or interceptors at module level
# This prevents duplicate proto registration when the package is imported
# Import directly from submodules when needed:
#   from grpc_server.servicers.auth_servicer import AuthServicer
#   from grpc_server.interceptors.auth_interceptor import AuthenticationInterceptor

__all__ = []
