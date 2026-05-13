"""gRPC Servicers"""

# NOTE: Do not import servicers at module level
# This prevents duplicate proto registration when the package is imported
# Import directly from submodules when needed:
#   from grpc_server.servicers.auth_servicer import AuthServicer

__all__ = []
