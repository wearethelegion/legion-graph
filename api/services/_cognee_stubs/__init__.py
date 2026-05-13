# Generated cognee gRPC stubs for the kgrag-api container.
# Generated from protos/cognee.proto — regenerate if the proto changes:
#   python -m grpc_tools.protoc \
#       -I./protos \
#       --python_out=./api/services/_cognee_stubs \
#       --grpc_python_out=./api/services/_cognee_stubs \
#       ./protos/cognee.proto
# Then fix the import in cognee_pb2_grpc.py:
#   sed -i 's/^import cognee_pb2/from api.services._cognee_stubs import cognee_pb2/' \
#       ./api/services/_cognee_stubs/cognee_pb2_grpc.py
from api.services._cognee_stubs import cognee_pb2, cognee_pb2_grpc

__all__ = ["cognee_pb2", "cognee_pb2_grpc"]
