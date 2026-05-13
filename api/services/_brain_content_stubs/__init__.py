# Generated BrainContent gRPC stubs for the kgrag-api container.
# Generated from protos/brain.proto — regenerate if the proto changes:
#   python -m grpc_tools.protoc \
#       -I./protos \
#       --python_out=./api/services/_brain_content_stubs \
#       --grpc_python_out=./api/services/_brain_content_stubs \
#       ./protos/brain.proto
# Then fix the import in brain_pb2_grpc.py:
#   sed -i 's/^import brain_pb2/from api.services._brain_content_stubs import brain_pb2/' \
#       ./api/services/_brain_content_stubs/brain_pb2_grpc.py
from api.services._brain_content_stubs import brain_pb2, brain_pb2_grpc

__all__ = ["brain_pb2", "brain_pb2_grpc"]
