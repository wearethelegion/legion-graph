#!/bin/bash
# Generate gRPC proto stubs and fix relative imports.
#
# Runs in two contexts:
#   - Local dev (the user has uv installed and a project venv).
#   - Docker image builds (uv is NOT installed; the active python is the
#     runtime python from the image).
#
# We pick the python interpreter at runtime: prefer `uv run python` when uv is
# available (so local dev stays inside the project venv), fall back to plain
# `python` otherwise (Docker image builds).

set -e

if command -v uv >/dev/null 2>&1; then
  PY="uv run python"
else
  PY="python"
fi

echo "🔧 Generating proto stubs (using: $PY)..."
$PY -m grpc_tools.protoc \
    -I./grpc_server/protos \
    --python_out=./grpc_server/protos \
    --grpc_python_out=./grpc_server/protos \
    --pyi_out=./grpc_server/protos \
    ./grpc_server/protos/*.proto

echo "🔧 Removing kgrag_common_pb2_grpc.py (kgrag_common.proto has no services)..."
rm -f grpc_server/protos/kgrag_common_pb2_grpc.py

echo "🔧 Patching generated files (version validation + relative imports)..."
# The grpcio-tools package bundles libprotoc 31.x which generates code for protobuf 6.x
# but the runtime protobuf is 5.x (required by other dependencies like mem0ai, google-generativeai).
# The Python patch script:
# 1. Removes the version validation call that causes the mismatch error
# 2. Fixes relative imports (import x_pb2 -> from . import x_pb2)
$PY scripts/patch_protobuf_version.py grpc_server/protos/*_pb2.py grpc_server/protos/*_pb2_grpc.py

echo "✅ Proto generation complete"
