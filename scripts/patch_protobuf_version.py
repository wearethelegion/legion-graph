#!/usr/bin/env python3
"""Patch generated protobuf files for KGRAG gRPC server.

This script:
1. Removes runtime version validation that causes mismatch errors when
   grpcio-tools (bundling libprotoc 31.x/protobuf 6.x) generates code
   for a runtime using protobuf 5.x.
2. Fixes relative imports in generated files (import x_pb2 -> from . import x_pb2)

Usage:
    python3 patch_protobuf_version.py grpc_server/protos/*_pb2.py grpc_server/protos/*_pb2_grpc.py
    python3 patch_protobuf_version.py cognee_service/generated/brain_pb2.py cognee_service/generated/brain_pb2_grpc.py
"""

import re
import sys
from pathlib import Path


def patch_file(filepath: Path) -> bool:
    """Patch a generated protobuf file.

    Returns True if the file was modified, False otherwise.
    """
    content = filepath.read_text()
    original = content

    # Fix 1: Remove the runtime_version import line
    content = re.sub(
        r"^from google\.protobuf import runtime_version as _runtime_version\n",
        "",
        content,
        flags=re.MULTILINE,
    )

    # Fix 2: Remove the ValidateProtobufRuntimeVersion call (multi-line)
    # Pattern matches:
    #   _runtime_version.ValidateProtobufRuntimeVersion(
    #       _runtime_version.Domain.PUBLIC,
    #       6,
    #       31,
    #       1,
    #       '',
    #       'kgrag_common.proto'
    #   )
    content = re.sub(
        r"_runtime_version\.ValidateProtobufRuntimeVersion\(\s*"
        r"_runtime_version\.Domain\.PUBLIC,\s*"
        r"\d+,\s*"  # major version
        r"\d+,\s*"  # minor version
        r"\d+,\s*"  # patch version
        r"'[^']*',\s*"  # suffix (empty string)
        r"'[^']+'\s*"  # location (proto filename)
        r"\)\n",
        "",
        content,
        flags=re.DOTALL,
    )

    # Fix 3: Convert absolute imports to relative imports
    # "import foo_pb2 as foo__pb2" -> "from . import foo_pb2 as foo__pb2"
    content = re.sub(
        r"^import ([a-z_]+_pb2) as ", r"from . import \1 as ", content, flags=re.MULTILINE
    )

    # Fix 4: Strip grpcio runtime version check from *_pb2_grpc.py.
    # grpcio-tools 1.80+ bakes in `if _version_not_supported: raise RuntimeError(...)`
    # which fails against the runtime grpcio 1.71 used inside our containers.
    # The check is purely a guard rail; the generated code itself is forward-
    # compatible with the runtime API surface we exercise.
    content = re.sub(
        r"GRPC_GENERATED_VERSION = '[^']+'\n"
        r"GRPC_VERSION = grpc\.__version__\n"
        r"_version_not_supported = False\n+"
        r"try:\n"
        r"    from grpc\._utilities import first_version_is_lower\n"
        r"    _version_not_supported = first_version_is_lower\("
        r"GRPC_VERSION, GRPC_GENERATED_VERSION\)\n"
        r"except ImportError:\n"
        r"    _version_not_supported = True\n+"
        r"if _version_not_supported:\n"
        r"    raise RuntimeError\([\s\S]*?\)\n",
        "",
        content,
    )

    if content != original:
        filepath.write_text(content)
        return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: patch_protobuf_version.py <file1.py> [file2.py ...]")
        sys.exit(1)

    patched_count = 0
    for arg in sys.argv[1:]:
        path = Path(arg)
        if path.exists() and path.suffix == ".py":
            if patch_file(path):
                print(f"  Patched: {path.name}")
                patched_count += 1

    print(f"  Total: {patched_count} files patched")
