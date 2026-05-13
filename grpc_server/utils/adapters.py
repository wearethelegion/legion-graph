"""
Protobuf ↔ Service Layer Adapters
Converts between protobuf messages and service layer dict/Pydantic models.

Phase 1 STRIP: Only code service adapters remain.
Knowledge, expertise, lessons adapters deleted with their servicers.
"""

from typing import Dict, Any, List
from grpc_server.protos.loader import (
    code_pb2,
)


# Code Service Adapters


def code_result_to_proto(result: Dict[str, Any]) -> code_pb2.CodeResult:
    """
    Convert service layer code result to protobuf CodeResult.

    Args:
        result: Dictionary from CodeQueryService.find_similar_code()

    Returns:
        CodeResult protobuf message
    """

    # Helper to safely convert to int
    def safe_int(value, default=0):
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        return default

    return code_pb2.CodeResult(
        entity_id=result.get("entity_id", ""),
        score=result.get("score", 0.0),
        entity_name=result.get("entity_name", ""),
        entity_type=result.get("entity_type", ""),
        filename=result.get("filename", ""),
        file_path=result.get("file_path", ""),
        language=result.get("language", ""),
        line_start=safe_int(result.get("line_start"), 0),
        line_end=safe_int(result.get("line_end"), 0),
        code_snippet=result.get("code_snippet", ""),
        summary=result.get("summary", ""),
        dependencies=result.get("dependencies", []),
        complexity=safe_int(result.get("complexity"), 0),
        is_entry_point=result.get("is_entry_point", False),
    )


def entity_info_to_proto(entity: Dict[str, Any]) -> code_pb2.EntityInfo:
    """
    Convert service layer entity info to protobuf EntityInfo.

    Args:
        entity: Dictionary with entity information

    Returns:
        EntityInfo protobuf message
    """
    return code_pb2.EntityInfo(
        name=entity.get("name", ""),
        type=entity.get("type", ""),
        file_path=entity.get("file_path", ""),
        filename=entity.get("filename", ""),
        line_start=entity.get("line_start", 0),
        line_end=entity.get("line_end", 0),
        code_snippet=entity.get("code_snippet", ""),
    )


def impact_path_to_proto(path: List[Dict[str, Any]]) -> code_pb2.ImpactPath:
    """
    Convert service layer impact path to protobuf ImpactPath.

    Args:
        path: List of entity dictionaries

    Returns:
        ImpactPath protobuf message
    """
    return code_pb2.ImpactPath(path=[entity_info_to_proto(entity) for entity in path])


def execution_path_to_proto(path_data: Dict[str, Any]) -> code_pb2.ExecutionPath:
    """
    Convert service layer execution path to protobuf ExecutionPath.

    Args:
        path_data: Dictionary with path and depth

    Returns:
        ExecutionPath protobuf message
    """
    return code_pb2.ExecutionPath(
        path=[entity_info_to_proto(entity) for entity in path_data.get("path", [])],
        depth=path_data.get("depth", 0),
    )
