import kgrag_common_pb2 as _kgrag_common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class CreateCodeRequest(_message.Message):
    __slots__ = ("code", "filename", "user_token", "metadata", "request_id")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

    CODE_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    code: str
    filename: str
    user_token: str
    metadata: _containers.ScalarMap[str, str]
    request_id: str
    def __init__(
        self,
        code: _Optional[str] = ...,
        filename: _Optional[str] = ...,
        user_token: _Optional[str] = ...,
        metadata: _Optional[_Mapping[str, str]] = ...,
        request_id: _Optional[str] = ...,
    ) -> None: ...

class CreateCodeResponse(_message.Message):
    __slots__ = (
        "status",
        "message",
        "code_id",
        "filename",
        "language",
        "title",
        "summary",
        "chunks_count",
        "entities_count",
        "relationships_count",
        "error_message",
        "error_code",
    )
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CODE_ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_COUNT_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIPS_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    code_id: str
    filename: str
    language: str
    title: str
    summary: str
    chunks_count: int
    entities_count: int
    relationships_count: int
    error_message: str
    error_code: str
    def __init__(
        self,
        status: _Optional[str] = ...,
        message: _Optional[str] = ...,
        code_id: _Optional[str] = ...,
        filename: _Optional[str] = ...,
        language: _Optional[str] = ...,
        title: _Optional[str] = ...,
        summary: _Optional[str] = ...,
        chunks_count: _Optional[int] = ...,
        entities_count: _Optional[int] = ...,
        relationships_count: _Optional[int] = ...,
        error_message: _Optional[str] = ...,
        error_code: _Optional[str] = ...,
    ) -> None: ...

class FindSimilarCodeRequest(_message.Message):
    __slots__ = ("query", "language", "project_id", "limit", "user_token")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    query: str
    language: str
    project_id: str
    limit: int
    user_token: str
    def __init__(
        self,
        query: _Optional[str] = ...,
        language: _Optional[str] = ...,
        project_id: _Optional[str] = ...,
        limit: _Optional[int] = ...,
        user_token: _Optional[str] = ...,
    ) -> None: ...

class CodeResult(_message.Message):
    __slots__ = (
        "entity_id",
        "score",
        "entity_name",
        "entity_type",
        "filename",
        "file_path",
        "language",
        "line_start",
        "line_end",
        "code_snippet",
        "summary",
        "dependencies",
        "complexity",
        "is_entry_point",
    )
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    LINE_START_FIELD_NUMBER: _ClassVar[int]
    LINE_END_FIELD_NUMBER: _ClassVar[int]
    CODE_SNIPPET_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    IS_ENTRY_POINT_FIELD_NUMBER: _ClassVar[int]
    entity_id: str
    score: float
    entity_name: str
    entity_type: str
    filename: str
    file_path: str
    language: str
    line_start: int
    line_end: int
    code_snippet: str
    summary: str
    dependencies: _containers.RepeatedScalarFieldContainer[str]
    complexity: int
    is_entry_point: bool
    def __init__(
        self,
        entity_id: _Optional[str] = ...,
        score: _Optional[float] = ...,
        entity_name: _Optional[str] = ...,
        entity_type: _Optional[str] = ...,
        filename: _Optional[str] = ...,
        file_path: _Optional[str] = ...,
        language: _Optional[str] = ...,
        line_start: _Optional[int] = ...,
        line_end: _Optional[int] = ...,
        code_snippet: _Optional[str] = ...,
        summary: _Optional[str] = ...,
        dependencies: _Optional[_Iterable[str]] = ...,
        complexity: _Optional[int] = ...,
        is_entry_point: bool = ...,
    ) -> None: ...

class FindSimilarCodeResponse(_message.Message):
    __slots__ = ("status", "results", "total", "error_message", "error_code")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    results: _containers.RepeatedCompositeFieldContainer[CodeResult]
    total: int
    error_message: str
    error_code: str
    def __init__(
        self,
        status: _Optional[str] = ...,
        results: _Optional[_Iterable[_Union[CodeResult, _Mapping]]] = ...,
        total: _Optional[int] = ...,
        error_message: _Optional[str] = ...,
        error_code: _Optional[str] = ...,
    ) -> None: ...

class AnalyzeImpactRequest(_message.Message):
    __slots__ = ("entity_name", "entity_type", "project_id", "max_depth", "user_token")
    ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_DEPTH_FIELD_NUMBER: _ClassVar[int]
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    entity_name: str
    entity_type: str
    project_id: str
    max_depth: int
    user_token: str
    def __init__(
        self,
        entity_name: _Optional[str] = ...,
        entity_type: _Optional[str] = ...,
        project_id: _Optional[str] = ...,
        max_depth: _Optional[int] = ...,
        user_token: _Optional[str] = ...,
    ) -> None: ...

class ImpactPath(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedCompositeFieldContainer[EntityInfo]
    def __init__(self, path: _Optional[_Iterable[_Union[EntityInfo, _Mapping]]] = ...) -> None: ...

class EntityInfo(_message.Message):
    __slots__ = ("name", "type", "file_path", "filename", "line_start", "line_end", "code_snippet")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    LINE_START_FIELD_NUMBER: _ClassVar[int]
    LINE_END_FIELD_NUMBER: _ClassVar[int]
    CODE_SNIPPET_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: str
    file_path: str
    filename: str
    line_start: int
    line_end: int
    code_snippet: str
    def __init__(
        self,
        name: _Optional[str] = ...,
        type: _Optional[str] = ...,
        file_path: _Optional[str] = ...,
        filename: _Optional[str] = ...,
        line_start: _Optional[int] = ...,
        line_end: _Optional[int] = ...,
        code_snippet: _Optional[str] = ...,
    ) -> None: ...

class AnalyzeImpactResponse(_message.Message):
    __slots__ = (
        "status",
        "entity",
        "upstream",
        "downstream",
        "risk_level",
        "upstream_count",
        "downstream_count",
        "error_message",
        "error_code",
    )
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_FIELD_NUMBER: _ClassVar[int]
    DOWNSTREAM_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_COUNT_FIELD_NUMBER: _ClassVar[int]
    DOWNSTREAM_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    entity: EntityInfo
    upstream: _containers.RepeatedCompositeFieldContainer[ImpactPath]
    downstream: _containers.RepeatedCompositeFieldContainer[ImpactPath]
    risk_level: str
    upstream_count: int
    downstream_count: int
    error_message: str
    error_code: str
    def __init__(
        self,
        status: _Optional[str] = ...,
        entity: _Optional[_Union[EntityInfo, _Mapping]] = ...,
        upstream: _Optional[_Iterable[_Union[ImpactPath, _Mapping]]] = ...,
        downstream: _Optional[_Iterable[_Union[ImpactPath, _Mapping]]] = ...,
        risk_level: _Optional[str] = ...,
        upstream_count: _Optional[int] = ...,
        downstream_count: _Optional[int] = ...,
        error_message: _Optional[str] = ...,
        error_code: _Optional[str] = ...,
    ) -> None: ...

class TraceExecutionFlowRequest(_message.Message):
    __slots__ = ("entry_point", "project_id", "max_depth", "user_token")
    ENTRY_POINT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_DEPTH_FIELD_NUMBER: _ClassVar[int]
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    entry_point: str
    project_id: str
    max_depth: int
    user_token: str
    def __init__(
        self,
        entry_point: _Optional[str] = ...,
        project_id: _Optional[str] = ...,
        max_depth: _Optional[int] = ...,
        user_token: _Optional[str] = ...,
    ) -> None: ...

class ExecutionPath(_message.Message):
    __slots__ = ("path", "depth")
    PATH_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedCompositeFieldContainer[EntityInfo]
    depth: int
    def __init__(
        self,
        path: _Optional[_Iterable[_Union[EntityInfo, _Mapping]]] = ...,
        depth: _Optional[int] = ...,
    ) -> None: ...

class TraceExecutionFlowResponse(_message.Message):
    __slots__ = ("status", "paths", "total_paths", "error_message", "error_code")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PATHS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PATHS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    paths: _containers.RepeatedCompositeFieldContainer[ExecutionPath]
    total_paths: int
    error_message: str
    error_code: str
    def __init__(
        self,
        status: _Optional[str] = ...,
        paths: _Optional[_Iterable[_Union[ExecutionPath, _Mapping]]] = ...,
        total_paths: _Optional[int] = ...,
        error_message: _Optional[str] = ...,
        error_code: _Optional[str] = ...,
    ) -> None: ...
