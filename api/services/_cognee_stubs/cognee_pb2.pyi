from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SearchType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GRAPH_COMPLETION: _ClassVar[SearchType]
    TRIPLET_COMPLETION: _ClassVar[SearchType]
    CHUNKS: _ClassVar[SearchType]
    RAG_COMPLETION: _ClassVar[SearchType]
    SUMMARIES: _ClassVar[SearchType]
    GRAPH_SUMMARY_COMPLETION: _ClassVar[SearchType]
    CYPHER: _ClassVar[SearchType]
    NATURAL_LANGUAGE: _ClassVar[SearchType]
    GRAPH_COMPLETION_COT: _ClassVar[SearchType]
    GRAPH_COMPLETION_CONTEXT_EXTENSION: _ClassVar[SearchType]
    FEELING_LUCKY: _ClassVar[SearchType]
    TEMPORAL: _ClassVar[SearchType]
    CODING_RULES: _ClassVar[SearchType]
    CHUNKS_LEXICAL: _ClassVar[SearchType]
GRAPH_COMPLETION: SearchType
TRIPLET_COMPLETION: SearchType
CHUNKS: SearchType
RAG_COMPLETION: SearchType
SUMMARIES: SearchType
GRAPH_SUMMARY_COMPLETION: SearchType
CYPHER: SearchType
NATURAL_LANGUAGE: SearchType
GRAPH_COMPLETION_COT: SearchType
GRAPH_COMPLETION_CONTEXT_EXTENSION: SearchType
FEELING_LUCKY: SearchType
TEMPORAL: SearchType
CODING_RULES: SearchType
CHUNKS_LEXICAL: SearchType

class CognifyRequest(_message.Message):
    __slots__ = ("text", "dataset_name", "entity_id", "tags", "company_id")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    text: str
    dataset_name: str
    entity_id: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    company_id: str
    def __init__(self, text: _Optional[str] = ..., dataset_name: _Optional[str] = ..., entity_id: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., company_id: _Optional[str] = ...) -> None: ...

class CognifyResponse(_message.Message):
    __slots__ = ("success", "message", "dataset_name", "entity_id", "input_tokens", "output_tokens", "total_tokens", "estimated_cost_usd", "duration_seconds")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_COST_USD_FIELD_NUMBER: _ClassVar[int]
    DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    dataset_name: str
    entity_id: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: float
    duration_seconds: float
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., dataset_name: _Optional[str] = ..., entity_id: _Optional[str] = ..., input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., total_tokens: _Optional[int] = ..., estimated_cost_usd: _Optional[float] = ..., duration_seconds: _Optional[float] = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("query", "limit", "dataset_name", "company_id", "search_type", "only_context", "project_id", "branch", "wide_search_top_k", "triplet_distance_penalty", "scope", "system_prompt", "top_k")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    ONLY_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    WIDE_SEARCH_TOP_K_FIELD_NUMBER: _ClassVar[int]
    TRIPLET_DISTANCE_PENALTY_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    query: str
    limit: int
    dataset_name: str
    company_id: str
    search_type: SearchType
    only_context: bool
    project_id: str
    branch: str
    wide_search_top_k: int
    triplet_distance_penalty: float
    scope: str
    system_prompt: str
    top_k: int
    def __init__(self, query: _Optional[str] = ..., limit: _Optional[int] = ..., dataset_name: _Optional[str] = ..., company_id: _Optional[str] = ..., search_type: _Optional[_Union[SearchType, str]] = ..., only_context: bool = ..., project_id: _Optional[str] = ..., branch: _Optional[str] = ..., wide_search_top_k: _Optional[int] = ..., triplet_distance_penalty: _Optional[float] = ..., scope: _Optional[str] = ..., system_prompt: _Optional[str] = ..., top_k: _Optional[int] = ...) -> None: ...

class SearchResult(_message.Message):
    __slots__ = ("id", "text", "score", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    score: float
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ..., score: _Optional[float] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("success", "message", "results", "actual_search_type", "error_code")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    results: _containers.RepeatedCompositeFieldContainer[SearchResult]
    actual_search_type: str
    error_code: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., results: _Optional[_Iterable[_Union[SearchResult, _Mapping]]] = ..., actual_search_type: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class FlexibleCogneeSearchRequest(_message.Message):
    __slots__ = ("query", "search_type", "limit", "wide_search_top_k", "triplet_distance_penalty", "only_context", "dataset_name", "branch", "system_prompt", "top_k")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    WIDE_SEARCH_TOP_K_FIELD_NUMBER: _ClassVar[int]
    TRIPLET_DISTANCE_PENALTY_FIELD_NUMBER: _ClassVar[int]
    ONLY_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    query: str
    search_type: SearchType
    limit: int
    wide_search_top_k: int
    triplet_distance_penalty: float
    only_context: bool
    dataset_name: str
    branch: str
    system_prompt: str
    top_k: int
    def __init__(self, query: _Optional[str] = ..., search_type: _Optional[_Union[SearchType, str]] = ..., limit: _Optional[int] = ..., wide_search_top_k: _Optional[int] = ..., triplet_distance_penalty: _Optional[float] = ..., only_context: bool = ..., dataset_name: _Optional[str] = ..., branch: _Optional[str] = ..., system_prompt: _Optional[str] = ..., top_k: _Optional[int] = ...) -> None: ...

class FlexibleCodeSearchRequest(_message.Message):
    __slots__ = ("query", "search_type", "limit", "wide_search_top_k", "triplet_distance_penalty", "only_context", "dataset_name", "branch", "project_id", "project_name", "system_prompt", "top_k")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    WIDE_SEARCH_TOP_K_FIELD_NUMBER: _ClassVar[int]
    TRIPLET_DISTANCE_PENALTY_FIELD_NUMBER: _ClassVar[int]
    ONLY_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_NAME_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    query: str
    search_type: SearchType
    limit: int
    wide_search_top_k: int
    triplet_distance_penalty: float
    only_context: bool
    dataset_name: str
    branch: str
    project_id: str
    project_name: str
    system_prompt: str
    top_k: int
    def __init__(self, query: _Optional[str] = ..., search_type: _Optional[_Union[SearchType, str]] = ..., limit: _Optional[int] = ..., wide_search_top_k: _Optional[int] = ..., triplet_distance_penalty: _Optional[float] = ..., only_context: bool = ..., dataset_name: _Optional[str] = ..., branch: _Optional[str] = ..., project_id: _Optional[str] = ..., project_name: _Optional[str] = ..., system_prompt: _Optional[str] = ..., top_k: _Optional[int] = ...) -> None: ...

class FlexibleCogneeSearchResponse(_message.Message):
    __slots__ = ("success", "message", "results", "actual_search_type", "error_code")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    results: _containers.RepeatedCompositeFieldContainer[SearchResult]
    actual_search_type: str
    error_code: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., results: _Optional[_Iterable[_Union[SearchResult, _Mapping]]] = ..., actual_search_type: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class FlexibleCodeSearchResponse(_message.Message):
    __slots__ = ("success", "message", "results", "actual_search_type", "error_code")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_SEARCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    results: _containers.RepeatedCompositeFieldContainer[SearchResult]
    actual_search_type: str
    error_code: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., results: _Optional[_Iterable[_Union[SearchResult, _Mapping]]] = ..., actual_search_type: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class PruneRequest(_message.Message):
    __slots__ = ("dataset_name", "company_id")
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    dataset_name: str
    company_id: str
    def __init__(self, dataset_name: _Optional[str] = ..., company_id: _Optional[str] = ...) -> None: ...

class PruneResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class HealthRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...
