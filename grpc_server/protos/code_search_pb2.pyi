from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DomainInfo(_message.Message):
    __slots__ = ("name", "description", "project_id", "entity_count")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_COUNT_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    project_id: str
    entity_count: int
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., project_id: _Optional[str] = ..., entity_count: _Optional[int] = ...) -> None: ...

class RelatedEntity(_message.Message):
    __slots__ = ("relationship", "name", "type")
    RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    relationship: str
    name: str
    type: str
    def __init__(self, relationship: _Optional[str] = ..., name: _Optional[str] = ..., type: _Optional[str] = ...) -> None: ...

class EntityResult(_message.Message):
    __slots__ = ("entity_id", "name", "entity_type", "description", "project_id", "branch", "score", "related")
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    RELATED_FIELD_NUMBER: _ClassVar[int]
    entity_id: str
    name: str
    entity_type: str
    description: str
    project_id: str
    branch: str
    score: float
    related: _containers.RepeatedCompositeFieldContainer[RelatedEntity]
    def __init__(self, entity_id: _Optional[str] = ..., name: _Optional[str] = ..., entity_type: _Optional[str] = ..., description: _Optional[str] = ..., project_id: _Optional[str] = ..., branch: _Optional[str] = ..., score: _Optional[float] = ..., related: _Optional[_Iterable[_Union[RelatedEntity, _Mapping]]] = ...) -> None: ...

class SummaryResult(_message.Message):
    __slots__ = ("text", "file_path", "language", "project_id", "score")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    text: str
    file_path: str
    language: str
    project_id: str
    score: float
    def __init__(self, text: _Optional[str] = ..., file_path: _Optional[str] = ..., language: _Optional[str] = ..., project_id: _Optional[str] = ..., score: _Optional[float] = ...) -> None: ...

class CodeChunk(_message.Message):
    __slots__ = ("file_path", "language", "text", "start_line", "end_line")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    START_LINE_FIELD_NUMBER: _ClassVar[int]
    END_LINE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    language: str
    text: str
    start_line: int
    end_line: int
    def __init__(self, file_path: _Optional[str] = ..., language: _Optional[str] = ..., text: _Optional[str] = ..., start_line: _Optional[int] = ..., end_line: _Optional[int] = ...) -> None: ...

class GraphEdge(_message.Message):
    __slots__ = ("source", "relationship", "target", "target_type", "file_path")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    TARGET_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    source: str
    relationship: str
    target: str
    target_type: str
    file_path: str
    def __init__(self, source: _Optional[str] = ..., relationship: _Optional[str] = ..., target: _Optional[str] = ..., target_type: _Optional[str] = ..., file_path: _Optional[str] = ...) -> None: ...

class DomainsBlock(_message.Message):
    __slots__ = ("business_domains", "technical_tags")
    BUSINESS_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    TECHNICAL_TAGS_FIELD_NUMBER: _ClassVar[int]
    business_domains: _containers.RepeatedCompositeFieldContainer[DomainInfo]
    technical_tags: _containers.RepeatedCompositeFieldContainer[DomainInfo]
    def __init__(self, business_domains: _Optional[_Iterable[_Union[DomainInfo, _Mapping]]] = ..., technical_tags: _Optional[_Iterable[_Union[DomainInfo, _Mapping]]] = ...) -> None: ...

class GetDomainsRequest(_message.Message):
    __slots__ = ("project_id", "include_technical")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_TECHNICAL_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    include_technical: bool
    def __init__(self, project_id: _Optional[str] = ..., include_technical: bool = ...) -> None: ...

class GetDomainsResponse(_message.Message):
    __slots__ = ("business_domains", "technical_tags", "error_message", "error_code")
    BUSINESS_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    TECHNICAL_TAGS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    business_domains: _containers.RepeatedCompositeFieldContainer[DomainInfo]
    technical_tags: _containers.RepeatedCompositeFieldContainer[DomainInfo]
    error_message: str
    error_code: str
    def __init__(self, business_domains: _Optional[_Iterable[_Union[DomainInfo, _Mapping]]] = ..., technical_tags: _Optional[_Iterable[_Union[DomainInfo, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class SearchEntitiesRequest(_message.Message):
    __slots__ = ("query", "project_id", "branch", "domain", "entity_type", "limit", "exclude_tests", "exclude_domain_meta")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_DOMAIN_META_FIELD_NUMBER: _ClassVar[int]
    query: str
    project_id: str
    branch: str
    domain: str
    entity_type: str
    limit: int
    exclude_tests: bool
    exclude_domain_meta: bool
    def __init__(self, query: _Optional[str] = ..., project_id: _Optional[str] = ..., branch: _Optional[str] = ..., domain: _Optional[str] = ..., entity_type: _Optional[str] = ..., limit: _Optional[int] = ..., exclude_tests: bool = ..., exclude_domain_meta: bool = ...) -> None: ...

class SearchEntitiesResponse(_message.Message):
    __slots__ = ("results", "error_message", "error_code", "low_confidence_reason")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    LOW_CONFIDENCE_REASON_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[EntityResult]
    error_message: str
    error_code: str
    low_confidence_reason: str
    def __init__(self, results: _Optional[_Iterable[_Union[EntityResult, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ..., low_confidence_reason: _Optional[str] = ...) -> None: ...

class SearchSummariesRequest(_message.Message):
    __slots__ = ("query", "project_id", "limit", "exclude_tests")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    query: str
    project_id: str
    limit: int
    exclude_tests: bool
    def __init__(self, query: _Optional[str] = ..., project_id: _Optional[str] = ..., limit: _Optional[int] = ..., exclude_tests: bool = ...) -> None: ...

class SearchSummariesResponse(_message.Message):
    __slots__ = ("results", "error_message", "error_code", "low_confidence_reason")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    LOW_CONFIDENCE_REASON_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SummaryResult]
    error_message: str
    error_code: str
    low_confidence_reason: str
    def __init__(self, results: _Optional[_Iterable[_Union[SummaryResult, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ..., low_confidence_reason: _Optional[str] = ...) -> None: ...

class GetCodeForEntityRequest(_message.Message):
    __slots__ = ("entity_name", "exclude_tests", "limit")
    ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    entity_name: str
    exclude_tests: bool
    limit: int
    def __init__(self, entity_name: _Optional[str] = ..., exclude_tests: bool = ..., limit: _Optional[int] = ...) -> None: ...

class GetCodeForEntityResponse(_message.Message):
    __slots__ = ("results", "error_message", "error_code")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[CodeChunk]
    error_message: str
    error_code: str
    def __init__(self, results: _Optional[_Iterable[_Union[CodeChunk, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class TraverseGraphRequest(_message.Message):
    __slots__ = ("entity_name", "exclude_tests")
    ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    entity_name: str
    exclude_tests: bool
    def __init__(self, entity_name: _Optional[str] = ..., exclude_tests: bool = ...) -> None: ...

class TraverseGraphResponse(_message.Message):
    __slots__ = ("edges", "error_message", "error_code")
    EDGES_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    edges: _containers.RepeatedCompositeFieldContainer[GraphEdge]
    error_message: str
    error_code: str
    def __init__(self, edges: _Optional[_Iterable[_Union[GraphEdge, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class GetEntityGraphRequest(_message.Message):
    __slots__ = ("entity_name", "depth", "exclude_tests")
    ENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    entity_name: str
    depth: int
    exclude_tests: bool
    def __init__(self, entity_name: _Optional[str] = ..., depth: _Optional[int] = ..., exclude_tests: bool = ...) -> None: ...

class GetEntityGraphResponse(_message.Message):
    __slots__ = ("edges", "error_message", "error_code")
    EDGES_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    edges: _containers.RepeatedCompositeFieldContainer[GraphEdge]
    error_message: str
    error_code: str
    def __init__(self, edges: _Optional[_Iterable[_Union[GraphEdge, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class FullSearchRequest(_message.Message):
    __slots__ = ("query", "depth", "project_id", "domain", "entity_type", "limit", "exclude_tests")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TESTS_FIELD_NUMBER: _ClassVar[int]
    query: str
    depth: int
    project_id: str
    domain: str
    entity_type: str
    limit: int
    exclude_tests: bool
    def __init__(self, query: _Optional[str] = ..., depth: _Optional[int] = ..., project_id: _Optional[str] = ..., domain: _Optional[str] = ..., entity_type: _Optional[str] = ..., limit: _Optional[int] = ..., exclude_tests: bool = ...) -> None: ...

class FullSearchResponse(_message.Message):
    __slots__ = ("domains", "entities", "summaries", "code", "error_message", "error_code")
    DOMAINS_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    domains: DomainsBlock
    entities: _containers.RepeatedCompositeFieldContainer[EntityResult]
    summaries: _containers.RepeatedCompositeFieldContainer[SummaryResult]
    code: _containers.RepeatedCompositeFieldContainer[CodeChunk]
    error_message: str
    error_code: str
    def __init__(self, domains: _Optional[_Union[DomainsBlock, _Mapping]] = ..., entities: _Optional[_Iterable[_Union[EntityResult, _Mapping]]] = ..., summaries: _Optional[_Iterable[_Union[SummaryResult, _Mapping]]] = ..., code: _Optional[_Iterable[_Union[CodeChunk, _Mapping]]] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

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
