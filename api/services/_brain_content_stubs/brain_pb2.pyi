import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BrainContentKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BRAIN_CONTENT_KIND_UNSPECIFIED: _ClassVar[BrainContentKind]
    BRAIN_CONTENT_KIND_EXPERTISE: _ClassVar[BrainContentKind]
    BRAIN_CONTENT_KIND_KNOWLEDGE: _ClassVar[BrainContentKind]
    BRAIN_CONTENT_KIND_LESSON: _ClassVar[BrainContentKind]
BRAIN_CONTENT_KIND_UNSPECIFIED: BrainContentKind
BRAIN_CONTENT_KIND_EXPERTISE: BrainContentKind
BRAIN_CONTENT_KIND_KNOWLEDGE: BrainContentKind
BRAIN_CONTENT_KIND_LESSON: BrainContentKind

class GetByIdRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeleteByIdRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("success", "message", "error_code", "deleted_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    DELETED_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    error_code: str
    deleted_id: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., error_code: _Optional[str] = ..., deleted_id: _Optional[str] = ...) -> None: ...

class LinkRequest(_message.Message):
    __slots__ = ("agent_id", "expertise_id", "company_id", "linked_by")
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERTISE_ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    LINKED_BY_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    expertise_id: str
    company_id: str
    linked_by: str
    def __init__(self, agent_id: _Optional[str] = ..., expertise_id: _Optional[str] = ..., company_id: _Optional[str] = ..., linked_by: _Optional[str] = ...) -> None: ...

class LinkResponse(_message.Message):
    __slots__ = ("success", "message", "link_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LINK_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    link_id: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., link_id: _Optional[str] = ...) -> None: ...

class AddToBrainRequest(_message.Message):
    __slots__ = ("kind", "title", "content", "metadata", "when_to_use", "symptom", "root_cause", "solution", "prevention", "severity")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    kind: BrainContentKind
    title: str
    content: str
    metadata: _containers.ScalarMap[str, str]
    when_to_use: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    def __init__(self, kind: _Optional[_Union[BrainContentKind, str]] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., when_to_use: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ...) -> None: ...

class UpdateBrainRequest(_message.Message):
    __slots__ = ("id", "kind", "title", "content", "metadata", "when_to_use", "symptom", "root_cause", "solution", "prevention", "severity")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    id: str
    kind: BrainContentKind
    title: str
    content: str
    metadata: _containers.ScalarMap[str, str]
    when_to_use: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    def __init__(self, id: _Optional[str] = ..., kind: _Optional[_Union[BrainContentKind, str]] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., when_to_use: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ...) -> None: ...

class DeleteFromBrainRequest(_message.Message):
    __slots__ = ("id", "kind")
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    id: str
    kind: BrainContentKind
    def __init__(self, id: _Optional[str] = ..., kind: _Optional[_Union[BrainContentKind, str]] = ...) -> None: ...

class GetBrainContentRequest(_message.Message):
    __slots__ = ("id", "kind")
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    id: str
    kind: BrainContentKind
    def __init__(self, id: _Optional[str] = ..., kind: _Optional[_Union[BrainContentKind, str]] = ...) -> None: ...

class ListBrainContentRequest(_message.Message):
    __slots__ = ("kind", "page", "page_size")
    KIND_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    kind: BrainContentKind
    page: int
    page_size: int
    def __init__(self, kind: _Optional[_Union[BrainContentKind, str]] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class BrainContentResponse(_message.Message):
    __slots__ = ("success", "message", "error_code", "id", "kind", "title", "content_preview", "created_at", "updated_at", "metadata", "content", "when_to_use", "symptom", "root_cause", "solution", "prevention", "severity")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_PREVIEW_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    error_code: str
    id: str
    kind: BrainContentKind
    title: str
    content_preview: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    metadata: _containers.ScalarMap[str, str]
    content: str
    when_to_use: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., error_code: _Optional[str] = ..., id: _Optional[str] = ..., kind: _Optional[_Union[BrainContentKind, str]] = ..., title: _Optional[str] = ..., content_preview: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., content: _Optional[str] = ..., when_to_use: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ...) -> None: ...

class ListBrainContentResponse(_message.Message):
    __slots__ = ("success", "message", "error_code", "items", "total", "page", "page_size")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    error_code: str
    items: _containers.RepeatedCompositeFieldContainer[BrainContentResponse]
    total: int
    page: int
    page_size: int
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., error_code: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BrainContentResponse, _Mapping]]] = ..., total: _Optional[int] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class CreateKnowledgeRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "title", "text_content", "when_to_use", "metadata", "created_by_user_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TEXT_CONTENT_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    title: str
    text_content: str
    when_to_use: str
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., text_content: _Optional[str] = ..., when_to_use: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ...) -> None: ...

class UpdateKnowledgeRequest(_message.Message):
    __slots__ = ("id", "title", "text_content", "when_to_use", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TEXT_CONTENT_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    text_content: str
    when_to_use: str
    metadata: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., text_content: _Optional[str] = ..., when_to_use: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class ListKnowledgeRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "offset")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    offset: int
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class KnowledgeResponse(_message.Message):
    __slots__ = ("id", "company_id", "project_id", "title", "text_content", "when_to_use", "content_hash", "metadata", "created_by_user_id", "created_at", "updated_at", "chunks")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TEXT_CONTENT_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_FIELD_NUMBER: _ClassVar[int]
    id: str
    company_id: str
    project_id: str
    title: str
    text_content: str
    when_to_use: str
    content_hash: str
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    chunks: _containers.RepeatedCompositeFieldContainer[KnowledgeChunk]
    def __init__(self, id: _Optional[str] = ..., company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., text_content: _Optional[str] = ..., when_to_use: _Optional[str] = ..., content_hash: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., chunks: _Optional[_Iterable[_Union[KnowledgeChunk, _Mapping]]] = ...) -> None: ...

class KnowledgeChunk(_message.Message):
    __slots__ = ("id", "knowledge_id", "content", "summary", "position", "level", "parent_chunk_id", "chunk_type", "section_title", "has_code", "keywords", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    KNOWLEDGE_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECTION_TITLE_FIELD_NUMBER: _ClassVar[int]
    HAS_CODE_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    knowledge_id: str
    content: str
    summary: str
    position: int
    level: int
    parent_chunk_id: str
    chunk_type: str
    section_title: str
    has_code: bool
    keywords: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., knowledge_id: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., position: _Optional[int] = ..., level: _Optional[int] = ..., parent_chunk_id: _Optional[str] = ..., chunk_type: _Optional[str] = ..., section_title: _Optional[str] = ..., has_code: bool = ..., keywords: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListKnowledgeResponse(_message.Message):
    __slots__ = ("items", "total_count")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[KnowledgeResponse]
    total_count: int
    def __init__(self, items: _Optional[_Iterable[_Union[KnowledgeResponse, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class CreateExpertiseRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "title", "content", "summary", "when_to_use", "is_company_level", "metadata", "created_by_user_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    IS_COMPANY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    title: str
    content: str
    summary: str
    when_to_use: str
    is_company_level: bool
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., when_to_use: _Optional[str] = ..., is_company_level: bool = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ...) -> None: ...

class UpdateExpertiseRequest(_message.Message):
    __slots__ = ("id", "title", "content", "summary", "when_to_use", "is_company_level", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    IS_COMPANY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    content: str
    summary: str
    when_to_use: str
    is_company_level: bool
    metadata: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., when_to_use: _Optional[str] = ..., is_company_level: bool = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class ListExpertiseRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "offset", "is_company_level", "filter_company_level")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    IS_COMPANY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    FILTER_COMPANY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    offset: int
    is_company_level: bool
    filter_company_level: bool
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., is_company_level: bool = ..., filter_company_level: bool = ...) -> None: ...

class ExpertiseResponse(_message.Message):
    __slots__ = ("id", "company_id", "project_id", "title", "content", "summary", "when_to_use", "is_company_level", "content_hash", "metadata", "created_by_user_id", "created_at", "updated_at", "chunks")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    WHEN_TO_USE_FIELD_NUMBER: _ClassVar[int]
    IS_COMPANY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_FIELD_NUMBER: _ClassVar[int]
    id: str
    company_id: str
    project_id: str
    title: str
    content: str
    summary: str
    when_to_use: str
    is_company_level: bool
    content_hash: str
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    chunks: _containers.RepeatedCompositeFieldContainer[ExpertiseChunk]
    def __init__(self, id: _Optional[str] = ..., company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., when_to_use: _Optional[str] = ..., is_company_level: bool = ..., content_hash: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., chunks: _Optional[_Iterable[_Union[ExpertiseChunk, _Mapping]]] = ...) -> None: ...

class ExpertiseChunk(_message.Message):
    __slots__ = ("id", "expertise_id", "content", "summary", "position", "level", "parent_chunk_id", "chunk_path", "chunk_type", "section_title", "has_code", "keywords", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXPERTISE_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_PATH_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECTION_TITLE_FIELD_NUMBER: _ClassVar[int]
    HAS_CODE_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    expertise_id: str
    content: str
    summary: str
    position: int
    level: int
    parent_chunk_id: str
    chunk_path: str
    chunk_type: str
    section_title: str
    has_code: bool
    keywords: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., expertise_id: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., position: _Optional[int] = ..., level: _Optional[int] = ..., parent_chunk_id: _Optional[str] = ..., chunk_path: _Optional[str] = ..., chunk_type: _Optional[str] = ..., section_title: _Optional[str] = ..., has_code: bool = ..., keywords: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class AddExpertiseChunkRequest(_message.Message):
    __slots__ = ("expertise_id", "content", "summary", "parent_chunk_id", "chunk_type", "section_title")
    EXPERTISE_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECTION_TITLE_FIELD_NUMBER: _ClassVar[int]
    expertise_id: str
    content: str
    summary: str
    parent_chunk_id: str
    chunk_type: str
    section_title: str
    def __init__(self, expertise_id: _Optional[str] = ..., content: _Optional[str] = ..., summary: _Optional[str] = ..., parent_chunk_id: _Optional[str] = ..., chunk_type: _Optional[str] = ..., section_title: _Optional[str] = ...) -> None: ...

class ExpertiseChunkResponse(_message.Message):
    __slots__ = ("chunk",)
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    chunk: ExpertiseChunk
    def __init__(self, chunk: _Optional[_Union[ExpertiseChunk, _Mapping]] = ...) -> None: ...

class ListExpertiseResponse(_message.Message):
    __slots__ = ("items", "total_count")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ExpertiseResponse]
    total_count: int
    def __init__(self, items: _Optional[_Iterable[_Union[ExpertiseResponse, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class RecordLessonRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "title", "category", "symptom", "root_cause", "solution", "prevention", "severity", "tags", "files_changed", "content", "metadata", "created_by_user_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    FILES_CHANGED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    title: str
    category: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    files_changed: _containers.RepeatedScalarFieldContainer[str]
    content: str
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., category: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., files_changed: _Optional[_Iterable[str]] = ..., content: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ...) -> None: ...

class UpdateLessonRequest(_message.Message):
    __slots__ = ("id", "title", "category", "symptom", "root_cause", "solution", "prevention", "severity", "tags", "files_changed", "content", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    FILES_CHANGED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    category: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    files_changed: _containers.RepeatedScalarFieldContainer[str]
    content: str
    metadata: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., category: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., files_changed: _Optional[_Iterable[str]] = ..., content: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class ListLessonsRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "offset", "category", "severity")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    offset: int
    category: str
    severity: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., category: _Optional[str] = ..., severity: _Optional[str] = ...) -> None: ...

class LessonResponse(_message.Message):
    __slots__ = ("id", "company_id", "project_id", "title", "category", "symptom", "root_cause", "solution", "prevention", "severity", "tags", "files_changed", "content", "content_hash", "metadata", "created_by_user_id", "created_at", "updated_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SYMPTOM_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    PREVENTION_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    FILES_CHANGED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    company_id: str
    project_id: str
    title: str
    category: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    severity: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    files_changed: _containers.RepeatedScalarFieldContainer[str]
    content: str
    content_hash: str
    metadata: _struct_pb2.Struct
    created_by_user_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., category: _Optional[str] = ..., symptom: _Optional[str] = ..., root_cause: _Optional[str] = ..., solution: _Optional[str] = ..., prevention: _Optional[str] = ..., severity: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., files_changed: _Optional[_Iterable[str]] = ..., content: _Optional[str] = ..., content_hash: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., created_by_user_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListLessonsResponse(_message.Message):
    __slots__ = ("items", "total_count")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[LessonResponse]
    total_count: int
    def __init__(self, items: _Optional[_Iterable[_Union[LessonResponse, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class CreateEngagementRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "name", "ultimate_goal", "agent_id", "user_id", "summary", "session_id", "parent_engagement_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ULTIMATE_GOAL_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    name: str
    ultimate_goal: str
    agent_id: str
    user_id: str
    summary: str
    session_id: str
    parent_engagement_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., ultimate_goal: _Optional[str] = ..., agent_id: _Optional[str] = ..., user_id: _Optional[str] = ..., summary: _Optional[str] = ..., session_id: _Optional[str] = ..., parent_engagement_id: _Optional[str] = ...) -> None: ...

class UpdateEngagementRequest(_message.Message):
    __slots__ = ("id", "name", "status", "summary", "ultimate_goal", "parent_engagement_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    ULTIMATE_GOAL_FIELD_NUMBER: _ClassVar[int]
    PARENT_ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    status: str
    summary: str
    ultimate_goal: str
    parent_engagement_id: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., status: _Optional[str] = ..., summary: _Optional[str] = ..., ultimate_goal: _Optional[str] = ..., parent_engagement_id: _Optional[str] = ...) -> None: ...

class ListEngagementsRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "offset", "status", "parent_engagement_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PARENT_ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    offset: int
    status: str
    parent_engagement_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., status: _Optional[str] = ..., parent_engagement_id: _Optional[str] = ...) -> None: ...

class EngagementResponse(_message.Message):
    __slots__ = ("id", "company_id", "project_id", "name", "ultimate_goal", "agent_id", "user_id", "summary", "status", "session_id", "parent_engagement_id", "created_at", "updated_at", "entry_count")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ULTIMATE_GOAL_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    ENTRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    id: str
    company_id: str
    project_id: str
    name: str
    ultimate_goal: str
    agent_id: str
    user_id: str
    summary: str
    status: str
    session_id: str
    parent_engagement_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    entry_count: int
    def __init__(self, id: _Optional[str] = ..., company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., ultimate_goal: _Optional[str] = ..., agent_id: _Optional[str] = ..., user_id: _Optional[str] = ..., summary: _Optional[str] = ..., status: _Optional[str] = ..., session_id: _Optional[str] = ..., parent_engagement_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., entry_count: _Optional[int] = ...) -> None: ...

class ListEngagementsResponse(_message.Message):
    __slots__ = ("items", "total_count")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[EngagementResponse]
    total_count: int
    def __init__(self, items: _Optional[_Iterable[_Union[EngagementResponse, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class AddEntryRequest(_message.Message):
    __slots__ = ("engagement_id", "entry_type", "title", "content", "created_by_agent_id", "references", "tags", "summary", "session_id")
    ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    engagement_id: str
    entry_type: str
    title: str
    content: str
    created_by_agent_id: str
    references: _containers.RepeatedScalarFieldContainer[str]
    tags: _containers.RepeatedScalarFieldContainer[str]
    summary: str
    session_id: str
    def __init__(self, engagement_id: _Optional[str] = ..., entry_type: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., created_by_agent_id: _Optional[str] = ..., references: _Optional[_Iterable[str]] = ..., tags: _Optional[_Iterable[str]] = ..., summary: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class UpdateEntryRequest(_message.Message):
    __slots__ = ("id", "content", "references", "tags", "summary", "update_references", "update_tags")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    UPDATE_REFERENCES_FIELD_NUMBER: _ClassVar[int]
    UPDATE_TAGS_FIELD_NUMBER: _ClassVar[int]
    id: str
    content: str
    references: _containers.RepeatedScalarFieldContainer[str]
    tags: _containers.RepeatedScalarFieldContainer[str]
    summary: str
    update_references: bool
    update_tags: bool
    def __init__(self, id: _Optional[str] = ..., content: _Optional[str] = ..., references: _Optional[_Iterable[str]] = ..., tags: _Optional[_Iterable[str]] = ..., summary: _Optional[str] = ..., update_references: bool = ..., update_tags: bool = ...) -> None: ...

class ListEntriesRequest(_message.Message):
    __slots__ = ("engagement_id", "entry_type", "limit", "offset")
    ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    engagement_id: str
    entry_type: str
    limit: int
    offset: int
    def __init__(self, engagement_id: _Optional[str] = ..., entry_type: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class SearchEntriesRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "entry_type", "engagement_id")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ENTRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    entry_type: str
    engagement_id: str
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., entry_type: _Optional[str] = ..., engagement_id: _Optional[str] = ...) -> None: ...

class EntryResponse(_message.Message):
    __slots__ = ("id", "engagement_id", "entry_type", "title", "content", "created_by_agent_id", "references", "tags", "summary", "session_id", "version", "memory_level", "created_at", "updated_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MEMORY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    engagement_id: str
    entry_type: str
    title: str
    content: str
    created_by_agent_id: str
    references: _containers.RepeatedScalarFieldContainer[str]
    tags: _containers.RepeatedScalarFieldContainer[str]
    summary: str
    session_id: str
    version: int
    memory_level: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., engagement_id: _Optional[str] = ..., entry_type: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., created_by_agent_id: _Optional[str] = ..., references: _Optional[_Iterable[str]] = ..., tags: _Optional[_Iterable[str]] = ..., summary: _Optional[str] = ..., session_id: _Optional[str] = ..., version: _Optional[int] = ..., memory_level: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListEntriesResponse(_message.Message):
    __slots__ = ("items", "total_count")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[EntryResponse]
    total_count: int
    def __init__(self, items: _Optional[_Iterable[_Union[EntryResponse, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class ResumeEngagementResponse(_message.Message):
    __slots__ = ("engagement_id", "name", "status", "ultimate_goal", "summary", "entry_groups")
    ENGAGEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ULTIMATE_GOAL_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    ENTRY_GROUPS_FIELD_NUMBER: _ClassVar[int]
    engagement_id: str
    name: str
    status: str
    ultimate_goal: str
    summary: str
    entry_groups: _containers.RepeatedCompositeFieldContainer[EntryGroup]
    def __init__(self, engagement_id: _Optional[str] = ..., name: _Optional[str] = ..., status: _Optional[str] = ..., ultimate_goal: _Optional[str] = ..., summary: _Optional[str] = ..., entry_groups: _Optional[_Iterable[_Union[EntryGroup, _Mapping]]] = ...) -> None: ...

class EntryGroup(_message.Message):
    __slots__ = ("entry_type", "entries")
    ENTRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entry_type: str
    entries: _containers.RepeatedCompositeFieldContainer[EntryResponse]
    def __init__(self, entry_type: _Optional[str] = ..., entries: _Optional[_Iterable[_Union[EntryResponse, _Mapping]]] = ...) -> None: ...

class UnifiedSearchRequest(_message.Message):
    __slots__ = ("company_id", "project_id", "query", "limit", "types")
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    TYPES_FIELD_NUMBER: _ClassVar[int]
    company_id: str
    project_id: str
    query: str
    limit: int
    types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, company_id: _Optional[str] = ..., project_id: _Optional[str] = ..., query: _Optional[str] = ..., limit: _Optional[int] = ..., types: _Optional[_Iterable[str]] = ...) -> None: ...

class UnifiedSearchResponse(_message.Message):
    __slots__ = ("results", "total_count")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[UnifiedSearchResult]
    total_count: int
    def __init__(self, results: _Optional[_Iterable[_Union[UnifiedSearchResult, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class UnifiedSearchResult(_message.Message):
    __slots__ = ("entity_type", "id", "title", "snippet", "score", "metadata")
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SNIPPET_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    entity_type: str
    id: str
    title: str
    snippet: str
    score: float
    metadata: _struct_pb2.Struct
    def __init__(self, entity_type: _Optional[str] = ..., id: _Optional[str] = ..., title: _Optional[str] = ..., snippet: _Optional[str] = ..., score: _Optional[float] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
