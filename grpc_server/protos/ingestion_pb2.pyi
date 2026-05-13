from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetIngestionStatusRequest(_message.Message):
    __slots__ = ("user_token", "ingestion_id")
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    INGESTION_ID_FIELD_NUMBER: _ClassVar[int]
    user_token: str
    ingestion_id: str
    def __init__(self, user_token: _Optional[str] = ..., ingestion_id: _Optional[str] = ...) -> None: ...

class GetIngestionStatusResponse(_message.Message):
    __slots__ = ("status", "ingestion_id", "ingestion_status", "repository", "branch", "total_files", "files_processed", "files_failed", "files_skipped", "current_file", "failed_files", "started_at", "completed_at", "percentage", "error_message", "error_code")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INGESTION_ID_FIELD_NUMBER: _ClassVar[int]
    INGESTION_STATUS_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FILES_FIELD_NUMBER: _ClassVar[int]
    FILES_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    FILES_FAILED_FIELD_NUMBER: _ClassVar[int]
    FILES_SKIPPED_FIELD_NUMBER: _ClassVar[int]
    CURRENT_FILE_FIELD_NUMBER: _ClassVar[int]
    FAILED_FILES_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    ingestion_id: str
    ingestion_status: str
    repository: str
    branch: str
    total_files: int
    files_processed: int
    files_failed: int
    files_skipped: int
    current_file: str
    failed_files: _containers.RepeatedCompositeFieldContainer[FailedFile]
    started_at: str
    completed_at: str
    percentage: float
    error_message: str
    error_code: str
    def __init__(self, status: _Optional[str] = ..., ingestion_id: _Optional[str] = ..., ingestion_status: _Optional[str] = ..., repository: _Optional[str] = ..., branch: _Optional[str] = ..., total_files: _Optional[int] = ..., files_processed: _Optional[int] = ..., files_failed: _Optional[int] = ..., files_skipped: _Optional[int] = ..., current_file: _Optional[str] = ..., failed_files: _Optional[_Iterable[_Union[FailedFile, _Mapping]]] = ..., started_at: _Optional[str] = ..., completed_at: _Optional[str] = ..., percentage: _Optional[float] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class FailedFile(_message.Message):
    __slots__ = ("file_path", "error", "timestamp")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    error: str
    timestamp: str
    def __init__(self, file_path: _Optional[str] = ..., error: _Optional[str] = ..., timestamp: _Optional[str] = ...) -> None: ...

class ListIngestionsRequest(_message.Message):
    __slots__ = ("user_token", "project_id", "status_filter", "limit", "offset")
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    user_token: str
    project_id: str
    status_filter: str
    limit: int
    offset: int
    def __init__(self, user_token: _Optional[str] = ..., project_id: _Optional[str] = ..., status_filter: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ListIngestionsResponse(_message.Message):
    __slots__ = ("status", "ingestions", "total_count", "error_message", "error_code")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INGESTIONS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    status: str
    ingestions: _containers.RepeatedCompositeFieldContainer[IngestionSummary]
    total_count: int
    error_message: str
    error_code: str
    def __init__(self, status: _Optional[str] = ..., ingestions: _Optional[_Iterable[_Union[IngestionSummary, _Mapping]]] = ..., total_count: _Optional[int] = ..., error_message: _Optional[str] = ..., error_code: _Optional[str] = ...) -> None: ...

class IngestionSummary(_message.Message):
    __slots__ = ("ingestion_id", "repository", "branch", "ingestion_status", "total_files", "files_processed", "started_at", "percentage")
    INGESTION_ID_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    INGESTION_STATUS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FILES_FIELD_NUMBER: _ClassVar[int]
    FILES_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    ingestion_id: str
    repository: str
    branch: str
    ingestion_status: str
    total_files: int
    files_processed: int
    started_at: str
    percentage: float
    def __init__(self, ingestion_id: _Optional[str] = ..., repository: _Optional[str] = ..., branch: _Optional[str] = ..., ingestion_status: _Optional[str] = ..., total_files: _Optional[int] = ..., files_processed: _Optional[int] = ..., started_at: _Optional[str] = ..., percentage: _Optional[float] = ...) -> None: ...
