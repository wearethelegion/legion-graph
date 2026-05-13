from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AuthRequest(_message.Message):
    __slots__ = ("email", "password")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    email: str
    password: str
    def __init__(self, email: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class AuthResponse(_message.Message):
    __slots__ = ("status", "access_token", "refresh_token", "token_type", "expires_in", "user_email", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOKEN_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_FIELD_NUMBER: _ClassVar[int]
    USER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int
    user_email: str
    message: str
    def __init__(self, status: _Optional[str] = ..., access_token: _Optional[str] = ..., refresh_token: _Optional[str] = ..., token_type: _Optional[str] = ..., expires_in: _Optional[int] = ..., user_email: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class GetProjectsRequest(_message.Message):
    __slots__ = ("user_token",)
    USER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    user_token: str
    def __init__(self, user_token: _Optional[str] = ...) -> None: ...

class ProjectItem(_message.Message):
    __slots__ = ("id", "company_id", "name", "description", "company_name", "cognee_enabled")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMPANY_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    COMPANY_NAME_FIELD_NUMBER: _ClassVar[int]
    COGNEE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    id: str
    company_id: str
    name: str
    description: str
    company_name: str
    cognee_enabled: bool
    def __init__(self, id: _Optional[str] = ..., company_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., company_name: _Optional[str] = ..., cognee_enabled: bool = ...) -> None: ...

class GetProjectsResponse(_message.Message):
    __slots__ = ("status", "message", "projects_count", "projects", "user_email")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PROJECTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    PROJECTS_FIELD_NUMBER: _ClassVar[int]
    USER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    projects_count: int
    projects: _containers.RepeatedCompositeFieldContainer[ProjectItem]
    user_email: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ..., projects_count: _Optional[int] = ..., projects: _Optional[_Iterable[_Union[ProjectItem, _Mapping]]] = ..., user_email: _Optional[str] = ...) -> None: ...
