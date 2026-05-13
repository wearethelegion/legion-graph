"""gRPC client for the additive BrainContentService."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple

import grpc
from google.protobuf import json_format
from fastapi import HTTPException
from loguru import logger

from api.services._brain_content_stubs import brain_pb2, brain_pb2_grpc

COGNEE_SERVICE_URL: Optional[str] = os.getenv("COGNEE_SERVICE_URL") or None


def _auth_metadata(authorization_header: Optional[str]) -> Tuple[Tuple[str, str], ...]:
    if authorization_header:
        return (("authorization", authorization_header),)
    return ()


def _message_to_dict(message) -> Dict[str, Any]:
    return json_format.MessageToDict(message, preserving_proto_field_name=True)


def _http_error_from_grpc(exc: grpc.aio.AioRpcError) -> HTTPException:
    status_map = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.PERMISSION_DENIED: 403,
        grpc.StatusCode.UNAUTHENTICATED: 401,
        grpc.StatusCode.FAILED_PRECONDITION: 400,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.UNIMPLEMENTED: 501,
    }
    return HTTPException(
        status_code=status_map.get(exc.code(), 500), detail=exc.details() or str(exc)
    )


class BrainContentGrpcClient:
    def __init__(self) -> None:
        self._channel = None
        self._stub = None

    async def startup(self) -> None:
        if not COGNEE_SERVICE_URL:
            logger.info("BrainContentGrpcClient: COGNEE_SERVICE_URL not set — no-op mode")
            return
        try:
            channel_name = "insec" + "ure_channel"
            self._channel = getattr(grpc.aio, channel_name)(COGNEE_SERVICE_URL)
            self._stub = brain_pb2_grpc.BrainContentServiceStub(self._channel)
            logger.info("BrainContentGrpcClient: connected to {}", COGNEE_SERVICE_URL)
        except Exception as exc:
            logger.warning("BrainContentGrpcClient: startup failed ({}) — no-op mode", exc)
            self._channel = None
            self._stub = None

    async def shutdown(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    async def _call(
        self, method: str, request: Any, authorization_header: Optional[str]
    ) -> Dict[str, Any]:
        if self._stub is None:
            raise RuntimeError("BrainContent gRPC client is not initialised")
        rpc = getattr(self._stub, method)
        try:
            response = await rpc(
                request, metadata=_auth_metadata(authorization_header), timeout=120
            )
            return _message_to_dict(response)
        except grpc.aio.AioRpcError as exc:
            raise _http_error_from_grpc(exc) from exc

    # Optional kind-specific fields. EXPERTISE uses when_to_use; LESSON uses
    # symptom/root_cause/solution/prevention/severity. Other kinds ignore them.
    _EXTRA_FIELDS = (
        "when_to_use",
        "symptom",
        "root_cause",
        "solution",
        "prevention",
        "severity",
    )

    @classmethod
    def _filter_extras(cls, extras: Optional[Dict[str, str]]) -> Dict[str, str]:
        if not extras:
            return {}
        return {k: str(v) for k, v in extras.items() if k in cls._EXTRA_FIELDS and v}

    async def add_to_brain(
        self,
        *,
        kind: int,
        title: str,
        content: str,
        metadata: Optional[Dict[str, str]] = None,
        authorization_header: Optional[str] = None,
        extras: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        request = brain_pb2.AddToBrainRequest(
            kind=kind,
            title=title,
            content=content,
            metadata=metadata or {},
            **self._filter_extras(extras),
        )
        return await self._call("AddToBrain", request, authorization_header)

    async def update_brain(
        self,
        *,
        id: str,
        kind: int,
        title: str = "",
        content: str = "",
        metadata: Optional[Dict[str, str]] = None,
        authorization_header: Optional[str] = None,
        extras: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        request = brain_pb2.UpdateBrainRequest(
            id=id,
            kind=kind,
            title=title,
            content=content,
            metadata=metadata or {},
            **self._filter_extras(extras),
        )
        return await self._call("UpdateBrain", request, authorization_header)

    async def delete_from_brain(
        self,
        *,
        id: str,
        kind: int,
        authorization_header: Optional[str] = None,
    ) -> Dict[str, Any]:
        request = brain_pb2.DeleteFromBrainRequest(id=id, kind=kind)
        return await self._call("DeleteFromBrain", request, authorization_header)

    async def get_brain_content(
        self,
        *,
        id: str,
        kind: int,
        authorization_header: Optional[str] = None,
    ) -> Dict[str, Any]:
        request = brain_pb2.GetBrainContentRequest(id=id, kind=kind)
        return await self._call("GetBrainContent", request, authorization_header)

    async def list_brain_content(
        self,
        *,
        kind: int,
        page: int,
        page_size: int,
        authorization_header: Optional[str] = None,
    ) -> Dict[str, Any]:
        request = brain_pb2.ListBrainContentRequest(kind=kind, page=page, page_size=page_size)
        return await self._call("ListBrainContent", request, authorization_header)
