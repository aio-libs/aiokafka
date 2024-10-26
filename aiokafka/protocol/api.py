from __future__ import annotations

import abc
from io import BytesIO
from typing import Any, ClassVar, Optional, Union

from .struct import Struct
from .types import Array, Int16, Int32, Schema, String, TaggedFields


class RequestHeader_v0(Struct):
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
    )

    def __init__(
        self, request: Request, correlation_id: int = 0, client_id: str = "aiokafka"
    ) -> None:
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id
        )


class RequestHeader_v1(Struct):
    # Flexible response / request headers end in field buffer
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
        ("tags", TaggedFields),
    )

    def __init__(
        self,
        request: Request,
        correlation_id: int = 0,
        client_id: str = "aiokafka",
        tags: Optional[dict[int, bytes]] = None,
    ):
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id, tags or {}
        )


class ResponseHeader_v0(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
    )


class ResponseHeader_v1(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
        ("tags", TaggedFields),
    )


class Request(Struct, metaclass=abc.ABCMeta):
    FLEXIBLE_VERSION: ClassVar[bool] = False

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request"""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request version"""

    @property
    @abc.abstractmethod
    def RESPONSE_TYPE(self) -> type[Response]:
        """The Response class associated with the api request"""

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the request structure"""

    def expect_response(self) -> bool:
        """Override this method if an api request does not always generate a response"""
        return True

    def to_object(self) -> dict[str, Any]:
        return _to_object(self.SCHEMA, self)

    def build_request_header(
        self, correlation_id: int, client_id: str
    ) -> Union[RequestHeader_v0, RequestHeader_v1]:
        if self.FLEXIBLE_VERSION:
            return RequestHeader_v1(
                self, correlation_id=correlation_id, client_id=client_id
            )
        return RequestHeader_v0(
            self, correlation_id=correlation_id, client_id=client_id
        )

    def parse_response_header(
        self, read_buffer: Union[BytesIO, bytes]
    ) -> Union[ResponseHeader_v0, ResponseHeader_v1]:
        if self.FLEXIBLE_VERSION:
            return ResponseHeader_v1.decode(read_buffer)
        return ResponseHeader_v0.decode(read_buffer)


class Response(Struct, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the response structure"""

    def to_object(self) -> dict[str, Any]:
        return _to_object(self.SCHEMA, self)


def _to_object(schema: Schema, data: Union[Struct, dict[int, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for idx, (name, _type) in enumerate(zip(schema.names, schema.fields)):
        if isinstance(data, Struct):
            val = data.get_item(name)
        else:
            val = data[idx]

        if isinstance(_type, Schema):
            obj[name] = _to_object(_type, val)
        elif isinstance(_type, Array):
            if isinstance(_type.array_of, Schema):
                obj[name] = [_to_object(_type.array_of, x) for x in val]
            else:
                obj[name] = val
        else:
            obj[name] = val

    return obj
