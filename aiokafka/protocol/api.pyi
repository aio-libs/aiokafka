import abc
from io import BytesIO
from typing import Any, ClassVar, Dict, Optional, Type, Union
from .struct import Struct
from .types import Schema

class RequestHeader_v0(Struct):
    SCHEMA = ...
    def __init__(self, request: Request, correlation_id: int = ..., client_id: str = ...) -> None:
        ...
    


class RequestHeader_v1(Struct):
    SCHEMA = ...
    def __init__(self, request: Request, correlation_id: int = ..., client_id: str = ..., tags: Optional[Dict[int, bytes]] = ...) -> None:
        ...
    


class ResponseHeader_v0(Struct):
    SCHEMA = ...


class ResponseHeader_v1(Struct):
    SCHEMA = ...


class Request(Struct, metaclass=abc.ABCMeta):
    FLEXIBLE_VERSION: ClassVar[bool] = ...
    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request"""
        ...
    
    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request version"""
        ...
    
    @property
    @abc.abstractmethod
    def RESPONSE_TYPE(self) -> Type[Response]:
        """The Response class associated with the api request"""
        ...
    
    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the request structure"""
        ...
    
    def expect_response(self) -> bool:
        """Override this method if an api request does not always generate a response"""
        ...
    
    def to_object(self) -> Dict[str, Any]:
        ...
    
    def build_request_header(self, correlation_id: int, client_id: str) -> Union[RequestHeader_v0, RequestHeader_v1]:
        ...
    
    def parse_response_header(self, read_buffer: Union[BytesIO, bytes]) -> Union[ResponseHeader_v0, ResponseHeader_v1]:
        ...
    


class Response(Struct, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""
        ...
    
    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""
        ...
    
    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the response structure"""
        ...
    
    def to_object(self) -> Dict[str, Any]:
        ...
    


