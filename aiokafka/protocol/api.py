import abc

from .struct import Struct
from .types import Array, Int16, Int32, Schema, String, TaggedFields


class RequestHeader_v0(Struct):
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
    )

    def __init__(self, request, correlation_id=0, client_id="aiokafka"):
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

    def __init__(self, request, correlation_id=0, client_id="aiokafka", tags=None):
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


class Request(Struct):
    __metaclass__ = abc.ABCMeta

    FLEXIBLE_VERSION = False

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request"""

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request version"""

    @abc.abstractproperty
    def SCHEMA(self):
        """An instance of Schema() representing the request structure"""

    @abc.abstractproperty
    def RESPONSE_TYPE(self):
        """The Response class associated with the api request"""

    def expect_response(self):
        """Override this method if an api request does not always generate a response"""
        return True

    def to_object(self):
        return _to_object(self.SCHEMA, self)

    def build_request_header(self, correlation_id, client_id):
        if self.FLEXIBLE_VERSION:
            return RequestHeader_v1(
                self, correlation_id=correlation_id, client_id=client_id
            )
        return RequestHeader_v0(
            self, correlation_id=correlation_id, client_id=client_id
        )

    def parse_response_header(self, read_buffer):
        if self.FLEXIBLE_VERSION:
            return ResponseHeader_v1.decode(read_buffer)
        return ResponseHeader_v0.decode(read_buffer)


class Response(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request/response"""

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request/response version"""

    @abc.abstractproperty
    def SCHEMA(self):
        """An instance of Schema() representing the response structure"""

    def to_object(self):
        return _to_object(self.SCHEMA, self)


def _to_object(schema, data):
    obj = {}
    for idx, (name, _type) in enumerate(zip(schema.names, schema.fields)):
        if isinstance(data, Struct):
            val = data.get_item(name)
        else:
            val = data[idx]

        if isinstance(_type, Schema):
            obj[name] = _to_object(_type, val)
        elif isinstance(_type, Array):
            if isinstance(_type.array_of, (Array, Schema)):
                obj[name] = [_to_object(_type.array_of, x) for x in val]
            else:
                obj[name] = val
        else:
            obj[name] = val

    return obj
