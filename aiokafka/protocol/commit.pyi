from .api import Request, Response
import abc

class OffsetCommitResponse_v0(Response, metaclass=abc.ABCMeta):
    pass

class OffsetCommitResponse_v1(Response, metaclass=abc.ABCMeta):
    pass

class OffsetCommitResponse_v2(Response, metaclass=abc.ABCMeta):
    pass

class OffsetCommitResponse_v3(Response, metaclass=abc.ABCMeta):
    pass

class OffsetCommitRequest_v0(Request, metaclass=abc.ABCMeta):
    pass

class OffsetCommitRequest_v1(Request, metaclass=abc.ABCMeta):
    pass

class OffsetCommitRequest_v2(Request, metaclass=abc.ABCMeta):
    DEFAULT_GENERATION_ID = ...
    DEFAULT_RETENTION_TIME = ...

class OffsetCommitRequest_v3(Request, metaclass=abc.ABCMeta):
    pass

OffsetCommitRequest = ...
OffsetCommitResponse = ...

class OffsetFetchResponse_v0(Response, metaclass=abc.ABCMeta):
    pass

class OffsetFetchResponse_v1(Response, metaclass=abc.ABCMeta):
    pass

class OffsetFetchResponse_v2(Response, metaclass=abc.ABCMeta):
    pass

class OffsetFetchResponse_v3(Response, metaclass=abc.ABCMeta):
    pass

class OffsetFetchRequest_v0(Request, metaclass=abc.ABCMeta):
    pass

class OffsetFetchRequest_v1(Request, metaclass=abc.ABCMeta):
    pass

class OffsetFetchRequest_v2(Request, metaclass=abc.ABCMeta):
    pass

class OffsetFetchRequest_v3(Request, metaclass=abc.ABCMeta):
    pass

OffsetFetchRequest = ...
OffsetFetchResponse = ...

class GroupCoordinatorResponse_v0(Response, metaclass=abc.ABCMeta):
    pass

class GroupCoordinatorResponse_v1(Response, metaclass=abc.ABCMeta):
    pass

class GroupCoordinatorRequest_v0(Request, metaclass=abc.ABCMeta):
    pass

class GroupCoordinatorRequest_v1(Request, metaclass=abc.ABCMeta):
    pass

GroupCoordinatorRequest = ...
GroupCoordinatorResponse = ...
