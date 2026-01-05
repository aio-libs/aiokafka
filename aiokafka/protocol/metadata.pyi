import abc
from .api import Request, Response

class MetadataResponse_v0(Response, metaclass=abc.ABCMeta):
    pass

class MetadataResponse_v1(Response, metaclass=abc.ABCMeta):
    pass

class MetadataResponse_v2(Response, metaclass=abc.ABCMeta):
    pass

class MetadataResponse_v3(Response, metaclass=abc.ABCMeta):
    pass

class MetadataResponse_v4(Response, metaclass=abc.ABCMeta):
    pass

class MetadataResponse_v5(Response, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v0(Request, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v1(Request, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v2(Request, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v3(Request, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v4(Request, metaclass=abc.ABCMeta):
    pass

class MetadataRequest_v5(Request, metaclass=abc.ABCMeta):
    """
    The v5 metadata request is the same as v4.
    An additional field for offline_replicas has been added to the v5 metadata response
    """

MetadataRequest = ...
MetadataResponse = ...
