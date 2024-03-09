import struct

_SCHEMA = struct.Struct(">HH")


class ControlRecord:
    def __init__(self, version, type_):
        self._version = version
        self._type = type_

    @property
    def version(self):
        return self._version

    @property
    def type_(self):
        return self._type

    def __eq__(self, other):
        if isinstance(other, ControlRecord):
            return other._version == self._version and other._type == self._type
        return False

    @classmethod
    def parse(cls, data: bytes):
        version, type_ = _SCHEMA.unpack_from(data)
        return cls(version, type_)

    def __repr__(self):
        return f"ControlRecord(version={self._version}, type_={self._type})"


ABORT_MARKER = ControlRecord(0, 0)
COMMIT_MARKER = ControlRecord(0, 1)
