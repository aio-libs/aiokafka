from io import BytesIO

from .abstract import AbstractType
from .types import Schema


class Struct(AbstractType):
    SCHEMA = Schema()

    def __init__(self, *args, **kwargs):
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError("Args must be empty or mirror schema")
        else:
            for name in self.SCHEMA.names:
                self.__dict__[name] = kwargs.pop(name, None)
            if kwargs:
                raise ValueError(
                    "Keyword(s) not in schema {}: {}".format(
                        list(self.SCHEMA.names), ", ".join(kwargs.keys())
                    )
                )

    def encode(self):
        return self.SCHEMA.encode([self.__dict__[name] for name in self.SCHEMA.names])

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*[field.decode(data) for field in cls.SCHEMA.fields])

    def get_item(self, name):
        if name not in self.SCHEMA.names:
            raise KeyError("%s is not in the schema" % name)
        return self.__dict__[name]

    def __repr__(self):
        key_vals = []
        for name, field in zip(self.SCHEMA.names, self.SCHEMA.fields):
            key_vals.append(f"{name}={field.repr(self.__dict__[name])}")
        return self.__class__.__name__ + "(" + ", ".join(key_vals) + ")"

    def __eq__(self, other):
        if self.SCHEMA != other.SCHEMA:
            return False
        for attr in self.SCHEMA.names:
            if self.__dict__[attr] != other.__dict__[attr]:
                return False
        return True
