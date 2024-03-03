import abc


class AbstractType:
    __metaclass__ = abc.ABCMeta

    @classmethod
    @abc.abstractmethod
    def encode(cls, value): ...

    @classmethod
    @abc.abstractmethod
    def decode(cls, data): ...

    @classmethod
    def repr(cls, value):
        return repr(value)
