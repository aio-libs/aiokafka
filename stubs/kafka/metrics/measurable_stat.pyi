import abc
from kafka.metrics.measurable import AbstractMeasurable as AbstractMeasurable
from kafka.metrics.stat import AbstractStat as AbstractStat
from typing import Any

class AbstractMeasurableStat(AbstractStat, AbstractMeasurable, metaclass=abc.ABCMeta):
    __metaclass__: Any = ...
