import abc

from .measurable import AbstractMeasurable
from .stat import AbstractStat


class AbstractMeasurableStat(AbstractStat, AbstractMeasurable, metaclass=abc.ABCMeta):
    """
    An AbstractMeasurableStat is an AbstractStat that is also
    an AbstractMeasurable (i.e. can produce a single floating point value).
    This is the interface used for most of the simple statistics such
    as Avg, Max, Count, etc.
    """
