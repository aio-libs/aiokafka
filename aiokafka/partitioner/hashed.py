from __future__ import absolute_import

from kafka.partitioner.base import Partitioner

from .murmur2 import murmur2


class Murmur2Partitioner(Partitioner):
    """
    Implements a partitioner which selects the target partition based on
    the hash of the key. Attempts to apply the same hashing
    function as mainline java client.
    """
    def __call__(self, key, partitions=None, available=None):
        if available:
            return self.partition(key, available)
        return self.partition(key, partitions)

    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions

        # https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/Partitioner.java#L69
        idx = (murmur2(key) & 0x7fffffff) % len(partitions)

        return partitions[idx]


class LegacyPartitioner(object):
    """DEPRECATED -- See Issue 374

    Implements a partitioner which selects the target partition based on
    the hash of the key
    """
    def __init__(self, partitions):
        self.partitions = partitions

    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        size = len(partitions)
        idx = hash(key) % size

        return partitions[idx]


# Default will change to Murmur2 in 0.10 release
HashedPartitioner = LegacyPartitioner
