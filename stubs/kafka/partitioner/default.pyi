from typing import Any

class DefaultPartitioner:
    @classmethod
    def __call__(cls, key: Any, all_partitions: Any, available: Any): ...

def murmur2(data: Any): ...
