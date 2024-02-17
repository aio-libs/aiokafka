from typing import Dict, List, Optional

from aiokafka.errors import IllegalArgumentError


class NewTopic:
    """A class for new topic creation
    Arguments:
        name (string): name of the topic
        num_partitions (int): number of partitions
            or -1 if replica_assignment has been specified
        replication_factor (int): replication factor or -1 if
            replica assignment is specified
        replica_assignments (dict of int: [int]): A mapping containing
            partition id and replicas to assign to it.
        topic_configs (dict of str: str): A mapping of config key
            and value for the topic.
    """

    def __init__(
        self,
        name: str,
        num_partitions: int,
        replication_factor: int,
        replica_assignments: Optional[Dict[int, List[int]]] = None,
        topic_configs: Optional[Dict[str, str]] = None,
    ) -> None:
        if not (
            (num_partitions == -1 or replication_factor == -1)
            ^ (replica_assignments is None)
        ):
            raise IllegalArgumentError(
                "either num_partitions/replication_factor or replica_assignment "
                "must be specified"
            )
        self.name: str = name
        self.num_partitions: int = num_partitions
        self.replication_factor: int = replication_factor
        self.replica_assignments: Dict[int, List[int]] = replica_assignments or {}
        self.topic_configs: Dict[str, str] = topic_configs or {}
