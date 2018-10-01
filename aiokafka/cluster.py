from kafka.cluster import ClusterMetadata as BaseClusterMetadata
from aiokafka.structs import BrokerMetadata


class ClusterMetadata(BaseClusterMetadata):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._coordinators = {}
        self._coordinator_by_key = {}

    def coordinator_metadata(self, node_id):
        return self._coordinators.get(node_id)

    def add_coordinator(self, node_id, host, port, rack=None, *, purpose):
        """ Keep track of all coordinator nodes separately and remove them if
        a new one was elected for the same purpose (For example group
        coordinator for group X).
        """
        if purpose in self._coordinator_by_key:
            old_id = self._coordinator_by_key.pop(purpose)
            del self._coordinators[old_id]

        self._coordinators[node_id] = BrokerMetadata(node_id, host, port, rack)
        self._coordinator_by_key[purpose] = node_id
