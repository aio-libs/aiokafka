from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
import abc


class AbstractStaticPartitionAssignor(AbstractPartitionAssignor):
    """
    Abstract assignor implementation that also supports static assignments (KIP-345)
    """

    @abc.abstractmethod
    def assign(self, cluster, members,  # lgtm[py/inheritance/signature-mismatch]
               member_group_instance_ids):
        """Perform group assignment given cluster metadata, member subscriptions
           and group_instance_ids
        Arguments:
            cluster (ClusterMetadata): metadata for use in assignment
            members (dict of {member_id: MemberMetadata}): decoded metadata for
                each member in the group.
            member_group_instance_ids (dict of {member_id: MemberMetadata}):
                decoded metadata for each member in the group.
        Returns:
            dict: {member_id: MemberAssignment}
        """
        pass
