import asyncio
import collections
import logging

from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocol
from kafka.common import OffsetAndMetadata, TopicPartition
from kafka.protocol.commit import (
    OffsetCommitRequest_v2, OffsetCommitRequest_v1, OffsetCommitRequest_v0,
    OffsetFetchRequest_v0, OffsetFetchRequest_v1)
import kafka.common as Errors

from aiokafka.coordinator.base import BaseCoordinator

log = logging.getLogger(__name__)


class AIOConsumerCoordinator(BaseCoordinator):
    """
    This class manages the coordination process with the consumer coordinator.
    """
    def __init__(self, client, subscription, *, loop,
                 group_id='aiokafka-default-group',
                 session_timeout_ms=30000, heartbeat_interval_ms=3000,
                 retry_backoff_ms=100,
                 enable_auto_commit=True, auto_commit_interval_ms=5000,
                 assignors=(RoundRobinPartitionAssignor,),
                 api_version=(0, 9)):
        """Initialize the coordination manager.

        Parameters:
            client (AIOKafkaClient): kafka client
            subscription (SubscriptionState): instance of SubscriptionState
                located in kafka.consumer.subscription_state
            group_id (str): name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. Default: 'kafka-python-default-group'
            enable_auto_commit (bool): If true the consumer's offset will be
                periodically committed in the background. Default: True.
            auto_commit_interval_ms (int): milliseconds between automatic
                offset commits, if enable_auto_commit is True. Default: 5000.
            assignors (list): List of objects to use to distribute partition
                ownership amongst consumer instances when group management is
                used. Default: [RoundRobinPartitionAssignor]
            heartbeat_interval_ms (int): The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than session_timeout_ms, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances. Default: 3000
            session_timeout_ms (int): The timeout used to detect failures when
                using Kafka's group managementment facilities. Default: 30000
            retry_backoff_ms (int): Milliseconds to backoff when retrying on
                errors. Default: 100.
        """
        super(AIOConsumerCoordinator, self).__init__(
            client, loop=loop, group_id=group_id,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            retry_backoff_ms=retry_backoff_ms)

        self._api_version = api_version
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._assignors = assignors
        self._subscription = subscription
        self._partitions_per_topic = {}
        self._cluster = client.cluster
        self._cluster.request_update()
        self._cluster.add_listener(self._handle_metadata_update)
        self._auto_commit_task = None

        if self._api_version >= (0, 9) and self.group_id is not None:
            assert self._assignors, 'Coordinator requires assignors'
        if self._enable_auto_commit:
            if self._api_version < (0, 8, 1):
                log.warning(
                    'Broker version (%s) does not support offset'
                    ' commits; disabling auto-commit.', self._api_version)
            elif self.group_id is None:
                log.warning('group_id is None: disabling auto-commit.')
            else:
                interval = self._auto_commit_interval_ms / 1000.0
                self._auto_commit_task = asyncio.ensure_future(
                    self.auto_commit_routine(interval), loop=loop)

    @asyncio.coroutine
    def close(self):
        yield from self._maybe_auto_commit_offsets_sync()
        if self._auto_commit_task:
            self._auto_commit_task.cancel()
            try:
                yield from self._auto_commit_task
            except asyncio.CancelledError:
                pass
            self._auto_commit_task = None

        yield from super().close()

    def protocol_type(self):
        return ConsumerProtocol.PROTOCOL_TYPE

    def group_protocols(self):
        """Returns list of preferred (protocols, metadata)"""
        topics = self._subscription.subscription
        assert topics is not None, 'Consumer has not subscribed to topics'
        metadata_list = []
        for assignor in self._assignors:
            metadata = assignor.metadata(topics)
            group_protocol = (assignor.name, metadata)
            metadata_list.append(group_protocol)
        return metadata_list

    def _handle_metadata_update(self, cluster):
        if self._subscription.subscribed_pattern:
            topics = []
            for topic in cluster.topics():
                if self._subscription.subscribed_pattern.match(topic):
                    topics.append(topic)

            self._subscription.change_subscription(topics)
            self._client.set_topics(self._subscription.group_subscription())

        # check if there are any changes to the metadata which should trigger
        # a rebalance
        if self._subscription_metadata_changed():
            if (self._api_version >= (0, 9) and self.group_id is not None):
                self._subscription.mark_for_reassignment()
            # If we haven't got group coordinator support,
            # just assign all partitions locally
            else:
                self._subscription.assign_from_subscribed([
                    TopicPartition(topic, partition)
                    for topic in self._subscription.subscription
                    for partition in self._partitions_per_topic[topic]
                ])

    def _subscription_metadata_changed(self):
        if not self._subscription.partitions_auto_assigned():
            return False

        old_partitions_per_topic = self._partitions_per_topic
        self._partitions_per_topic = {}
        for topic in self._subscription.group_subscription():
            partitions = self._cluster.partitions_for_topic(topic) or []
            self._partitions_per_topic[topic] = set(partitions)

        if self._partitions_per_topic != old_partitions_per_topic:
            return True
        return False

    def _lookup_assignor(self, name):
        for assignor in self._assignors:
            if assignor.name == name:
                return assignor
        return None

    def _on_join_complete(self, generation, member_id, protocol,
                          member_assignment_bytes):
        assignor = self._lookup_assignor(protocol)
        assert assignor, 'invalid assignment protocol: %s' % protocol

        assignment = ConsumerProtocol.ASSIGNMENT.decode(
            member_assignment_bytes)

        # set the flag to refresh last committed offsets
        self._subscription.needs_fetch_committed_offsets = True

        # update partition assignment
        self._subscription.assign_from_subscribed(assignment.partitions())

        # give the assignor a chance to update internal state
        # based on the received assignment
        assignor.on_assignment(assignment)

        assigned = set(self._subscription.assigned_partitions())
        log.debug("Set newly assigned partitions %s", assigned)

        # execute the user's callback after rebalance
        if self._subscription.listener:
            try:
                self._subscription.listener.on_partitions_assigned(assigned)
            except Exception:
                log.exception("User provided listener failed on partition"
                              " assignment: %s", assigned)

    def _perform_assignment(self, leader_id, assignment_strategy, members):
        assignor = self._lookup_assignor(assignment_strategy)
        assert assignor, \
            'Invalid assignment protocol: %s' % assignment_strategy
        member_metadata = {}
        all_subscribed_topics = set()
        for member_id, metadata_bytes in members:
            metadata = ConsumerProtocol.METADATA.decode(metadata_bytes)
            member_metadata[member_id] = metadata
            all_subscribed_topics.update(metadata.subscription)

        # the leader will begin watching for changes to any of the topics
        # the group is interested in, which ensures that all metadata changes
        # will eventually be seen
        self._subscription.group_subscribe(all_subscribed_topics)
        self._client.set_topics(self._subscription.group_subscription())

        log.debug("Performing %s assignment for subscriptions %s",
                  assignor.name, member_metadata)

        assignments = assignor.assign(self._cluster, member_metadata)
        log.debug("Finished assignment: %s", assignments)

        group_assignment = {}
        for member_id, assignment in assignments.items():
            group_assignment[member_id] = assignment
        return group_assignment

    @asyncio.coroutine
    def _on_join_prepare(self, generation, member_id):
        self._subscription.mark_for_reassignment()

        # commit offsets prior to rebalance if auto-commit enabled
        yield from self._maybe_auto_commit_offsets_sync()

        # execute the user's callback before rebalance
        log.debug("Revoking previously assigned partitions %s",
                  self._subscription.assigned_partitions())
        if self._subscription.listener:
            try:
                revoked = set(self._subscription.assigned_partitions())
                self._subscription.listener.on_partitions_revoked(revoked)
            except Exception:
                log.exception("User provided subscription listener failed"
                              " on_partitions_revoked")

    def need_rejoin(self):
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        return (self._subscription.partitions_auto_assigned() and
                (super(AIOConsumerCoordinator, self).need_rejoin() or
                 self._subscription.needs_partition_assignment))

    @asyncio.coroutine
    def refresh_committed_offsets(self):
        """Fetch committed offsets for assigned partitions."""
        if self._subscription.needs_fetch_committed_offsets:
            offsets = yield from self.fetch_committed_offsets(
                self._subscription.assigned_partitions())

            for partition, offset in offsets.items():
                # verify assignment is still active
                if self._subscription.is_assigned(partition):
                    partition = self._subscription.assignment[partition]
                    partition.committed = offset.offset

            self._subscription.needs_fetch_committed_offsets = False

    @asyncio.coroutine
    def fetch_committed_offsets(self, partitions):
        """Fetch the current committed offsets for specified partitions

        Arguments:
            partitions (list of TopicPartition): partitions to fetch

        Returns:
            dict: {TopicPartition: OffsetAndMetadata}
        """
        if not partitions:
            return {}

        while True:
            if self._api_version >= (0, 8, 2):
                yield from self.ensure_coordinator_known()
                node_id = self.coordinator_id
            else:
                node_id = self._client.get_random_node()

            log.debug(
                "Fetching committed offsets for partitions: %s", partitions)
            # construct the request
            topic_partitions = collections.defaultdict(set)
            for tp in partitions:
                topic_partitions[tp.topic].add(tp.partition)

            if self._api_version >= (0, 8, 2):
                OffsetFetchRequest = OffsetFetchRequest_v1
            else:
                OffsetFetchRequest = OffsetFetchRequest_v0

            request = OffsetFetchRequest(
                self.group_id,
                list(topic_partitions.items())
            )

            try:
                offsets = yield from self._proc_offsets_fetch_request(
                    node_id, request)
            except Errors.KafkaError as err:
                if not err.retriable:
                    raise err
                else:
                    # wait backoff and try again
                    yield from asyncio.sleep(
                        self._retry_backoff_ms / 1000.0, loop=self.loop)
            else:
                return offsets

    @asyncio.coroutine
    def _proc_offsets_fetch_request(self, node_id, request):
        response = yield from self._send_req(node_id, request)
        offsets = {}
        for topic, partitions in response.topics:
            for partition, offset, metadata, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    error = error_type()
                    log.debug("Error fetching offset for %s: %s", tp, error)
                    if error_type is Errors.GroupLoadInProgressError:
                        # just retry
                        raise error
                    elif error_type is Errors.NotCoordinatorForGroupError:
                        # re-discover the coordinator and retry
                        self.coordinator_dead()
                        raise error
                    elif error_type in (Errors.UnknownMemberIdError,
                                        Errors.IllegalGenerationError):
                        # need to re-join group
                        self._subscription.mark_for_reassignment()
                        raise error
                    elif error_type is Errors.UnknownTopicOrPartitionError:
                        log.warning(
                            "OffsetFetchRequest -- unknown topic %s", topic)
                        continue
                    else:
                        log.error("Unknown error fetching offsets for %s: %s",
                                  tp, error)
                        raise error
                elif offset >= 0:
                    # record the position with the offset
                    # (-1 indicates no committed offset to fetch)
                    offsets[tp] = OffsetAndMetadata(offset, metadata)
                else:
                    log.debug(
                        "No committed offset for partition %s", tp)

        return offsets

    @asyncio.coroutine
    def commit_offsets(self, offsets):
        """Commit specific offsets asynchronously.

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit

        Raises error on failure
        """
        assert self._api_version >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), offsets))
        assert all(map(lambda v: isinstance(v, OffsetAndMetadata),
                       offsets.values()))

        self._subscription.needs_fetch_committed_offsets = True
        if not offsets:
            log.debug('No offsets to commit')
            return True

        if self._api_version >= (0, 8, 2):
            if (yield from self.coordinator_unknown()):
                raise Errors.GroupCoordinatorNotAvailableError()
            node_id = self.coordinator_id
        else:
            node_id = self._client.get_random_node()

        # create the offset commit request
        offset_data = collections.defaultdict(dict)
        for tp, offset in offsets.items():
            offset_data[tp.topic][tp.partition] = offset

        if self._api_version >= (0, 9):
            request = OffsetCommitRequest_v2(
                self.group_id,
                self.generation,
                self.member_id,
                OffsetCommitRequest_v2.DEFAULT_RETENTION_TIME,
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        offset.metadata
                    ) for partition, offset in partitions.items()]
                ) for topic, partitions in offset_data.items()]
            )
        elif self._api_version >= (0, 8, 2):
            request = OffsetCommitRequest_v1(
                self.group_id, -1, '',
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        -1,
                        offset.metadata
                    ) for partition, offset in partitions.items()]
                ) for topic, partitions in offset_data.items()]
            )
        elif self._api_version >= (0, 8, 1):
            request = OffsetCommitRequest_v0(
                self.group_id,
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        offset.metadata
                    ) for partition, offset in partitions.items()]
                ) for topic, partitions in offset_data.items()]
            )

        log.debug(
            "Sending offset-commit request with %s to %s", offsets, node_id)

        response = yield from self._send_req(node_id, request)

        unauthorized_topics = set()
        for topic, partitions in response.topics:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                offset = offsets[tp]

                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    log.debug(
                        "Committed offset %s for partition %s", offset, tp)
                    if self._subscription.is_assigned(tp):
                        partition = self._subscription.assignment[tp]
                        partition.committed = offset.offset
                elif error_type is Errors.GroupAuthorizationFailedError:
                    log.error("OffsetCommit failed for group %s - %s",
                              self.group_id, error_type.__name__)
                    raise error_type()
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(topic)
                elif error_type in (Errors.OffsetMetadataTooLargeError,
                                    Errors.InvalidCommitOffsetSizeError):
                    # raise the error to the user
                    log.info(
                        "OffsetCommit failed for group %s on partition %s"
                        " due to %s, will retry", self.group_id, tp,
                        error_type.__name__)
                    raise error_type()
                elif error_type is Errors.GroupLoadInProgressError:
                    # just retry
                    log.info(
                        "OffsetCommit failed for group %s because group is"
                        " initializing (%s), will retry", self.group_id,
                        error_type.__name__)
                    raise error_type()
                elif error_type in (Errors.GroupCoordinatorNotAvailableError,
                                    Errors.NotCoordinatorForGroupError,
                                    Errors.RequestTimedOutError):
                    log.info(
                        "OffsetCommit failed for group %s due to a"
                        " coordinator error (%s), will find new coordinator"
                        " and retry", self.group_id, error_type.__name__)
                    self.coordinator_dead()
                    raise error_type()
                elif error_type in (Errors.UnknownMemberIdError,
                                    Errors.IllegalGenerationError,
                                    Errors.RebalanceInProgressError):
                    # need to re-join group
                    error = error_type(self.group_id)
                    log.error(
                        "OffsetCommit failed for group %s due to group"
                        " error (%s), will rejoin", self.group_id, error)
                    self._subscription.mark_for_reassignment()
                    raise error
                else:
                    log.error(
                        "OffsetCommit failed for group %s on partition %s"
                        " with offset %s: %s", self.group_id, tp, offset,
                        error_type.__name__)
                    raise error_type()

        if unauthorized_topics:
            log.error("OffsetCommit failed for unauthorized topics %s",
                      unauthorized_topics)
            raise Errors.TopicAuthorizationFailedError(unauthorized_topics)

    @asyncio.coroutine
    def _maybe_auto_commit_offsets_sync(self):
        if self._api_version < (0, 8, 1) or (not self._enable_auto_commit):
            return

        yield from self.commit_offsets(
            self._subscription.all_consumed_offsets()
        )

    @asyncio.coroutine
    def auto_commit_routine(self, interval):
        while True:
            if (yield from self.coordinator_unknown()):
                log.debug(
                    "Cannot auto-commit offsets because the coordinator is"
                    " unknown, will retry after backoff")
                yield from asyncio.sleep(
                    self._retry_backoff_ms / 1000.0, loop=self.loop)
                continue

            # select offsets that should be committed
            offsets = {}
            for partition in self._subscription.assigned_partitions():
                tp = self._subscription.assignment[partition]
                if tp.position != tp.committed and tp.has_valid_position:
                    offsets[partition] = OffsetAndMetadata(tp.position, '')

            try:
                yield from self.commit_offsets(offsets)
            except Errors.KafkaError as error:
                if error.retriable:
                    log.debug("Failed to auto-commit offsets: %s, will retry"
                              " immediately", error)
                    continue
                else:
                    log.warning("Auto offset commit failed: %s", error)

            yield from asyncio.sleep(interval, loop=self.loop)
