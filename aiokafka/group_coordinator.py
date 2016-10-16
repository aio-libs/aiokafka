import asyncio
import collections
import logging

import kafka.common as Errors
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocol
from kafka.common import OffsetAndMetadata, TopicPartition
from kafka.protocol.commit import (
    GroupCoordinatorRequest_v0 as GroupCoordinatorRequest,
    OffsetCommitRequest_v2 as OffsetCommitRequest,
    OffsetFetchRequest_v1 as OffsetFetchRequest)
from kafka.protocol.group import (
    HeartbeatRequest_v0 as HeartbeatRequest,
    JoinGroupRequest_v0 as JoinGroupRequest,
    LeaveGroupRequest_v0 as LeaveGroupRequest,
    SyncGroupRequest_v0 as SyncGroupRequest)

from aiokafka import ensure_future

log = logging.getLogger(__name__)


class GroupCoordinator(object):
    """
    GroupCoordinator implements group management for single group member
    by interacting with a designated Kafka broker (the coordinator). Group
    semantics are provided by extending this class.

    From a high level, Kafka's group management protocol consists of the
    following sequence of actions:

    1. Group Registration: Group members register with the coordinator
       providing their own metadata
       (such as the set of topics they are interested in).

    2. Group/Leader Selection: The coordinator (one of Kafka nodes) select
       the members of the group and chooses one member (one of client's)
       as the leader.

    3. State Assignment: The leader receives metadata for all members and
       assigns partitions to them.

    4. Group Stabilization: Each member receives the state assigned by the
       leader and begins processing.
       Between each phase coordinator awaits all clients to respond. If some
       do not respond in time - it will revoke their membership
    """
    def __init__(self, client, subscription, *, loop,
                 group_id='aiokafka-default-group',
                 session_timeout_ms=30000, heartbeat_interval_ms=3000,
                 retry_backoff_ms=100,
                 enable_auto_commit=True, auto_commit_interval_ms=5000,
                 assignors=(RoundRobinPartitionAssignor,)
                 ):
        """Initialize the coordination manager.

        Parameters:
            client (AIOKafkaClient): kafka client
            subscription (SubscriptionState): instance of SubscriptionState
                located in kafka.consumer.subscription_state
            group_id (str): name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. Default: 'aiokafka-default-group'
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
        self._client = client
        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._retry_backoff_ms = retry_backoff_ms
        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.group_id = group_id
        self.coordinator_id = None
        self.rejoin_needed = True
        self.needs_join_prepare = True
        self.loop = loop
        # rejoin group can be called in parallel
        # (from consumer and from heartbeat task), so we need lock
        self._rejoin_lock = asyncio.Lock(loop=loop)
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._assignors = assignors
        self._subscription = subscription
        self._partitions_per_topic = {}
        self._cluster = client.cluster
        self._auto_commit_task = None
        # _closing future used as a signal to heartbeat task for finish ASAP
        self._closing = asyncio.Future(loop=loop)
        # update subscribe state usint currently known metadata
        self._handle_metadata_update(client.cluster)
        self._cluster.add_listener(self._handle_metadata_update)
        self._group_rebalanced_callback = None

        self.heartbeat_task = ensure_future(
            self._heartbeat_task_routine(), loop=loop)

        if self._enable_auto_commit:
            interval = self._auto_commit_interval_ms / 1000
            self._auto_commit_task = ensure_future(
                self.auto_commit_routine(interval), loop=loop)

    @asyncio.coroutine
    def close(self):
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
        self._closing.set_result(None)
        if self._auto_commit_task:
            yield from self._auto_commit_task
            self._auto_commit_task = None

        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                yield from self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None

        if not (yield from self.coordinator_unknown()) and self.generation > 0:
            # this is a minimal effort attempt to leave the group. we do not
            # attempt any resending if the request fails or times out.
            request = LeaveGroupRequest(self.group_id, self.member_id)
            try:
                yield from self._send_req(self.coordinator_id, request)
            except Errors.KafkaError as err:
                log.error("LeaveGroup request failed: %s", err)
            else:
                log.info("LeaveGroup request succeeded")

    def on_group_rebalanced(self, callback):
        self._group_rebalanced_callback = callback

    @asyncio.coroutine
    def _send_req(self, node_id, request):
        """send request to Kafka node and mark coordinator as `dead`
        in error case
        """
        try:
            resp = yield from self._client.send(node_id, request)
        except Errors.KafkaError as err:
            log.error(
                'Error sending %s to node %s [%s] -- marking coordinator dead',
                request.__class__.__name__, node_id, err)
            self.coordinator_dead()
            raise err
        else:
            if not hasattr(resp, 'error_code'):
                return resp
            error_type = Errors.for_code(resp.error_code)
            if error_type is Errors.NoError:
                return resp
            else:
                raise error_type()

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
            self._subscription.mark_for_reassignment()

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

        if self._group_rebalanced_callback:
            self._group_rebalanced_callback()

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
            yield from self.ensure_coordinator_known()
            node_id = self.coordinator_id

            log.debug(
                "Fetching committed offsets for partitions: %s", partitions)
            # construct the request
            topic_partitions = collections.defaultdict(set)
            for tp in partitions:
                topic_partitions[tp.topic].add(tp.partition)

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
                        self._retry_backoff_ms / 1000, loop=self.loop)
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
        self._subscription.needs_fetch_committed_offsets = True
        if not offsets:
            log.debug('No offsets to commit')
            return True

        if (yield from self.coordinator_unknown()):
            raise Errors.GroupCoordinatorNotAvailableError()
        node_id = self.coordinator_id

        # create the offset commit request
        offset_data = collections.defaultdict(list)
        for tp, offset in offsets.items():
            offset_data[tp.topic].append(
                (tp.partition,
                 offset.offset,
                 offset.metadata))

        request = OffsetCommitRequest(
            self.group_id,
            self.generation,
            self.member_id,
            OffsetCommitRequest.DEFAULT_RETENTION_TIME,
            [(topic, tp_offsets) for topic, tp_offsets in offset_data.items()]
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
        if not self._enable_auto_commit:
            return

        yield from self.ensure_coordinator_known()
        yield from self.commit_offsets(
            self._subscription.all_consumed_offsets())

    @asyncio.coroutine
    def auto_commit_routine(self, interval):
        while not self._closing.done():
            if (yield from self.coordinator_unknown()):
                log.debug(
                    "Cannot auto-commit offsets because the coordinator is"
                    " unknown, will retry after backoff")
                yield from asyncio.sleep(
                    self._retry_backoff_ms / 1000, loop=self.loop)
                continue

            yield from asyncio.wait(
                [self._closing], timeout=interval, loop=self.loop)

            # select offsets that should be committed
            offsets = {}
            for partition in self._subscription.assigned_partitions():
                tp = self._subscription.assignment[partition]
                if tp.position != tp.committed and tp.has_valid_position:
                    offsets[partition] = OffsetAndMetadata(tp.position, '')

            try:
                yield from self.commit_offsets(offsets)
            except Errors.KafkaError as error:
                if error.retriable and not self._closing.done():
                    log.debug("Failed to auto-commit offsets: %s, will retry"
                              " immediately", error)
                else:
                    log.warning("Auto offset commit failed: %s", error)

    @asyncio.coroutine
    def coordinator_unknown(self):
        """Check if we know who the coordinator is
        and have an active connection

        Returns:
            bool: True if the coordinator is unknown
        """
        if self.coordinator_id is None:
            return True

        ready = yield from self._client.ready(self.coordinator_id)
        return not ready

    @asyncio.coroutine
    def ensure_coordinator_known(self):
        """Block until the coordinator for this group is known
        (and we have an active connection -- Java client uses unsent queue).
        """
        while (yield from self.coordinator_unknown()):
            node_id = self._client.get_random_node()
            if node_id is None or not (yield from self._client.ready(node_id)):
                raise Errors.NoBrokersAvailable()

            log.debug("Issuing group metadata request to broker %s", node_id)
            request = GroupCoordinatorRequest(self.group_id)
            try:
                resp = yield from self._send_req(node_id, request)
            except Errors.KafkaError as err:
                log.error("Group Coordinator Request failed: %s", err)
                yield from asyncio.sleep(
                    self._retry_backoff_ms / 1000, loop=self.loop)
                if err.retriable is True:
                    yield from self._client.force_metadata_update()
            else:
                log.debug("Group metadata response %s", resp)
                if not (yield from self.coordinator_unknown()):
                    # We already found the coordinator, so ignore the response
                    log.debug("Coordinator already known, ignoring response")
                    break
                self.coordinator_id = resp.coordinator_id

    def need_rejoin(self):
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        return (self._subscription.partitions_auto_assigned() and
                (self.rejoin_needed or
                 self._subscription.needs_partition_assignment))

    @asyncio.coroutine
    def ensure_active_group(self):
        """Ensure that the group is active (i.e. joined and synced)"""
        with (yield from self._rejoin_lock):
            if not self.need_rejoin():
                return

            if self.needs_join_prepare:
                yield from self._on_join_prepare(
                    self.generation, self.member_id)
                self.needs_join_prepare = False

            while self.need_rejoin():
                yield from self.ensure_coordinator_known()

                yield from self._perform_group_join()

    @asyncio.coroutine
    def _perform_group_join(self):
        """Join the group and return the assignment for the next generation.

        This function handles both JoinGroup and SyncGroup, delegating to
        _perform_assignment() if elected leader by the coordinator.

        Returns:
            Future: resolves to the encoded-bytes assignment returned from the
                group leader
        """
        # send a join group request to the coordinator
        log.debug("(Re-)joining group %s", self.group_id)

        topics = self._subscription.subscription
        assert topics is not None, 'Consumer has not subscribed to topics'
        metadata_list = []
        for assignor in self._assignors:
            metadata = assignor.metadata(topics)
            if not isinstance(metadata, bytes):
                metadata = metadata.encode()
            group_protocol = (assignor.name, metadata)
            metadata_list.append(group_protocol)

        request = JoinGroupRequest(
            self.group_id,
            self._session_timeout_ms,
            self.member_id,
            ConsumerProtocol.PROTOCOL_TYPE,
            metadata_list)

        # create the request for the coordinator
        log.debug("Issuing request (%s) to coordinator %s",
                  request, self.coordinator_id)
        try:
            response = yield from self._send_req(self.coordinator_id, request)
        except Errors.GroupLoadInProgressError:
            log.debug("Attempt to join group %s rejected since coordinator is"
                      " loading the group.", self.group_id)
        except Errors.UnknownMemberIdError:
            # reset the member id and retry immediately
            self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
            log.info(
                "Attempt to join group %s failed due to unknown member id,"
                " resetting and retrying.", self.group_id)
            return
        except (Errors.GroupCoordinatorNotAvailableError,
                Errors.NotCoordinatorForGroupError):
            # re-discover the coordinator and retry with backoff
            self.coordinator_dead()
            log.info("Attempt to join group %s failed due to obsolete "
                     "coordinator information, retrying.", self.group_id)
        except Errors.KafkaError as err:
            log.error(
                "Error in join group '%s' response: %s", self.group_id, err)
        else:
            log.debug("Join group response %s", response)
            self.member_id = response.member_id
            self.generation = response.generation_id
            self.rejoin_needed = False
            self.protocol = response.group_protocol
            log.info("Joined group '%s' (generation %s) with member_id %s",
                     self.group_id, self.generation, self.member_id)

            if response.leader_id == response.member_id:
                log.info("Elected group leader -- performing partition"
                         " assignments using %s", self.protocol)
                cor = self._on_join_leader(response)
            else:
                cor = self._on_join_follower()

            try:
                member_assignment_bytes = yield from cor
            except (Errors.UnknownMemberIdError,
                    Errors.RebalanceInProgressError,
                    Errors.IllegalGenerationError):
                pass
            except Errors.KafkaError as err:
                if err.retriable is False:
                    raise err
            else:
                self._on_join_complete(self.generation, self.member_id,
                                       self.protocol, member_assignment_bytes)
                self.needs_join_prepare = True
                return

        # backoff wait - failure case
        yield from asyncio.sleep(
            self._retry_backoff_ms / 1000, loop=self.loop)

    @asyncio.coroutine
    def _on_join_follower(self):
        # send follower's sync group with an empty assignment
        request = SyncGroupRequest(
            self.group_id,
            self.generation,
            self.member_id,
            {})
        log.debug("Issuing follower SyncGroup (%s) to coordinator %s",
                  request, self.coordinator_id)
        return (yield from self._send_sync_group_request(request))

    @asyncio.coroutine
    def _on_join_leader(self, response):
        """
        Perform leader synchronization and send back the assignment
        for the group via SyncGroupRequest

        Arguments:
            response (JoinResponse): broker response to parse

        Returns:
            Future: resolves to member assignment encoded-bytes
        """
        try:
            group_assignment = self._perform_assignment(
                response.leader_id,
                response.group_protocol,
                response.members)
        except Exception as e:
            raise Errors.KafkaError(str(e))

        assignment_req = []
        for member_id, assignment in group_assignment.items():
            if not isinstance(assignment, bytes):
                assignment = assignment.encode()
            assignment_req.append((member_id, assignment))

        request = SyncGroupRequest(
            self.group_id,
            self.generation,
            self.member_id,
            assignment_req)

        log.debug("Issuing leader SyncGroup (%s) to coordinator %s",
                  request, self.coordinator_id)
        return (yield from self._send_sync_group_request(request))

    @asyncio.coroutine
    def _send_sync_group_request(self, request):
        if (yield from self.coordinator_unknown()):
            raise Errors.GroupCoordinatorNotAvailableError()

        response = None
        try:
            response = yield from self._send_req(self.coordinator_id, request)
            log.debug(
                "Received successful sync group response for group %s: %s",
                self.group_id, response)
            return response.member_assignment
        except Errors.RebalanceInProgressError as err:
            log.info("SyncGroup for group %s failed due to coordinator"
                     " rebalance, rejoining the group", self.group_id)
            raise err
        except (Errors.UnknownMemberIdError,
                Errors.IllegalGenerationError) as err:
            log.info("SyncGroup for group %s failed due to %s,"
                     " rejoining the group", self.group_id, err)
            self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
            raise err
        except (Errors.GroupCoordinatorNotAvailableError,
                Errors.NotCoordinatorForGroupError) as err:
            log.info("SyncGroup for group %s failed due to %s, will find new"
                     " coordinator and rejoin", self.group_id, err)
            self.coordinator_dead()
            raise err
        except Errors.KafkaError as err:
            log.error("Error from SyncGroup: %s", err)
            raise err
        finally:
            if response is None:
                # Always rejoin on error
                self.rejoin_needed = True

    def coordinator_dead(self, error=None):
        """Mark the current coordinator as dead."""
        if self.coordinator_id is not None:
            log.info("Marking the coordinator dead (node %s): %s.",
                     self.coordinator_id, error)
            self.coordinator_id = None
        self.rejoin_needed = True

    @asyncio.coroutine
    def _heartbeat_task_routine(self):
        last_ok_heartbeat = self.loop.time()
        hb_interval = self._heartbeat_interval_ms / 1000
        session_timeout = self._session_timeout_ms / 1000
        retry_backoff_time = self._retry_backoff_ms / 1000
        sleep_time = hb_interval

        while True:
            yield from asyncio.sleep(sleep_time, loop=self.loop)

            try:
                yield from self.ensure_active_group()
            except Errors.KafkaError as err:
                log.error("Skipping heartbeat: no active group: %s", err)
                last_ok_heartbeat = self.loop.time()
                sleep_time = retry_backoff_time
                continue

            t0 = self.loop.time()
            request = HeartbeatRequest(
                self.group_id, self.generation, self.member_id)
            log.debug("Heartbeat: %s[%s] %s",
                      self.group_id, self.generation, self.member_id)

            try:
                yield from self._send_req(self.coordinator_id, request)
            except (Errors.GroupCoordinatorNotAvailableError,
                    Errors.NotCoordinatorForGroupError):
                log.info(
                    "Heartbeat failed: coordinator is either not started or"
                    " not valid; will refresh metadata and retry")
                self.coordinator_dead()
            except Errors.RebalanceInProgressError:
                log.info(
                    "Heartbeat failed: group is rebalancing; re-joining group")
                self.rejoin_needed = True
            except Errors.IllegalGenerationError:
                log.info(
                    "Heartbeat failed: local generation id is not current;"
                    " re-joining group")
                self.rejoin_needed = True
            except Errors.UnknownMemberIdError:
                log.info(
                    "Heartbeat failed: local member_id was not recognized;"
                    " resetting and re-joining group")
                self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
                self.rejoin_needed = True
            except Errors.KafkaError as err:
                log.error("Heartbeat failed: %s", err)
            else:
                log.debug("Received successful heartbeat response.")
                last_ok_heartbeat = self.loop.time()

            if self.rejoin_needed:
                sleep_time = retry_backoff_time
            else:
                sleep_time = max((0, hb_interval - self.loop.time() + t0))
            session_time = self.loop.time() - last_ok_heartbeat
            if session_time > session_timeout:
                # we haven't received a successful heartbeat in one session
                # interval so mark the coordinator dead
                log.error(
                    "Heartbeat session expired - marking coordinator dead")
                self.coordinator_dead()
                continue
