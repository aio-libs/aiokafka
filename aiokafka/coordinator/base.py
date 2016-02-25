import abc
import logging
import time
import asyncio

import kafka.common as Errors
from kafka.protocol.commit import (
    GroupCoordinatorRequest,
    OffsetCommitRequest_v2 as OffsetCommitRequest)
from kafka.protocol.group import (HeartbeatRequest, JoinGroupRequest,
                                  LeaveGroupRequest, SyncGroupRequest)

log = logging.getLogger('kafka.coordinator')


class BaseCoordinator(object):
    """
    BaseCoordinator implements group management for a single group member
    by interacting with a designated Kafka broker (the coordinator). Group
    semantics are provided by extending this class.  See ConsumerCoordinator
    for example usage.

    From a high level, Kafka's group management protocol consists of the
    following sequence of actions:

    1. Group Registration: Group members register with the coordinator
       providing their own metadata
       (such as the set of topics they are interested in).

    2. Group/Leader Selection: The coordinator select the members of the group
       and chooses one member as the leader.

    3. State Assignment: The leader collects the metadata from all the members
       of the group and assigns state.

    4. Group Stabilization: Each member receives the state assigned by the
       leader and begins processing.

    To leverage this protocol, an implementation must define the format of
    metadata provided by each member for group registration in
    group_protocols() and the format of the state assignment provided by
    the leader in _perform_assignment() and which becomes available to members
    in _on_join_complete().
    """
    def __init__(self, client, *, loop, group_id='aiokafka-default-group',
                 session_timeout_ms=30000, heartbeat_interval_ms=3000,
                 retry_backoff_ms=100):
        """
        Keyword Arguments:
            group_id (str): name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. Default: 'kafka-python-default-group'
            session_timeout_ms (int): The timeout used to detect failures when
                using Kafka's group managementment facilities. Default: 30000
            heartbeat_interval_ms (int): The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than session_timeout_ms, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances. Default: 3000
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
        self.heartbeat_task = asyncio.ensure_future(
            self._heartbeat_task_routine(), loop=self.loop)
        # rejoin group can be called in parallel
        # (from consumer and from heartbeat task), so we need lock
        self._rejoin_lock = asyncio.Lock(loop=self.loop)

    @abc.abstractmethod
    def protocol_type(self):
        """
        Unique identifier for the class of protocols implements
        (e.g. "consumer" or "connect").

        Returns:
            str: protocol type name
        """
        pass

    @abc.abstractmethod
    def group_protocols(self):
        """Return the list of supported group protocols and metadata.

        This list is submitted by each group member via a JoinGroupRequest.
        The order of the protocols in the list indicates the preference of the
        protocol (the first entry is the most preferred). The coordinator takes
        this preference into account when selecting the generation protocol
        (generally more preferred protocols will be selected as long as all
        members support them and there is no disagreement on the preference).

        Note: metadata must be type bytes or support an encode() method

        Returns:
            list: [(protocol, metadata), ...]
        """
        pass

    @abc.abstractmethod
    def _on_join_prepare(self, generation, member_id):
        """Invoked prior to each group join or rejoin.

        This is typically used to perform any cleanup from the previous
        generation (such as committing offsets for the consumer)

        Arguments:
            generation (int): The previous generation or -1 if there was none
            member_id (str): The identifier of this member in the previous
                group or '' if there was none
        """
        pass

    @abc.abstractmethod
    def _perform_assignment(self, leader_id, protocol, members):
        """Perform assignment for the group.

        This is used by the leader to push state to all the members of the
        group (e.g. to push partition assignments in the case of the new
        consumer)

        Arguments:
            leader_id (str): The id of the leader (which is this member)
            protocol (str): the chosen group protocol (assignment strategy)
            members (list): [(member_id, metadata_bytes)] from
                JoinGroupResponse. metadata_bytes are associated with the
                chosen group protocol, and the Coordinator subclass is
                responsible for decoding metadata_bytes based on that protocol

        Returns:
            dict: {member_id: assignment}; assignment must either be bytes
                or have an encode() method to convert to bytes
        """
        pass

    @abc.abstractmethod
    def _on_join_complete(self, generation, member_id, protocol,
                          member_assignment_bytes):
        """Invoked when a group member has successfully joined a group.

        Arguments:
            generation (int): the generation that was joined
            member_id (str): the identifier for the local member in the group
            protocol (str): the protocol selected by the coordinator
            member_assignment_bytes (bytes): the protocol-encoded assignment
                propagated from the group leader. The Coordinator instance is
                responsible for decoding based on the chosen protocol.
        """
        pass

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

    @asyncio.coroutine
    def coordinator_unknown(self):
        """Check if we know who the coordinator is
        and have an active connection

        Side-effect: reset coordinator_id to None if connection failed

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
        (and we have an active connection -- java client uses unsent queue).
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
                    self._retry_backoff_ms / 1000.0, loop=self.loop)
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
        (e.g. if metadata changes)

        Returns:
            bool: True if it should, False otherwise
        """
        return self.rejoin_needed

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
        request = JoinGroupRequest(
            self.group_id,
            self._session_timeout_ms,
            self.member_id,
            self.protocol_type(),
            [(protocol,
              metadata if isinstance(metadata, bytes) else metadata.encode())
             for protocol, metadata in self.group_protocols()])

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
            self._retry_backoff_ms / 1000.0, loop=self.loop)

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
        return self._send_sync_group_request(request)

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
        return self._send_sync_group_request(request)

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
    def close(self):
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
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

        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.rejoin_needed = True

    @asyncio.coroutine
    def _heartbeat_task_routine(self):
        last_ok_heartbeat = time.time()
        hb_interval = self._heartbeat_interval_ms / 1000
        session_timeout = self._session_timeout_ms / 1000
        retry_backoff_time = self._retry_backoff_ms / 1000
        sleep_time = hb_interval

        while True:
            yield from asyncio.sleep(sleep_time, loop=self.loop)

            try:
                yield from self.ensure_active_group()
            except Errors.KafkaError as err:
                log.debug("Skipping heartbeat: no active group: %s", err)
                last_ok_heartbeat = time.time()
                sleep_time = retry_backoff_time
                continue
            else:
                sleep_time = hb_interval

            t0 = time.time()
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
                last_ok_heartbeat = time.time()

            if self.rejoin_needed:
                sleep_time = retry_backoff_time
            else:
                sleep_time = max((0, hb_interval - time.time() + t0))
            session_time = time.time() - last_ok_heartbeat
            if session_time > session_timeout:
                # we haven't received a successful heartbeat in one session
                # interval so mark the coordinator dead
                log.error(
                    "Heartbeat session expired - marking coordinator dead")
                self.coordinator_dead()
                continue
