import asyncio
import re
from unittest import mock

from kafka.protocol.group import (
    JoinGroupRequest_v0 as JoinGroupRequest,
    SyncGroupResponse_v0 as SyncGroupResponse,
    LeaveGroupRequest_v0 as LeaveGroupRequest,
    HeartbeatRequest_v0 as HeartbeatRequest,
)
from kafka.protocol.commit import (
    OffsetCommitRequest, OffsetCommitResponse_v2,
    OffsetFetchRequest_v1 as OffsetFetchRequest
)
import kafka.common as Errors

from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka import ConsumerRebalanceListener
from aiokafka.producer import AIOKafkaProducer
from aiokafka.client import AIOKafkaClient
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.consumer.group_coordinator import (
    GroupCoordinator, CoordinatorGroupRebalance)
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.util import create_future, ensure_future

UNKNOWN_MEMBER_ID = JoinGroupRequest.UNKNOWN_MEMBER_ID


class RebalanceListenerForTest(ConsumerRebalanceListener):
    def __init__(self):
        self.revoked = []
        self.assigned = []

    def on_partitions_revoked(self, revoked):
        self.revoked.append(revoked)
        raise Exception("coordinator should ignore this exception")

    def on_partitions_assigned(self, assigned):
        self.assigned.append(assigned)
        raise Exception("coordinator should ignore this exception")


class TestKafkaCoordinatorIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    def test_coordinator_workflow(self):
        # Check if 2 coordinators will coordinate rebalances correctly

        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')

        # Check if the initial group join is performed correctly with minimal
        # setup
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1', 'topic2']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            session_timeout_ms=10000,
            heartbeat_interval_ms=500,
            retry_backoff_ms=100)

        self.assertEqual(coordinator.coordinator_id, None)
        self.assertTrue(coordinator.need_rejoin(subscription.subscription))

        yield from coordinator.ensure_coordinator_known()
        self.assertNotEqual(coordinator.coordinator_id, None)

        if subscription.subscription.assignment is None:
            yield from subscription.wait_for_assignment()
        self.assertNotEqual(coordinator.coordinator_id, None)
        self.assertFalse(coordinator.need_rejoin(subscription.subscription))

        tp_list = subscription.assigned_partitions()
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))

        # Check if adding an additional coordinator will rebalance correctly
        client2 = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client2.bootstrap()
        subscription2 = SubscriptionState(loop=self.loop)
        subscription2.subscribe(topics=set(['topic1', 'topic2']))
        coordinator2 = GroupCoordinator(
            client2, subscription2, loop=self.loop,
            session_timeout_ms=10000,
            heartbeat_interval_ms=500,
            retry_backoff_ms=100)
        yield from asyncio.gather(
            subscription.wait_for_assignment(),
            subscription2.wait_for_assignment()
        )

        tp_list = subscription.assigned_partitions()
        self.assertEqual(len(tp_list), 2)
        tp_list2 = subscription2.assigned_partitions()
        self.assertEqual(len(tp_list2), 2)
        tp_list |= tp_list2
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))

        # Check is closing the first coordinator will rebalance the second
        yield from coordinator.close()
        yield from client.close()

        yield from subscription2.wait_for_assignment()
        tp_list = subscription2.assigned_partitions()
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))

        yield from coordinator2.close()
        yield from client2.close()

    @run_until_complete
    def test_failed_group_join(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        self.add_cleanup(client.close)

        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            retry_backoff_ms=10)
        coordinator._coordination_task.cancel()  # disable for test
        try:
            yield from coordinator._coordination_task
        except asyncio.CancelledError:
            pass
        coordinator._coordination_task = asyncio.sleep(0.1, loop=self.loop)
        coordinator.coordinator_id = 15
        self.add_cleanup(coordinator.close)

        @asyncio.coroutine
        def do_rebalance():
            rebalance = CoordinatorGroupRebalance(
                coordinator, coordinator.group_id, coordinator.coordinator_id,
                subscription.subscription, coordinator._assignors,
                coordinator._session_timeout_ms,
                coordinator._retry_backoff_ms,
                loop=self.loop)
            yield from rebalance.perform_group_join()

        mocked = mock.MagicMock()
        coordinator._client = mocked
        error_type = None

        @asyncio.coroutine
        def send(*agrs, **kw):
            resp = JoinGroupRequest.RESPONSE_TYPE(
                error_code=error_type.errno,
                generation_id=-1,  # generation_id
                group_protocol="roundrobin",
                leader_id="111",  # leader_id
                member_id="111",  # member_id
                members=[]
            )
            return resp

        mocked.send.side_effect = send
        subsc = subscription.subscription

        # no exception expected, just wait
        error_type = Errors.GroupLoadInProgressError
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(subsc), True)

        error_type = Errors.InvalidGroupIdError
        with self.assertRaises(Errors.InvalidGroupIdError):
            yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(subsc), True)

        # no exception expected, member_id should be reseted
        coordinator.member_id = 'some_invalid_member_id'
        error_type = Errors.UnknownMemberIdError
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(subsc), True)
        self.assertEqual(
            coordinator.member_id, JoinGroupRequest.UNKNOWN_MEMBER_ID)

        error_type = Errors.UnknownError()
        with self.assertRaises(Errors.KafkaError):  # Masked as unknown error
            yield from do_rebalance()

        # no exception expected, coordinator_id should be reseted
        error_type = Errors.GroupCoordinatorNotAvailableError
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(subsc), True)
        self.assertEqual(coordinator.coordinator_id, None)

    @run_until_complete
    def test_failed_sync_group(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            heartbeat_interval_ms=20000)
        coordinator._coordination_task.cancel()  # disable for test
        try:
            yield from coordinator._coordination_task
        except asyncio.CancelledError:
            pass
        coordinator._coordination_task = asyncio.sleep(0.1, loop=self.loop)
        coordinator.coordinator_id = 15
        self.add_cleanup(coordinator.close)

        @asyncio.coroutine
        def do_sync_group():
            rebalance = CoordinatorGroupRebalance(
                coordinator, coordinator.group_id, coordinator.coordinator_id,
                subscription.subscription, coordinator._assignors,
                coordinator._session_timeout_ms,
                coordinator._retry_backoff_ms,
                loop=self.loop)
            yield from rebalance._on_join_follower()

        mocked = mock.MagicMock()
        coordinator._client = mocked
        subsc = subscription.subscription
        error_type = None

        @asyncio.coroutine
        def send(*agrs, **kw):
            resp = SyncGroupResponse(
                error_code=error_type.errno,
                member_assignment=b"123"
            )
            return resp

        mocked.send.side_effect = send

        coordinator.member_id = 'some_invalid_member_id'

        error_type = Errors.RebalanceInProgressError
        yield from do_sync_group()
        self.assertEqual(coordinator.member_id, 'some_invalid_member_id')
        self.assertEqual(coordinator.need_rejoin(subsc), True)

        error_type = Errors.UnknownMemberIdError
        yield from do_sync_group()
        self.assertEqual(coordinator.member_id, UNKNOWN_MEMBER_ID)
        self.assertEqual(coordinator.need_rejoin(subsc), True)

        error_type = Errors.NotCoordinatorForGroupError
        yield from do_sync_group()
        self.assertEqual(coordinator.coordinator_id, None)
        self.assertEqual(coordinator.need_rejoin(subsc), True)
        coordinator.coordinator_id = 15

        error_type = Errors.UnknownError()
        with self.assertRaises(Errors.KafkaError):  # Masked as some KafkaError
            yield from do_sync_group()
        self.assertEqual(coordinator.need_rejoin(subsc), True)

    @run_until_complete
    def test_subscribe_pattern(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()

        test_listener = RebalanceListenerForTest()
        subscription = SubscriptionState(loop=self.loop)
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='subs-pattern-group')

        yield from self.wait_topic(client, 'st-topic1')
        yield from self.wait_topic(client, 'st-topic2')

        subscription.subscribe_pattern(
            re.compile('st-topic*'), listener=test_listener)
        client.set_topics([])
        yield from subscription.wait_for_assignment()

        self.assertNotEqual(coordinator.coordinator_id, None)
        self.assertFalse(coordinator.need_rejoin(subscription.subscription))

        tp_list = subscription.assigned_partitions()
        assigned = set([('st-topic1', 0), ('st-topic1', 1),
                        ('st-topic2', 0), ('st-topic2', 1)])
        self.assertEqual(tp_list, assigned)

        self.assertEqual(test_listener.revoked, [set([])])
        self.assertEqual(test_listener.assigned, [assigned])
        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_get_offsets(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        tp1 = TopicPartition('topic1', 0)
        tp2 = TopicPartition('topic1', 1)

        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='getoffsets-group')
        yield from subscription.wait_for_assignment()
        assignment = subscription.subscription.assignment

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from producer.send('topic1', b'first msg', partition=0)
        yield from producer.send('topic1', b'second msg', partition=1)
        yield from producer.send('topic1', b'third msg', partition=1)
        yield from producer.stop()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, ''),
                   TopicPartition('topic1', 1): OffsetAndMetadata(2, '')}
        yield from coordinator.commit_offsets(assignment, offsets)

        self.assertEqual(assignment.all_consumed_offsets(), {})
        subscription.seek(tp1, 0)
        subscription.seek(tp2, 0)
        self.assertEqual(assignment.state_value(tp1).committed.offset, 1)
        self.assertEqual(assignment.state_value(tp2).committed.offset, 2)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_commit_failed_scenarios(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-offsets-group')

        yield from subscription.wait_for_assignment()
        assignment = subscription.subscription.assignment

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        yield from coordinator.commit_offsets(assignment, offsets)

        _orig_send_req = coordinator._send_req
        with mock.patch.object(coordinator, "_send_req") as mocked:
            commit_error = None

            @asyncio.coroutine
            def mock_send_req(request):
                if request.API_KEY == OffsetCommitRequest[0].API_KEY:
                    if isinstance(commit_error, list):
                        error_code = commit_error.pop(0).errno
                    else:
                        error_code = commit_error.errno
                    resp_topics = [("topic1", [(0, error_code)])]
                    return OffsetCommitResponse_v2(resp_topics)
                return (yield from _orig_send_req(request))
            mocked.side_effect = mock_send_req

            # Not retriable errors are propagated
            commit_error = Errors.GroupAuthorizationFailedError
            with self.assertRaises(Errors.GroupAuthorizationFailedError):
                yield from coordinator.commit_offsets(assignment, offsets)

            commit_error = Errors.TopicAuthorizationFailedError
            with self.assertRaises(Errors.TopicAuthorizationFailedError):
                yield from coordinator.commit_offsets(assignment, offsets)

            commit_error = Errors.InvalidCommitOffsetSizeError
            with self.assertRaises(Errors.InvalidCommitOffsetSizeError):
                yield from coordinator.commit_offsets(assignment, offsets)

            commit_error = Errors.OffsetMetadataTooLargeError
            with self.assertRaises(Errors.OffsetMetadataTooLargeError):
                yield from coordinator.commit_offsets(assignment, offsets)

            # retriable erros should be retried
            commit_error = [
                Errors.GroupLoadInProgressError,
                Errors.GroupLoadInProgressError,
                Errors.NoError,
            ]
            yield from coordinator.commit_offsets(assignment, offsets)

            # If rebalance is needed we can't commit offset
            commit_error = Errors.RebalanceInProgressError
            with self.assertRaises(Errors.CommitFailedError):
                yield from coordinator.commit_offsets(assignment, offsets)
            self.assertTrue(coordinator.need_rejoin(subscription.subscription))
            self.assertNotEqual(coordinator.member_id, UNKNOWN_MEMBER_ID)
            yield from subscription.wait_for_assignment()
            assignment = subscription.subscription.assignment

            commit_error = Errors.UnknownMemberIdError
            was_member_id = coordinator.member_id
            with self.assertRaises(Errors.CommitFailedError):
                yield from coordinator.commit_offsets(assignment, offsets)
            self.assertTrue(coordinator.need_rejoin(subscription.subscription))
            self.assertEqual(coordinator.member_id, UNKNOWN_MEMBER_ID)
            # NOTE: Reconnecting with unknown ID will force a
            # session_timeout_ms wait on broker, so we leave group to avoid
            # that. Hack for test purposes)
            request = LeaveGroupRequest(coordinator.group_id, was_member_id)
            yield from coordinator._send_req(request)
            yield from subscription.wait_for_assignment()
            assignment = subscription.subscription.assignment

            # Coordinator errors should be retried after it was found again
            commit_error = [
                Errors.GroupCoordinatorNotAvailableError,
                Errors.NoError
            ]
            yield from coordinator.commit_offsets(assignment, offsets)
            commit_error = [
                Errors.NotCoordinatorForGroupError,
                Errors.NoError
            ]
            yield from coordinator.commit_offsets(assignment, offsets)

            # Make sure coordinator_id is reset properly each retry
            commit_error = Errors.GroupCoordinatorNotAvailableError
            with self.assertRaises(Errors.GroupCoordinatorNotAvailableError):
                yield from coordinator._do_commit_offsets(assignment, offsets)
            self.assertEqual(coordinator.coordinator_id, None)

            # Unknown errors are just propagated too
            commit_error = Errors.UnknownError
            with self.assertRaises(Errors.UnknownError):
                yield from coordinator.commit_offsets(assignment, offsets)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_fetchoffsets_failed_scenarios(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='fetch-offsets-group')
        yield from subscription.wait_for_assignment()

        tp = TopicPartition('topic1', 0)
        partitions = {tp}
        _orig_send_req = coordinator._send_req
        with mock.patch.object(coordinator, "_send_req") as mocked:
            fetch_error = None

            @asyncio.coroutine
            def mock_send_req(request):
                if request.API_KEY == OffsetFetchRequest.API_KEY:
                    if isinstance(fetch_error, list):
                        error_code = fetch_error.pop(0).errno
                    else:
                        error_code = fetch_error.errno
                    if error_code == Errors.NoError.errno:
                        offset = 10
                    else:
                        offset = -1
                    resp_topics = [("topic1", [(0, offset, "", error_code)])]
                    return request.RESPONSE_TYPE(resp_topics)
                return (yield from _orig_send_req(request))
            mocked.side_effect = mock_send_req

            fetch_error = [
                Errors.GroupLoadInProgressError,
                Errors.GroupLoadInProgressError,
                Errors.NoError,
                Errors.NoError,
                Errors.NoError
            ]
            res = yield from coordinator.fetch_committed_offsets(partitions)
            self.assertEqual(res, {tp: OffsetAndMetadata(10, "")})

            # Just omit the topic with a warning
            fetch_error = Errors.UnknownTopicOrPartitionError
            res = yield from coordinator.fetch_committed_offsets(partitions)
            self.assertEqual(res, {})

            fetch_error = [
                Errors.NotCoordinatorForGroupError,
                Errors.NotCoordinatorForGroupError,
                Errors.NoError,
                Errors.NoError,
                Errors.NoError
            ]
            r = yield from coordinator.fetch_committed_offsets(partitions)
            self.assertEqual(r, {tp: OffsetAndMetadata(10, "")})

            fetch_error = Errors.UnknownError
            with self.assertRaises(Errors.KafkaError):
                yield from coordinator.fetch_committed_offsets(partitions)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_subscription_replace_on_rebalance(self):
        # See issue #88
        client = AIOKafkaClient(
            metadata_max_age_ms=2000, loop=self.loop,
            bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        client.set_topics(('topic1', ))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-subscribe-replace',
            heartbeat_interval_ms=1000)

        _perform_assignment = coordinator._perform_assignment
        with mock.patch.object(coordinator, '_perform_assignment') as mocked:

            def _new(*args, **kw):
                # Change the subscription to different topic before we finish
                # rebalance
                res = _perform_assignment(*args, **kw)
                if subscription.subscription.topics == set(["topic1"]):
                    subscription.subscribe(topics=set(['topic2']))
                    client.set_topics(('topic2', ))
                return res
            mocked.side_effect = _new

            yield from subscription.wait_for_assignment()
            topics = set([
                tp.topic for tp in subscription.assigned_partitions()])
            self.assertEqual(topics, {'topic2'})

            # There should only be 2 rebalances to finish the task
            self.assertEqual(mocked.call_count, 2)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_subscription_append_on_rebalance(self):
        # same as above, but with adding topics instead of replacing them
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-subscribe-append',
            heartbeat_interval_ms=20000000)

        _perform_assignment = coordinator._perform_assignment
        with mock.patch.object(coordinator, '_perform_assignment') as mocked:

            def _new(*args, **kw):
                # Change the subscription to different topic before we finish
                # rebalance
                res = _perform_assignment(*args, **kw)
                if subscription.subscription.topics == set(["topic1"]):
                    subscription.subscribe(topics=set(['topic1', 'topic2']))
                    client.set_topics(('topic1', 'topic2', ))
                return res
            mocked.side_effect = _new

            yield from subscription.wait_for_assignment()
            topics = set([
                tp.topic for tp in subscription.assigned_partitions()])
            self.assertEqual(topics, {'topic1', 'topic2'})

            # There should only be 2 rebalances to finish the task
            self.assertEqual(mocked.call_count, 2)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_metadata_update_during_rebalance(self):
        # Race condition where client.set_topics start MetadataUpdate, but it
        # fails to arrive before leader performed assignment

        # Just ensure topics are created
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')
        yield from client.close()

        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        self.add_cleanup(client.close)
        subscription = SubscriptionState(loop=self.loop)
        client.set_topics(("topic1", ))
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-metadata-update',
            heartbeat_interval_ms=20000000)
        self.add_cleanup(coordinator.close)
        yield from subscription.wait_for_assignment()
        # Check that topic's partitions are properly assigned
        self.assertEqual(
            subscription.assigned_partitions(),
            {TopicPartition("topic1", 0), TopicPartition("topic1", 1)})

        _metadata_update = client._metadata_update
        with mock.patch.object(client, '_metadata_update') as mocked:
            @asyncio.coroutine
            def _new(*args, **kw):
                # Just make metadata updates a bit more slow for test
                # robustness
                yield from asyncio.sleep(0.5, loop=self.loop)
                res = yield from _metadata_update(*args, **kw)
                return res
            mocked.side_effect = _new

            # This case will assure, that the started metadata update will be
            # waited for before assigning partitions. ``set_topics`` will start
            # the metadata update
            subscription.subscribe(topics=set(['topic2']))
            client.set_topics(('topic2', ))
            yield from subscription.wait_for_assignment()
            self.assertEqual(
                subscription.assigned_partitions(),
                {TopicPartition("topic2", 0), TopicPartition("topic2", 1)})

    @run_until_complete
    def test_coordinator_metadata_change_by_broker(self):
        # Issue #108. We can have a misleading metadata change, that will
        # trigger additional rebalance
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')
        client.set_topics(['other_topic'])
        yield from client.force_metadata_update()

        subscription = SubscriptionState(loop=self.loop)
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-subscribe-append',
            heartbeat_interval_ms=2000000)
        subscription.subscribe(topics=set(['topic1']))
        yield from client.set_topics(('topic1', ))
        yield from subscription.wait_for_assignment()

        _perform_assignment = coordinator._perform_assignment
        with mock.patch.object(coordinator, '_perform_assignment') as mocked:
            mocked.side_effect = _perform_assignment

            subscription.subscribe(topics=set(['topic2']))
            yield from client.set_topics(('topic2', ))

            # Should only trigger 1 rebalance, but will trigger 2 with bug:
            #   Metadata snapshot will change:
            #   {'topic1': {0, 1}} -> {'topic1': {0, 1}, 'topic2': {0, 1}}
            #   And then again:
            #   {'topic1': {0, 1}, 'topic2': {0, 1}} -> {'topic2': {0, 1}}
            yield from subscription.wait_for_assignment()
            yield from client.force_metadata_update()
            self.assertFalse(
                coordinator.need_rejoin(subscription.subscription))
            self.assertEqual(mocked.call_count, 1)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_ensure_active_group_on_expired_membership(self):
        # Do not fail group join if group membership has expired (ie autocommit
        # fails on join prepare)
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-offsets-group', session_timeout_ms=6000,
            heartbeat_interval_ms=1000)
        yield from subscription.wait_for_assignment()
        assignment = subscription.subscription.assignment

        # Make sure we have something to commit before rejoining
        tp = TopicPartition('topic1', 0)
        subscription.seek(tp, 0)
        offsets = assignment.all_consumed_offsets()
        self.assertTrue(offsets)  # Not empty

        # during OffsetCommit, UnknownMemberIdError is raised
        _orig_send_req = coordinator._send_req
        resp_topics = [("topic1", [(0, Errors.UnknownMemberIdError.errno)])]
        with mock.patch.object(coordinator, "_send_req") as mocked:
            @asyncio.coroutine
            def mock_send_req(request):
                if request.API_KEY == OffsetCommitRequest[0].API_KEY:
                    return OffsetCommitResponse_v2(resp_topics)
                return (yield from _orig_send_req(request))
            mocked.side_effect = mock_send_req

            with self.assertRaises(Errors.CommitFailedError):
                yield from coordinator.commit_offsets(assignment, offsets)
            self.assertTrue(coordinator.need_rejoin(subscription.subscription))
            # Waiting will assure we could rebalance even if commit fails
            yield from subscription.wait_for_assignment()

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator__send_req(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        self.add_cleanup(client.close)
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-my-group', session_timeout_ms=6000,
            heartbeat_interval_ms=1000)
        self.add_cleanup(coordinator.close)

        request = OffsetCommitRequest[2](topics=[])

        # We did not call ensure_coordinator_known yet
        with self.assertRaises(Errors.GroupCoordinatorNotAvailableError):
            yield from coordinator._send_req(request)

        yield from coordinator.ensure_coordinator_known()
        self.assertIsNotNone(coordinator.coordinator_id)

        with mock.patch.object(client, "send") as mocked:
            @asyncio.coroutine
            def mock_send(*args, **kw):
                raise Errors.KafkaError("Some unexpected error")
            mocked.side_effect = mock_send

            # _send_req should mark coordinator dead on errors
            with self.assertRaises(Errors.KafkaError):
                yield from coordinator._send_req(request)
            self.assertIsNone(coordinator.coordinator_id)

    @run_until_complete
    def test_coordinator_close(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        self.add_cleanup(client.close)
        subscription = SubscriptionState(loop=self.loop)
        waiter = create_future(loop=self.loop)

        class WaitingListener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked):
                pass

            @asyncio.coroutine
            def on_partitions_assigned(self, assigned, waiter=waiter):
                yield from waiter

        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-my-group', session_timeout_ms=6000,
            heartbeat_interval_ms=1000)
        subscription.subscribe(
            topics=set(['topic1']), listener=WaitingListener())

        # Close task should be loyal to rebalance and wait for it to finish
        close_task = ensure_future(coordinator.close(), loop=self.loop)
        yield from asyncio.sleep(0.1, loop=self.loop)
        self.assertFalse(close_task.done())

        # Releasing the waiter on listener will allow close task to finish
        waiter.set_result(True)
        yield from close_task

        # You can close again with no effect
        yield from coordinator.close()

    @run_until_complete
    def test_coordinator_close_autocommit(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        self.add_cleanup(client.close)
        subscription = SubscriptionState(loop=self.loop)
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-my-group', session_timeout_ms=6000,
            heartbeat_interval_ms=1000)
        subscription.subscribe(topics=set(['topic1']))
        yield from subscription.wait_for_assignment()

        waiter = create_future(loop=self.loop)

        @asyncio.coroutine
        def commit_offsets(*args, **kw):
            yield from waiter

        coordinator.commit_offsets = mocked = mock.Mock()
        mocked.side_effect = commit_offsets

        # Close task should call autocommit last time
        close_task = ensure_future(coordinator.close(), loop=self.loop)
        yield from asyncio.sleep(0.1, loop=self.loop)
        # self.assertFalse(close_task.done())

        # Raising an error should not prevent from closing. Error should be
        # just logged
        waiter.set_exception(Errors.UnknownError())
        yield from close_task

    @run_until_complete
    def test_coordinator_ensure_coordinator_known(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            heartbeat_interval_ms=20000)
        coordinator._coordination_task.cancel()  # disable for test
        try:
            yield from coordinator._coordination_task
        except asyncio.CancelledError:
            pass
        coordinator._coordination_task = asyncio.sleep(0.1, loop=self.loop)
        self.add_cleanup(coordinator.close)

        client.ready = mock.Mock()
        client.force_metadata_update = mock.Mock()
        client.force_metadata_update.side_effect = \
            asyncio.coroutine(lambda: True)

        @asyncio.coroutine
        def ready(node_id, group=None):
            if node_id == 0:
                return True
            return False
        client.ready.side_effect = ready
        coordinator._do_coordinator_lookup = mock.Mock()

        coordinator_lookup = None

        @asyncio.coroutine
        def _do_coordinator_lookup():
            node_id = coordinator_lookup.pop()
            if isinstance(node_id, Exception):
                raise node_id
            return node_id
        coordinator._do_coordinator_lookup.side_effect = _do_coordinator_lookup

        # CASE: the lookup returns a broken node, that can't be connected
        # to. Ensure should wait until coordinator lookup finds the correct
        # node.
        coordinator.coordinator_dead()
        coordinator_lookup = [0, 1, 1]
        yield from coordinator.ensure_coordinator_known()
        self.assertEqual(coordinator.coordinator_id, 0)
        self.assertEqual(client.force_metadata_update.call_count, 0)

        # CASE: lookup fails with error first time. We update metadata and try
        # again
        coordinator.coordinator_dead()
        coordinator_lookup = [0, Errors.UnknownError()]
        yield from coordinator.ensure_coordinator_known()
        self.assertEqual(client.force_metadata_update.call_count, 1)

    @run_until_complete
    def test_coordinator__do_heartbeat(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            heartbeat_interval_ms=20000)
        coordinator._coordination_task.cancel()  # disable for test
        try:
            yield from coordinator._coordination_task
        except asyncio.CancelledError:
            pass
        coordinator._coordination_task = asyncio.sleep(0.1, loop=self.loop)
        self.add_cleanup(coordinator.close)

        _orig_send_req = coordinator._send_req
        coordinator._send_req = mocked = mock.Mock()
        heartbeat_error = None

        @asyncio.coroutine
        def mock_send_req(request):
            if request.API_KEY == HeartbeatRequest.API_KEY:
                if isinstance(heartbeat_error, list):
                    error_code = heartbeat_error.pop(0).errno
                else:
                    error_code = heartbeat_error.errno
                return HeartbeatRequest.RESPONSE_TYPE(error_code)
            return (yield from _orig_send_req(request))
        mocked.side_effect = mock_send_req

        coordinator.coordinator_id = 15
        heartbeat_error = Errors.GroupCoordinatorNotAvailableError()
        success = yield from coordinator._do_heartbeat()
        self.assertFalse(success)
        self.assertIsNone(coordinator.coordinator_id)

        coordinator._rejoin_needed_fut = create_future(loop=self.loop)
        heartbeat_error = Errors.RebalanceInProgressError()
        success = yield from coordinator._do_heartbeat()
        self.assertTrue(success)
        self.assertTrue(coordinator._rejoin_needed_fut.done())

        coordinator.member_id = "some_member"
        coordinator._rejoin_needed_fut = create_future(loop=self.loop)
        heartbeat_error = Errors.IllegalGenerationError()
        success = yield from coordinator._do_heartbeat()
        self.assertFalse(success)
        self.assertTrue(coordinator._rejoin_needed_fut.done())
        self.assertEqual(coordinator.member_id, UNKNOWN_MEMBER_ID)

        coordinator.member_id = "some_member"
        coordinator._rejoin_needed_fut = create_future(loop=self.loop)
        heartbeat_error = Errors.UnknownMemberIdError()
        success = yield from coordinator._do_heartbeat()
        self.assertFalse(success)
        self.assertTrue(coordinator._rejoin_needed_fut.done())
        self.assertEqual(coordinator.member_id, UNKNOWN_MEMBER_ID)

        heartbeat_error = Errors.UnknownError()
        success = yield from coordinator._do_heartbeat()
        self.assertFalse(success)

        heartbeat_error = Errors.NoError()
        success = yield from coordinator._do_heartbeat()
        self.assertTrue(success)

    @run_until_complete
    def test_coordinator__heartbeat_routine(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState(loop=self.loop)
        subscription.subscribe(topics=set(['topic1']))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            heartbeat_interval_ms=100,
            session_timeout_ms=300,
            retry_backoff_ms=50)
        coordinator._coordination_task.cancel()  # disable for test
        try:
            yield from coordinator._coordination_task
        except asyncio.CancelledError:
            pass
        coordinator._coordination_task = asyncio.sleep(0.1, loop=self.loop)
        self.add_cleanup(coordinator.close)

        coordinator._do_heartbeat = mocked = mock.Mock()
        coordinator.coordinator_id = 15
        success = None

        @asyncio.coroutine
        def _do_heartbeat(*args, **kw):
            if isinstance(success, list):
                return success.pop(0)
            return success
        mocked.side_effect = _do_heartbeat

        coordinator.ensure_coordinator_known = mock.Mock()
        coordinator.ensure_coordinator_known.side_effect = asyncio.coroutine(
            lambda: None)

        routine = ensure_future(
            coordinator._heartbeat_routine(), loop=self.loop)

        def cleanup():
            routine.cancel()
            return routine
        self.add_cleanup(cleanup)

        # CASE: simple heartbeat
        success = True
        yield from asyncio.sleep(0.13, loop=self.loop)
        self.assertFalse(routine.done())
        self.assertEqual(mocked.call_count, 1)

        # CASE: 2 heartbeat fail
        success = False
        yield from asyncio.sleep(0.15, loop=self.loop)
        self.assertFalse(routine.done())
        # We did 2 heartbeats as we waited only retry_backoff_ms between them
        self.assertEqual(mocked.call_count, 3)

        # CASE: session_timeout_ms elapsed without heartbeat
        yield from asyncio.sleep(0.10, loop=self.loop)
        self.assertEqual(mocked.call_count, 5)
        self.assertEqual(coordinator.coordinator_id, 15)
        # last heartbeat try
        yield from asyncio.sleep(0.05, loop=self.loop)
        self.assertEqual(mocked.call_count, 6)
        self.assertIsNone(coordinator.coordinator_id)
