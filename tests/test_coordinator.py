import asyncio
from unittest import mock

from kafka.protocol.group import JoinGroupRequest_v0 as JoinGroupRequest
import kafka.common as Errors
from kafka.consumer.subscription_state import SubscriptionState

from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka import ConsumerRebalanceListener
from aiokafka.errors import (
    NoBrokersAvailable, GroupCoordinatorNotAvailableError, KafkaError)
from aiokafka.producer import AIOKafkaProducer
from aiokafka.client import AIOKafkaClient
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.consumer.group_coordinator import (
    GroupCoordinator, CoordinatorGroupRebalance)


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


class MockedKafkaErrCode:
    def __init__(self, *exc_classes):
        self.exc_classes = exc_classes
        self.idx = 0

    def __call__(self, *args, **kwargs):
        ec = self.exc_classes[self.idx]
        self.idx += 1
        self.idx %= len(self.exc_classes)
        return ec


class TestKafkaCoordinatorIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    def test_coordinator_workflow(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')

        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1', 'topic2'))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            session_timeout_ms=10000,
            heartbeat_interval_ms=500,
            retry_backoff_ms=100)

        self.assertEqual(coordinator.coordinator_id, None)
        self.assertEqual(coordinator.rejoin_needed, True)

        yield from coordinator.ensure_coordinator_known()
        self.assertNotEqual(coordinator.coordinator_id, None)

        yield from coordinator.ensure_active_group()
        self.assertNotEqual(coordinator.coordinator_id, None)
        self.assertEqual(coordinator.rejoin_needed, False)

        tp_list = subscription.assigned_partitions()
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))

        # start second coordinator
        client2 = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client2.bootstrap()
        subscription2 = SubscriptionState('latest')
        subscription2.subscribe(topics=('topic1', 'topic2'))
        coordinator2 = GroupCoordinator(
            client2, subscription2, loop=self.loop,
            session_timeout_ms=10000,
            heartbeat_interval_ms=500,
            retry_backoff_ms=100)
        yield from coordinator2.ensure_active_group()
        yield from coordinator.ensure_active_group()

        tp_list = subscription.assigned_partitions()
        self.assertEqual(len(tp_list), 2)
        tp_list2 = subscription2.assigned_partitions()
        self.assertEqual(len(tp_list2), 2)
        tp_list |= tp_list2
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))
        yield from coordinator.close()
        yield from client.close()

        yield from asyncio.sleep(0.6, loop=self.loop)  # wait heartbeat
        yield from coordinator2.ensure_active_group()
        tp_list = subscription2.assigned_partitions()
        self.assertEqual(tp_list, set([('topic1', 0), ('topic1', 1),
                                       ('topic2', 0), ('topic2', 1)]))

        yield from coordinator2.close()
        yield from client2.close()

    @run_until_complete
    def test_failed_broker_conn(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop)

        with self.assertRaises(NoBrokersAvailable):
            yield from coordinator.ensure_coordinator_known()

    @run_until_complete
    def test_failed_group_join(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            retry_backoff_ms=10)

        @asyncio.coroutine
        def do_rebalance():
            rebalance = CoordinatorGroupRebalance(
                coordinator, coordinator.group_id, coordinator.coordinator_id,
                subscription.subscription, coordinator._assignors,
                coordinator._session_timeout_ms,
                coordinator._retry_backoff_ms,
                loop=self.loop)
            yield from rebalance.perform_group_join()

        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')

        mocked = mock.MagicMock()
        coordinator._client = mocked

        # no exception expected, just wait
        mocked.send.side_effect = Errors.GroupLoadInProgressError()
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(), True)

        mocked.send.side_effect = Errors.InvalidGroupIdError()
        with self.assertRaises(Errors.InvalidGroupIdError):
            yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(), True)

        # no exception expected, member_id should be reseted
        coordinator.member_id = 'some_invalid_member_id'
        mocked.send.side_effect = Errors.UnknownMemberIdError()
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(), True)
        self.assertEqual(
            coordinator.member_id, JoinGroupRequest.UNKNOWN_MEMBER_ID)

        # no exception expected, coordinator_id should be reseted
        coordinator.coordinator_id = 'some_id'
        mocked.send.side_effect = Errors.GroupCoordinatorNotAvailableError()
        yield from do_rebalance()
        self.assertEqual(coordinator.need_rejoin(), True)
        self.assertEqual(coordinator.coordinator_id, None)
        yield from client.close()

    @run_until_complete
    def test_failed_sync_group(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            heartbeat_interval_ms=20000)

        @asyncio.coroutine
        def do_sync_group():
            rebalance = CoordinatorGroupRebalance(
                coordinator, coordinator.group_id, coordinator.coordinator_id,
                subscription.subscription, coordinator._assignors,
                coordinator._session_timeout_ms,
                coordinator._retry_backoff_ms,
                loop=self.loop)
            yield from rebalance._on_join_follower()

        with self.assertRaises(GroupCoordinatorNotAvailableError):
            yield from do_sync_group()

        mocked = mock.MagicMock()
        coordinator._client = mocked

        coordinator.member_id = 'some_invalid_member_id'
        coordinator.coordinator_unknown = asyncio.coroutine(lambda: False)
        mocked.send.side_effect = Errors.UnknownMemberIdError()
        with self.assertRaises(Errors.UnknownMemberIdError):
            yield from do_sync_group()
        self.assertEqual(
            coordinator.member_id, JoinGroupRequest.UNKNOWN_MEMBER_ID)

        mocked.send.side_effect = Errors.NotCoordinatorForGroupError()
        coordinator.coordinator_id = 'some_id'
        with self.assertRaises(Errors.NotCoordinatorForGroupError):
            yield from do_sync_group()
        self.assertEqual(coordinator.coordinator_id, None)

        mocked.send.side_effect = KafkaError()
        with self.assertRaises(KafkaError):
            yield from do_sync_group()

        # client sends LeaveGroupRequest to group coordinator
        # if generation > 0 (means that client is a member of group)
        # expecting no exception in this case (error should be ignored in close
        # method)
        coordinator.generation = 33
        yield from coordinator.close()

    @run_until_complete
    def test_with_nocommit_support(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            enable_auto_commit=False)
        self.assertEqual(coordinator._auto_commit_task, None)

    @run_until_complete
    def test_subscribe_pattern(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()

        test_listener = RebalanceListenerForTest()
        subscription = SubscriptionState('latest')
        subscription.subscribe(pattern='st-topic*', listener=test_listener)
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='subs-pattern-group')

        yield from self.wait_topic(client, 'st-topic1')
        yield from self.wait_topic(client, 'st-topic2')

        yield from coordinator.ensure_active_group()
        self.assertNotEqual(coordinator.coordinator_id, None)
        self.assertEqual(coordinator.rejoin_needed, False)

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

        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='getoffsets-group')

        yield from self.wait_topic(client, 'topic1')
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from producer.send('topic1', b'first msg', partition=0)
        yield from producer.send('topic1', b'second msg', partition=1)
        yield from producer.send('topic1', b'third msg', partition=1)
        yield from producer.stop()

        yield from coordinator.ensure_active_group()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, ''),
                   TopicPartition('topic1', 1): OffsetAndMetadata(2, '')}
        yield from coordinator.commit_offsets(offsets)

        self.assertEqual(subscription.all_consumed_offsets(), {})
        subscription.seek(('topic1', 0), 0)
        subscription.seek(('topic1', 1), 0)
        yield from coordinator.refresh_committed_offsets()
        self.assertEqual(subscription.assignment[('topic1', 0)].committed, 1)
        self.assertEqual(subscription.assignment[('topic1', 1)].committed, 2)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_offsets_failed_scenarios(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-offsets-group')

        yield from coordinator.ensure_active_group()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        yield from coordinator.commit_offsets(offsets)
        with mock.patch('aiokafka.errors.for_code') as mocked:
            mocked.return_value = Errors.GroupAuthorizationFailedError
            with self.assertRaises(Errors.GroupAuthorizationFailedError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.TopicAuthorizationFailedError
            with self.assertRaises(Errors.TopicAuthorizationFailedError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.InvalidCommitOffsetSizeError
            with self.assertRaises(Errors.InvalidCommitOffsetSizeError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.GroupLoadInProgressError
            with self.assertRaises(Errors.GroupLoadInProgressError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.RebalanceInProgressError
            with self.assertRaises(Errors.RebalanceInProgressError):
                yield from coordinator.commit_offsets(offsets)
            self.assertEqual(subscription.needs_partition_assignment, True)
            subscription.needs_partition_assignment = False

            mocked.return_value = Errors.UnknownMemberIdError
            with self.assertRaises(Errors.UnknownMemberIdError):
                yield from coordinator.commit_offsets(offsets)
            self.assertEqual(subscription.needs_partition_assignment, True)

            mocked.return_value = KafkaError
            with self.assertRaises(KafkaError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.NotCoordinatorForGroupError
            with self.assertRaises(Errors.NotCoordinatorForGroupError):
                yield from coordinator.commit_offsets(offsets)
            self.assertEqual(coordinator.coordinator_id, None)

            with self.assertRaises(Errors.GroupCoordinatorNotAvailableError):
                yield from coordinator.commit_offsets(offsets)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_fetchoffsets_failed_scenarios(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='fetch-offsets-group')

        yield from coordinator.ensure_active_group()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        with mock.patch('aiokafka.errors.for_code') as mocked:
            mocked.side_effect = MockedKafkaErrCode(
                Errors.GroupLoadInProgressError, Errors.NoError)
            yield from coordinator.fetch_committed_offsets(offsets)

            mocked.side_effect = MockedKafkaErrCode(
                Errors.UnknownMemberIdError, Errors.NoError)
            with self.assertRaises(Errors.UnknownMemberIdError):
                yield from coordinator.fetch_committed_offsets(offsets)
            self.assertEqual(subscription.needs_partition_assignment, True)

            mocked.side_effect = None
            mocked.return_value = Errors.UnknownTopicOrPartitionError
            r = yield from coordinator.fetch_committed_offsets(offsets)
            self.assertEqual(r, {})

            mocked.return_value = KafkaError
            with self.assertRaises(KafkaError):
                yield from coordinator.fetch_committed_offsets(offsets)

            mocked.side_effect = MockedKafkaErrCode(
                Errors.NotCoordinatorForGroupError,
                Errors.NoError, Errors.NoError, Errors.NoError)
            yield from coordinator.fetch_committed_offsets(offsets)

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
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
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
                subscription.subscribe(topics=('topic2', ))
                client.set_topics(('topic2', ))
                return res
            mocked.side_effect = _new

            yield from coordinator.ensure_active_group()
            # yield from asyncio.sleep(10, loop=self.loop)
            self.assertEqual(subscription.needs_partition_assignment, False)
            topics = set([tp.topic for tp in subscription.assignment])
            self.assertEqual(topics, {'topic2'})

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_subscription_append_on_rebalance(self):
        # same as above, but with adding topics instead of replacing them
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
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
                subscription.subscribe(topics=('topic1', 'topic2', ))
                client.set_topics(('topic1', 'topic2', ))
                return res
            mocked.side_effect = _new

            yield from coordinator.ensure_active_group()
            self.assertEqual(subscription.needs_partition_assignment, False)
            topics = set([tp.topic for tp in subscription.assignment])
            self.assertEqual(topics, {'topic1', 'topic2'})

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
        subscription = SubscriptionState('earliest')
        client.set_topics(["topic1"])
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-metadata-update',
            heartbeat_interval_ms=20000000)
        self.add_cleanup(coordinator.close)
        yield from coordinator.ensure_active_group()
        # Check that topic's partitions are properly assigned
        self.assertEqual(subscription.needs_partition_assignment, False)
        self.assertEqual(
            set(subscription.assignment.keys()),
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

            subscription.subscribe(topics=('topic2', ))
            client.set_topics(('topic2', ))
            yield from coordinator.ensure_active_group()
            self.assertEqual(subscription.needs_partition_assignment, False)
            self.assertEqual(
                set(subscription.assignment.keys()),
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

        subscription = SubscriptionState('earliest')
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='race-rebalance-subscribe-append',
            heartbeat_interval_ms=2000000)
        subscription.subscribe(topics=('topic1',))
        yield from client.set_topics(subscription.group_subscription())
        yield from coordinator.ensure_active_group()

        _perform_assignment = coordinator._perform_assignment
        with mock.patch.object(coordinator, '_perform_assignment') as mocked:
            mocked.side_effect = _perform_assignment

            subscription.subscribe(topics=('topic2',))
            yield from client.set_topics(subscription.group_subscription())

            # Should only trigger 1 rebalance, but will trigger 2 with bug:
            #   Metadata snapshot will change:
            #   {'topic1': {0, 1}} -> {'topic1': {0, 1}, 'topic2': {0, 1}}
            #   And then again:
            #   {'topic1': {0, 1}, 'topic2': {0, 1}} -> {'topic2': {0, 1}}
            yield from coordinator.ensure_active_group()
            yield from client.force_metadata_update()
            yield from coordinator.ensure_active_group()
            self.assertEqual(mocked.call_count, 1)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_coordinator_ensure_active_group_on_expired_membership(self):
        # Do not fail ensure_active_group() if group membership has expired
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = GroupCoordinator(
            client, subscription, loop=self.loop,
            group_id='test-offsets-group')
        yield from coordinator.ensure_active_group()

        # during OffsetCommit, UnknownMemberIdError is raised
        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        with mock.patch('aiokafka.errors.for_code') as mocked:
            mocked.return_value = Errors.UnknownMemberIdError
            with self.assertRaises(Errors.UnknownMemberIdError):
                yield from coordinator.commit_offsets(offsets)
            self.assertEqual(subscription.needs_partition_assignment, True)

        # same exception is raised during ensure_active_group()'s call to
        # commit_offsets() via _on_join_prepare() but doesn't break this method
        with mock.patch.object(coordinator, "commit_offsets") as mocked:
            @asyncio.coroutine
            def mock_commit_offsets(*args, **kwargs):
                raise Errors.UnknownMemberIdError()
            mocked.side_effect = mock_commit_offsets

            yield from coordinator.ensure_active_group()

        yield from coordinator.close()
        yield from client.close()
