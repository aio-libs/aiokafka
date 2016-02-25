import json
import asyncio
from unittest import mock

from kafka.protocol.group import JoinGroupRequest
from kafka.common import (NoBrokersAvailable,
                          GroupCoordinatorNotAvailableError,
                          KafkaError)
from kafka.common import OffsetAndMetadata, TopicPartition
import kafka.common as Errors
from kafka.consumer.subscription_state import (
    SubscriptionState,
    ConsumerRebalanceListener)

from .fixtures import ZookeeperFixture, KafkaFixture
from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka.coordinator.consumer import AIOConsumerCoordinator
from aiokafka.producer import AIOKafkaProducer
from aiokafka.client import AIOKafkaClient


class TestRebalanceListener(ConsumerRebalanceListener):
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
    @classmethod
    def setUpClass(cls):
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls.zk.close()

    @run_until_complete
    def test_coordinator_workflow(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        api_ver = yield from client.check_version()
        api_ver = tuple(map(int, api_ver.split('.')))
        if api_ver < (0, 9):
            # auto-assigning supported in >=kafka-0.9
            return
        yield from self.wait_topic(client, 'topic1')
        yield from self.wait_topic(client, 'topic2')

        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1', 'topic2'))
        coordinator = AIOConsumerCoordinator(
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
        coordinator2 = AIOConsumerCoordinator(
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
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop)

        with self.assertRaises(NoBrokersAvailable):
            yield from coordinator.ensure_coordinator_known()

    @run_until_complete
    def test_failed_group_join(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            retry_backoff_ms=10, api_version=(0, 8))

        yield from client.bootstrap()
        yield from self.wait_topic(client, 'topic1')

        mocked = mock.MagicMock()
        coordinator._client = mocked

        # no exception expected, just wait
        mocked.send.side_effect = Errors.GroupLoadInProgressError()
        yield from coordinator._perform_group_join()
        self.assertEqual(coordinator.need_rejoin(), True)

        mocked.send.side_effect = Errors.InvalidGroupIdError()
        yield from coordinator._perform_group_join()
        self.assertEqual(coordinator.need_rejoin(), True)

        # no exception expected, member_id should be reseted
        coordinator.member_id = 'some_invalid_member_id'
        mocked.send.side_effect = Errors.UnknownMemberIdError()
        yield from coordinator._perform_group_join()
        self.assertEqual(coordinator.need_rejoin(), True)
        self.assertEqual(
            coordinator.member_id, JoinGroupRequest.UNKNOWN_MEMBER_ID)

        # no exception expected, coordinator_id should be reseted
        coordinator.coordinator_id = 'some_id'
        mocked.send.side_effect = Errors.GroupCoordinatorNotAvailableError()
        yield from coordinator._perform_group_join()
        self.assertEqual(coordinator.need_rejoin(), True)
        self.assertEqual(coordinator.coordinator_id, None)
        yield from client.close()

    @run_until_complete
    def test_failed_sync_group(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop)

        with self.assertRaises(GroupCoordinatorNotAvailableError):
            yield from coordinator._on_join_follower()

        mocked = mock.MagicMock()
        coordinator._client = mocked

        coordinator.member_id = 'some_invalid_member_id'
        coordinator.coordinator_unknown = asyncio.coroutine(lambda: False)
        mocked.send.side_effect = Errors.UnknownMemberIdError()
        with self.assertRaises(Errors.UnknownMemberIdError):
            yield from coordinator._on_join_follower()
            self.assertEqual(
                coordinator.member_id, JoinGroupRequest.UNKNOWN_MEMBER_ID)

        mocked.send.side_effect = Errors.NotCoordinatorForGroupError()
        coordinator.coordinator_id = 'some_id'
        with self.assertRaises(Errors.NotCoordinatorForGroupError):
            yield from coordinator._on_join_follower()
            self.assertEqual(coordinator.coordinator_id, None)

        mocked.send.side_effect = KafkaError()
        with self.assertRaises(KafkaError):
            yield from coordinator._on_join_follower()

        # expect no exception
        coordinator.generation = 33
        yield from coordinator.close()

    @run_until_complete
    def test_with_nocommit_support(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        subscription = SubscriptionState('latest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            api_version=(0, 7, 4))
        self.assertEqual(coordinator._auto_commit_task, None)

        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            group_id=None)
        self.assertEqual(coordinator._auto_commit_task, None)

    @run_until_complete
    def test_subscribe_pattern(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        api_ver = yield from client.check_version()
        api_ver = tuple(map(int, api_ver.split('.')))

        test_listener = TestRebalanceListener()
        subscription = SubscriptionState('latest')
        subscription.subscribe(pattern='st-topic*', listener=test_listener)
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            group_id='subs-pattern-group', api_version=api_ver)

        yield from self.wait_topic(client, 'st-topic1')
        yield from self.wait_topic(client, 'st-topic2')
        if api_ver >= (0, 9):
            yield from coordinator.ensure_active_group()
            self.assertNotEqual(coordinator.coordinator_id, None)
            self.assertEqual(coordinator.rejoin_needed, False)

        tp_list = subscription.assigned_partitions()
        assigned = set([('st-topic1', 0), ('st-topic1', 1),
                        ('st-topic2', 0), ('st-topic2', 1)])
        self.assertEqual(tp_list, assigned)

        if api_ver >= (0, 9):
            self.assertEqual(test_listener.revoked, [set([])])
            self.assertEqual(test_listener.assigned, [assigned])
        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_get_offsets(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        api_ver = yield from client.check_version()
        api_ver = tuple(map(int, api_ver.split('.')))

        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            api_version=api_ver, group_id='getoffsets-group')

        yield from self.wait_topic(client, 'topic1')
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from producer.send('topic1', b'first msg', partition=0)
        yield from producer.send('topic1', b'second msg', partition=1)
        yield from producer.send('topic1', b'third msg', partition=1)
        yield from producer.stop()

        if api_ver >= (0, 9):
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
        api_ver = yield from client.check_version()
        api_ver = tuple(map(int, api_ver.split('.')))
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            api_version=api_ver, group_id='test-offsets-group')
        if api_ver >= (0, 9):
            yield from coordinator.ensure_active_group()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        yield from coordinator.commit_offsets(offsets)
        with mock.patch('kafka.common.for_code') as mocked:
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

            mocked.return_value = KafkaError
            with self.assertRaises(KafkaError):
                yield from coordinator.commit_offsets(offsets)

            mocked.return_value = Errors.NotCoordinatorForGroupError
            with self.assertRaises(Errors.NotCoordinatorForGroupError):
                yield from coordinator.commit_offsets(offsets)
            self.assertEqual(coordinator.coordinator_id, None)

            if api_ver >= (0, 9):
                with self.assertRaises(
                        Errors.GroupCoordinatorNotAvailableError):
                    yield from coordinator.commit_offsets(offsets)

        yield from coordinator.close()
        yield from client.close()

    @run_until_complete
    def test_fetchoffsets_failed_scenarios(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        api_ver = yield from client.check_version()
        api_ver = tuple(map(int, api_ver.split('.')))
        yield from self.wait_topic(client, 'topic1')
        subscription = SubscriptionState('earliest')
        subscription.subscribe(topics=('topic1',))
        coordinator = AIOConsumerCoordinator(
            client, subscription, loop=self.loop,
            api_version=api_ver, group_id='fetch-offsets-group')
        if api_ver >= (0, 9):
            yield from coordinator.ensure_active_group()

        offsets = {TopicPartition('topic1', 0): OffsetAndMetadata(1, '')}
        with mock.patch('kafka.common.for_code') as mocked:
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
                Errors.NotCoordinatorForGroupError, Errors.NoError)
            yield from coordinator.fetch_committed_offsets(offsets)

        yield from coordinator.close()
        yield from client.close()
