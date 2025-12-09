from unittest import mock

from aiokafka.client import AIOKafkaClient, ConnectionGroup, CoordinationType
from aiokafka.errors import (
    ConcurrentTransactions,
    CoordinatorLoadInProgressError,
    CoordinatorNotAvailableError,
    DuplicateSequenceNumber,
    GroupAuthorizationFailedError,
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
    InvalidTxnState,
    KafkaError,
    NodeNotReadyError,
    NoError,
    NotCoordinatorError,
    OperationNotAttempted,
    ProducerFenced,
    RequestTimedOutError,
    TopicAuthorizationFailedError,
    TransactionalIdAuthorizationFailed,
    UnknownError,
    UnknownTopicOrPartitionError,
)
from aiokafka.producer.message_accumulator import MessageAccumulator
from aiokafka.producer.sender import (
    AddOffsetsToTxnHandler,
    AddPartitionsToTxnHandler,
    BaseHandler,
    EndTxnHandler,
    InitPIDHandler,
    Sender,
    SendProduceReqHandler,
    TxnOffsetCommitHandler,
)
from aiokafka.producer.transaction_manager import TransactionManager, TransactionState
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.protocol.produce import ProduceRequest, ProduceResponse_v4
from aiokafka.protocol.transaction import (
    AddOffsetsToTxnRequest,
    AddOffsetsToTxnResponse_v0,
    AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse_v0,
    EndTxnRequest,
    EndTxnResponse_v0,
    InitProducerIdRequest,
    InitProducerIdResponse_v0,
    TxnOffsetCommitRequest,
    TxnOffsetCommitResponse_v0,
)
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import get_running_loop

from ._testutil import KafkaIntegrationTestCase, kafka_versions, run_until_complete

LOG_APPEND_TIME = 1


class TestSender(KafkaIntegrationTestCase):
    async def _setup_sender(self, no_init=False):
        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        await client.bootstrap()
        self.add_cleanup(client.close)
        await self.wait_topic(client, self.topic)

        tm = TransactionManager("test_tid", 30000)
        if not no_init:
            tm.set_pid_and_epoch(120, 22)
        ma = MessageAccumulator(client.cluster, 1000, 0, 30)
        sender = Sender(
            client,
            acks=-1,
            txn_manager=tm,
            message_accumulator=ma,
            retry_backoff_ms=100,
            request_timeout_ms=40000,
        )
        self.add_cleanup(sender.close)
        return sender

    @run_until_complete
    async def test_sender__sender_routine(self):
        sender = await self._setup_sender()

        async def mocked_call():
            return

        sender._maybe_wait_for_pid = mock.Mock(side_effect=mocked_call)
        await sender.start()

        self.assertNotEqual(sender._maybe_wait_for_pid.call_count, 0)

    async def _setup_sender_with_init_mocked(self):
        sender = await self._setup_sender(no_init=True)
        call_count = [0]

        async def mocked_call(node_id):
            call_count[0] += 1
            return call_count[0] != 1

        sender._do_init_pid = mock.Mock(side_effect=mocked_call)
        return sender

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sender_maybe_wait_for_pid_non_transactional(self):
        sender = await self._setup_sender_with_init_mocked()

        # If we are not using transactional manager will return right away
        sender._txn_manager = None
        await sender._maybe_wait_for_pid()
        sender._do_init_pid.assert_not_called()

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sender_maybe_wait_for_pid_on_failure(self):
        sender = await self._setup_sender_with_init_mocked()
        # Should retry metadata on failure
        sender.client.force_metadata_update = mock.Mock(
            side_effect=sender.client.force_metadata_update
        )
        await sender._maybe_wait_for_pid()
        self.assertNotEqual(sender._do_init_pid.call_count, 0)
        self.assertNotEqual(sender.client.force_metadata_update.call_count, 0)

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sender__find_coordinator(self):
        sender = await self._setup_sender()

        async def coordinator_lookup(coordinator_type, coordinator_key):
            if coordinator_type == CoordinationType.GROUP:
                return 0
            return 1

        sender.client.coordinator_lookup = mock.Mock(side_effect=coordinator_lookup)

        ready_count = 0

        async def ready(coordinator_id, group):
            nonlocal ready_count
            ready_count += 1
            return ready_count != 1

        sender.client.ready = mock.Mock(side_effect=ready)

        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.GROUP, coordinator_key="key"
        )
        self.assertEqual(node_id, 0)
        sender.client.coordinator_lookup.assert_called_with(
            CoordinationType.GROUP, "key"
        )
        sender.client.ready.assert_called_with(0, group=ConnectionGroup.COORDINATION)

        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.TRANSACTION, coordinator_key="key"
        )
        self.assertEqual(node_id, 1)
        sender.client.coordinator_lookup.assert_called_with(
            CoordinationType.TRANSACTION, "key"
        )
        sender.client.ready.assert_called_with(1, group=ConnectionGroup.COORDINATION)

        # Repeat call will return from cache
        self.assertEqual(sender.client.coordinator_lookup.call_count, 3)
        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.GROUP, coordinator_key="key"
        )
        self.assertEqual(node_id, 0)
        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.TRANSACTION, coordinator_key="key"
        )
        self.assertEqual(node_id, 1)
        self.assertEqual(sender.client.coordinator_lookup.call_count, 3)

        sender._coordinator_dead(CoordinationType.TRANSACTION)
        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.GROUP, coordinator_key="key"
        )
        self.assertEqual(node_id, 0)
        self.assertEqual(sender.client.coordinator_lookup.call_count, 3)
        node_id = await sender._find_coordinator(
            coordinator_type=CoordinationType.TRANSACTION, coordinator_key="key"
        )
        self.assertEqual(node_id, 1)
        self.assertEqual(sender.client.coordinator_lookup.call_count, 4)

        # On error
        sender._coordinator_dead(CoordinationType.TRANSACTION)
        sender.client.coordinator_lookup.side_effect = UnknownError
        with self.assertRaises(KafkaError):
            await sender._find_coordinator(
                coordinator_type=CoordinationType.TRANSACTION, coordinator_key="key"
            )

    @run_until_complete
    async def test_sender__handler_base_do(self):
        sender = await self._setup_sender()

        class MockHandler(BaseHandler):
            def create_request(self):
                return MetadataRequest([])

        mock_handler = MockHandler(sender)
        mock_handler.handle_response = mock.Mock(return_value=0.1)
        mock_handler.handle_error = mock.Mock(return_value=0.1)
        success = await mock_handler.do(node_id=0)
        self.assertFalse(success)

        MockHandler.return_value = None
        mock_handler.handle_response = mock.Mock(return_value=None)
        mock_handler.handle_error = mock.Mock(return_value=0.1)
        success = await mock_handler.do(node_id=0)
        self.assertTrue(success)

        loop = get_running_loop()
        time = loop.time()
        sender.client.send = mock.Mock(side_effect=NodeNotReadyError())
        success = await mock_handler.do(node_id=0)
        self.assertFalse(success)
        self.assertAlmostEqual(loop.time() - time, 0.1, 1)

    @run_until_complete
    async def test_sender__do_init_pid_create(self):
        sender = await self._setup_sender(no_init=True)
        init_handler = InitPIDHandler(sender)

        # Form request
        req = init_handler.create_request().prepare(
            {InitProducerIdRequest.API_KEY: (0, 0)}
        )
        self.assertEqual(req.API_KEY, InitProducerIdRequest.API_KEY)
        self.assertEqual(req.API_VERSION, 0)
        self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.transaction_timeout_ms, 30000)

    @run_until_complete
    async def test_sender__do_init_pid_handle_ok(self):
        sender = await self._setup_sender(no_init=True)
        init_handler = InitPIDHandler(sender)

        # Handle response
        self.assertEqual(sender._txn_manager.producer_id, -1)
        self.assertEqual(sender._txn_manager.producer_epoch, -1)
        cls = InitProducerIdResponse_v0
        resp = cls(
            throttle_time_ms=300,
            error_code=NoError.errno,
            producer_id=17,
            producer_epoch=1,
        )
        backoff = init_handler.handle_response(resp)
        self.assertIsNone(backoff)
        self.assertEqual(sender._txn_manager.producer_id, 17)
        self.assertEqual(sender._txn_manager.producer_epoch, 1)

    @run_until_complete
    async def test_sender__do_init_pid_handle_not_ok(self):
        sender = await self._setup_sender(no_init=True)
        init_handler = InitPIDHandler(sender)

        self.assertEqual(sender._txn_manager.producer_id, -1)
        self.assertEqual(sender._txn_manager.producer_epoch, -1)

        # Handle coordination errors
        for error_cls in [CoordinatorNotAvailableError, NotCoordinatorError]:
            with mock.patch.object(sender, "_coordinator_dead") as mocked:
                cls = InitProducerIdResponse_v0
                resp = cls(
                    throttle_time_ms=300,
                    error_code=error_cls.errno,
                    producer_id=17,
                    producer_epoch=1,
                )
                backoff = init_handler.handle_response(resp)
                self.assertEqual(backoff, 0.1)
                self.assertEqual(sender._txn_manager.producer_id, -1)
                self.assertEqual(sender._txn_manager.producer_epoch, -1)

                mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Handle node error
        with mock.patch.object(sender, "_coordinator_dead") as mocked:
            backoff = init_handler.handle_error()
            self.assertEqual(backoff, 0.1)
            mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Not coordination errors
        for error_cls in [CoordinatorLoadInProgressError, ConcurrentTransactions]:
            cls = InitProducerIdResponse_v0
            resp = cls(
                throttle_time_ms=300,
                error_code=error_cls.errno,
                producer_id=17,
                producer_epoch=1,
            )
            backoff = init_handler.handle_response(resp)
            self.assertEqual(backoff, 0.1)
            self.assertEqual(sender._txn_manager.producer_id, -1)
            self.assertEqual(sender._txn_manager.producer_epoch, -1)

        # Handle unknown error
        cls = InitProducerIdResponse_v0
        resp = cls(
            throttle_time_ms=300,
            error_code=UnknownError.errno,
            producer_id=17,
            producer_epoch=1,
        )
        with self.assertRaises(UnknownError):
            init_handler.handle_response(resp)
        self.assertEqual(sender._txn_manager.producer_id, -1)
        self.assertEqual(sender._txn_manager.producer_epoch, -1)

    @run_until_complete
    async def test_sender__do_add_partitions_to_txn_create(self):
        sender = await self._setup_sender()
        tps = [
            TopicPartition("topic", 0),
            TopicPartition("topic", 1),
            TopicPartition("topic2", 1),
        ]
        add_handler = AddPartitionsToTxnHandler(sender, tps)

        req = add_handler.create_request().prepare(
            {AddPartitionsToTxnRequest.API_KEY: (0, 0)}
        )
        self.assertEqual(req.API_KEY, AddPartitionsToTxnRequest.API_KEY)
        self.assertEqual(req.API_VERSION, 0)
        self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.producer_id, 120)
        self.assertEqual(req.producer_epoch, 22)
        self.assertEqual(
            sorted(req.topics), sorted([("topic", [0, 1]), ("topic2", [1])])
        )

    @run_until_complete
    async def test_sender__do_add_partitions_to_txn_ok(self):
        sender = await self._setup_sender()
        tps = [
            TopicPartition("topic", 0),
            TopicPartition("topic", 1),
            TopicPartition("topic2", 1),
        ]
        add_handler = AddPartitionsToTxnHandler(sender, tps)
        tm = sender._txn_manager
        tm.partition_added = mock.Mock()

        # Handle response
        cls = AddPartitionsToTxnResponse_v0
        resp = cls(
            throttle_time_ms=300,
            errors=[
                (
                    "topic",
                    [
                        (0, NoError.errno),
                        (1, NoError.errno),
                    ],
                ),
                (
                    "topic2",
                    [
                        (1, NoError.errno),
                    ],
                ),
            ],
        )
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        self.assertEqual(tm.partition_added.call_count, 3)
        tm.partition_added.assert_has_calls([mock.call(tp) for tp in tps])

    @run_until_complete
    async def test_sender__do_add_partitions_to_txn_not_ok(self):
        sender = await self._setup_sender()
        tps = [
            TopicPartition("topic", 0),
            TopicPartition("topic", 1),
            TopicPartition("topic2", 1),
        ]
        add_handler = AddPartitionsToTxnHandler(sender, tps)
        tm = sender._txn_manager
        tm.begin_transaction()
        tm.partition_added = mock.Mock()

        def create_response(error_type):
            cls = AddPartitionsToTxnResponse_v0
            resp = cls(
                throttle_time_ms=300,
                errors=[
                    (
                        "topic",
                        [
                            (0, error_type.errno),
                            (1, error_type.errno),
                        ],
                    ),
                    (
                        "topic2",
                        [
                            (1, error_type.errno),
                        ],
                    ),
                ],
            )
            return resp

        # Handle coordination errors
        for error_cls in [CoordinatorNotAvailableError, NotCoordinatorError]:
            with mock.patch.object(sender, "_coordinator_dead") as mocked:
                resp = create_response(error_cls)
                backoff = add_handler.handle_response(resp)
                self.assertEqual(backoff, 0.1)
                tm.partition_added.assert_not_called()
                mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Handle node error
        with mock.patch.object(sender, "_coordinator_dead") as mocked:
            backoff = add_handler.handle_error()
            self.assertEqual(backoff, 0.1)
            mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Special case for ConcurrentTransactions
        resp = create_response(ConcurrentTransactions)
        backoff = add_handler.handle_response(resp)
        self.assertEqual(backoff, 0.02)  # BACKOFF_OVERRIDE
        tm.partition_added.assert_not_called()

        # Special case for ConcurrentTransactions if this is not the first call
        tm._txn_partitions.add(tps[0])
        resp = create_response(ConcurrentTransactions)
        backoff = add_handler.handle_response(resp)
        self.assertEqual(backoff, 0.1)  # Default backoff
        tm.partition_added.assert_not_called()

        # Not coordination retriable errors
        for error_cls in [CoordinatorLoadInProgressError, UnknownTopicOrPartitionError]:
            resp = create_response(error_cls)
            backoff = add_handler.handle_response(resp)
            self.assertEqual(backoff, 0.1)
            tm.partition_added.assert_not_called()

        # ProducerFenced case
        resp = create_response(InvalidProducerEpoch)
        with self.assertRaises(ProducerFenced):
            add_handler.handle_response(resp)
        tm.partition_added.assert_not_called()

        for error_type in [InvalidProducerIdMapping, InvalidTxnState]:
            resp = create_response(error_type)
            with self.assertRaises(error_type):
                add_handler.handle_response(resp)
            tm.partition_added.assert_not_called()

        # Handle unknown error
        resp = create_response(UnknownError)
        with self.assertRaises(UnknownError):
            add_handler.handle_response(resp)
        tm.partition_added.assert_not_called()

        # Handle TransactionalIdAuthorizationFailed
        resp = create_response(TransactionalIdAuthorizationFailed)
        with self.assertRaises(TransactionalIdAuthorizationFailed) as cm:
            add_handler.handle_response(resp)
        tm.partition_added.assert_not_called()
        self.assertEqual(cm.exception.args[0], "test_tid")

        # TopicAuthorizationFailedError case
        resp = create_response(OperationNotAttempted)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.partition_added.assert_not_called()

        # TopicAuthorizationFailedError case
        self.assertNotEqual(tm.state, TransactionState.ABORTABLE_ERROR)
        resp = create_response(TopicAuthorizationFailedError)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.partition_added.assert_not_called()
        self.assertEqual(tm.state, TransactionState.ABORTABLE_ERROR)

    @run_until_complete
    async def test_sender__do_add_offsets_to_txn_create(self):
        sender = await self._setup_sender()
        add_handler = AddOffsetsToTxnHandler(sender, "some_group")

        req = add_handler.create_request().prepare(
            {AddOffsetsToTxnRequest.API_KEY: (0, 0)}
        )
        self.assertEqual(req.API_KEY, AddOffsetsToTxnRequest.API_KEY)
        self.assertEqual(req.API_VERSION, 0)
        self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.producer_id, 120)
        self.assertEqual(req.producer_epoch, 22)
        self.assertEqual(req.group_id, "some_group")

    @run_until_complete
    async def test_sender__do_add_offsets_to_txn_ok(self):
        sender = await self._setup_sender()
        add_handler = AddOffsetsToTxnHandler(sender, "some_group")
        tm = sender._txn_manager
        tm.consumer_group_added = mock.Mock()

        # Handle response
        cls = AddOffsetsToTxnResponse_v0
        resp = cls(throttle_time_ms=300, error_code=NoError.errno)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.consumer_group_added.assert_called_with("some_group")

    @run_until_complete
    async def test_sender__do_add_offsets_to_txn_not_ok(self):
        sender = await self._setup_sender()
        add_handler = AddOffsetsToTxnHandler(sender, "some_group")
        tm = sender._txn_manager
        tm.begin_transaction()
        tm.consumer_group_added = mock.Mock()

        def create_response(error_type):
            cls = AddOffsetsToTxnResponse_v0
            resp = cls(throttle_time_ms=300, error_code=error_type.errno)
            return resp

        # Handle coordination errors
        for error_cls in [CoordinatorNotAvailableError, NotCoordinatorError]:
            with mock.patch.object(sender, "_coordinator_dead") as mocked:
                resp = create_response(error_cls)
                backoff = add_handler.handle_response(resp)
                self.assertEqual(backoff, 0.1)
                tm.consumer_group_added.assert_not_called()
                mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Handle node error
        with mock.patch.object(sender, "_coordinator_dead") as mocked:
            backoff = add_handler.handle_error()
            self.assertEqual(backoff, 0.1)
            mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Not coordination retriable errors
        for error_cls in [CoordinatorLoadInProgressError, ConcurrentTransactions]:
            resp = create_response(error_cls)
            backoff = add_handler.handle_response(resp)
            self.assertEqual(backoff, 0.1)
            tm.consumer_group_added.assert_not_called()

        # ProducerFenced case
        resp = create_response(InvalidProducerEpoch)
        with self.assertRaises(ProducerFenced):
            add_handler.handle_response(resp)
        tm.consumer_group_added.assert_not_called()

        for error_type in [InvalidTxnState]:
            resp = create_response(error_type)
            with self.assertRaises(error_type):
                add_handler.handle_response(resp)
            tm.consumer_group_added.assert_not_called()

        # Handle unknown error
        resp = create_response(UnknownError)
        with self.assertRaises(UnknownError):
            add_handler.handle_response(resp)
        tm.consumer_group_added.assert_not_called()

        # Handle authorization error
        resp = create_response(TransactionalIdAuthorizationFailed)
        with self.assertRaises(TransactionalIdAuthorizationFailed) as cm:
            add_handler.handle_response(resp)
        tm.consumer_group_added.assert_not_called()
        self.assertEqual(cm.exception.args[0], "test_tid")

        # Handle group error
        self.assertNotEqual(tm.state, TransactionState.ABORTABLE_ERROR)
        resp = create_response(GroupAuthorizationFailedError)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.consumer_group_added.assert_not_called()
        self.assertEqual(tm.state, TransactionState.ABORTABLE_ERROR)

    @run_until_complete
    async def test_sender__do_txn_offset_commit_create(self):
        sender = await self._setup_sender()
        offsets = {
            TopicPartition("topic", 0): OffsetAndMetadata(10, ""),
            TopicPartition("topic", 1): OffsetAndMetadata(11, ""),
        }
        add_handler = TxnOffsetCommitHandler(sender, offsets, "some_group")

        req = add_handler.create_request().prepare(
            {TxnOffsetCommitRequest.API_KEY: (0, 0)}
        )
        self.assertEqual(req.API_KEY, TxnOffsetCommitRequest.API_KEY)
        self.assertEqual(req.API_VERSION, 0)
        self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.group_id, "some_group")
        self.assertEqual(req.producer_id, 120)
        self.assertEqual(req.producer_epoch, 22)
        self.assertEqual(req.topics, [("topic", [(0, 10, ""), (1, 11, "")])])

    @run_until_complete
    async def test_sender__do_txn_offset_commit_ok(self):
        sender = await self._setup_sender()
        offsets = {
            TopicPartition("topic", 0): OffsetAndMetadata(10, ""),
            TopicPartition("topic", 1): OffsetAndMetadata(11, ""),
        }
        add_handler = TxnOffsetCommitHandler(sender, offsets, "some_group")
        tm = sender._txn_manager
        tm.offset_committed = mock.Mock()

        # Handle response
        cls = TxnOffsetCommitResponse_v0
        resp = cls(
            throttle_time_ms=300,
            errors=[("topic", [(0, NoError.errno), (1, NoError.errno)])],
        )
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        self.assertEqual(tm.offset_committed.call_count, 2)
        tm.offset_committed.assert_has_calls(
            [
                mock.call(TopicPartition("topic", 0), 10, "some_group"),
                mock.call(TopicPartition("topic", 1), 11, "some_group"),
            ]
        )

    @run_until_complete
    async def test_sender__do_txn_offset_commit_not_ok(self):
        sender = await self._setup_sender()
        offsets = {
            TopicPartition("topic", 0): OffsetAndMetadata(10, ""),
            TopicPartition("topic", 1): OffsetAndMetadata(11, ""),
        }
        add_handler = TxnOffsetCommitHandler(sender, offsets, "some_group")
        tm = sender._txn_manager
        tm.begin_transaction()
        tm.offset_committed = mock.Mock()

        def create_response(error_type):
            cls = TxnOffsetCommitResponse_v0
            resp = cls(
                throttle_time_ms=300,
                errors=[("topic", [(0, error_type.errno), (1, error_type.errno)])],
            )
            return resp

        # Handle coordination errors
        for error_cls in [
            CoordinatorNotAvailableError,
            NotCoordinatorError,
            RequestTimedOutError,
        ]:
            with mock.patch.object(sender, "_coordinator_dead") as mocked:
                resp = create_response(error_cls)
                backoff = add_handler.handle_response(resp)
                self.assertEqual(backoff, 0.1)
                tm.offset_committed.assert_not_called()
                mocked.assert_called_with(CoordinationType.GROUP)

        # Handle node error
        with mock.patch.object(sender, "_coordinator_dead") as mocked:
            backoff = add_handler.handle_error()
            self.assertEqual(backoff, 0.1)
            mocked.assert_called_with(CoordinationType.GROUP)

        # Not coordination retriable errors
        for error_cls in [CoordinatorLoadInProgressError, UnknownTopicOrPartitionError]:
            resp = create_response(error_cls)
            backoff = add_handler.handle_response(resp)
            self.assertEqual(backoff, 0.1)
            tm.offset_committed.assert_not_called()

        # ProducerFenced case
        resp = create_response(InvalidProducerEpoch)
        with self.assertRaises(ProducerFenced):
            add_handler.handle_response(resp)
        tm.offset_committed.assert_not_called()

        # TransactionalIdAuthorizationFailed case
        resp = create_response(TransactionalIdAuthorizationFailed)
        with self.assertRaises(TransactionalIdAuthorizationFailed) as cm:
            add_handler.handle_response(resp)
        tm.offset_committed.assert_not_called()
        self.assertEqual(cm.exception.args[0], "test_tid")

        # Handle unknown error
        resp = create_response(UnknownError)
        with self.assertRaises(UnknownError):
            add_handler.handle_response(resp)
        tm.offset_committed.assert_not_called()

        # GroupAuthorizationFailedError case
        self.assertNotEqual(tm.state, TransactionState.ABORTABLE_ERROR)
        resp = create_response(GroupAuthorizationFailedError)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.offset_committed.assert_not_called()
        with self.assertRaises(GroupAuthorizationFailedError) as cm:
            tm.committing_transaction()
        self.assertEqual(cm.exception.args[0], "some_group")
        self.assertEqual(tm.state, TransactionState.ABORTABLE_ERROR)

    @run_until_complete
    async def test_sender__do_end_txn_create(self):
        sender = await self._setup_sender()
        add_handler = EndTxnHandler(sender, 0)

        req = add_handler.create_request().prepare({EndTxnRequest.API_KEY: (0, 0)})
        self.assertEqual(req.API_KEY, EndTxnRequest.API_KEY)
        self.assertEqual(req.API_VERSION, 0)
        self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.producer_id, 120)
        self.assertEqual(req.producer_epoch, 22)
        self.assertEqual(req.transaction_result, 0)

    @run_until_complete
    async def test_sender__do_end_txn_ok(self):
        sender = await self._setup_sender()
        add_handler = EndTxnHandler(sender, 0)
        tm = sender._txn_manager
        tm.complete_transaction = mock.Mock()

        # Handle response
        cls = EndTxnResponse_v0
        resp = cls(throttle_time_ms=300, error_code=NoError.errno)
        backoff = add_handler.handle_response(resp)
        self.assertIsNone(backoff)
        tm.complete_transaction.assert_called_with()

    @run_until_complete
    async def test_sender__do_end_txn_not_ok(self):
        sender = await self._setup_sender()
        add_handler = EndTxnHandler(sender, 0)
        tm = sender._txn_manager
        tm.complete_transaction = mock.Mock()

        def create_response(error_type):
            cls = EndTxnResponse_v0
            resp = cls(throttle_time_ms=300, error_code=error_type.errno)
            return resp

        # Handle coordination errors
        for error_cls in [CoordinatorNotAvailableError, NotCoordinatorError]:
            with mock.patch.object(sender, "_coordinator_dead") as mocked:
                resp = create_response(error_cls)
                backoff = add_handler.handle_response(resp)
                self.assertEqual(backoff, 0.1)
                tm.complete_transaction.assert_not_called()
                mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Handle node error
        with mock.patch.object(sender, "_coordinator_dead") as mocked:
            backoff = add_handler.handle_error()
            self.assertEqual(backoff, 0.1)
            mocked.assert_called_with(CoordinationType.TRANSACTION)

        # Not coordination retriable errors
        for error_cls in [CoordinatorLoadInProgressError, ConcurrentTransactions]:
            resp = create_response(error_cls)
            backoff = add_handler.handle_response(resp)
            self.assertEqual(backoff, 0.1)
            tm.complete_transaction.assert_not_called()

        # ProducerFenced case
        resp = create_response(InvalidProducerEpoch)
        with self.assertRaises(ProducerFenced):
            add_handler.handle_response(resp)
        tm.complete_transaction.assert_not_called()

        for error_type in [InvalidTxnState]:
            resp = create_response(error_type)
            with self.assertRaises(error_type):
                add_handler.handle_response(resp)
            tm.complete_transaction.assert_not_called()

        # Handle unknown error
        resp = create_response(UnknownError)
        with self.assertRaises(UnknownError):
            add_handler.handle_response(resp)
        tm.complete_transaction.assert_not_called()

    @run_until_complete
    async def test_sender__produce_request_create(self):
        sender = await self._setup_sender()
        tp = TopicPartition("my_topic", 0)
        batch_mock = mock.Mock()
        batch_mock.get_data_buffer = mock.Mock(return_value=b"123")
        send_handler = SendProduceReqHandler(sender, {tp: batch_mock})

        req = send_handler.create_request().prepare({ProduceRequest.API_KEY: (0, 4)})
        self.assertEqual(req.API_KEY, ProduceRequest.API_KEY)
        if req.API_VERSION > 2:
            self.assertEqual(req.transactional_id, "test_tid")
        self.assertEqual(req.required_acks, -1)
        self.assertEqual(req.timeout, 40000)
        self.assertEqual(req.topics, [("my_topic", [(0, b"123")])])

    @run_until_complete
    async def test_sender__produce_request_ok(self):
        sender = await self._setup_sender()
        tp = TopicPartition("my_topic", 0)
        batch_mock = mock.Mock()
        send_handler = SendProduceReqHandler(sender, {tp: batch_mock})

        def create_response(error_type):
            cls = ProduceResponse_v4
            resp = cls(
                throttle_time_ms=300,
                topics=[
                    (
                        "my_topic",
                        [
                            (0, error_type.errno, 100, 200),
                        ],
                    ),
                ],
            )
            return resp

        # Special case for DuplicateSequenceNumber
        resp = create_response(NoError)
        send_handler.handle_response(resp)
        batch_mock.done.assert_called_with(100, 200, None)
        self.assertEqual(send_handler._to_reenqueue, [])

    @run_until_complete
    async def test_sender__produce_request_not_ok(self):
        sender = await self._setup_sender()
        tp = TopicPartition("my_topic", 0)
        batch_mock = mock.Mock()
        send_handler = SendProduceReqHandler(sender, {tp: batch_mock})

        def create_response(error_type):
            cls = ProduceResponse_v4
            resp = cls(
                throttle_time_ms=300,
                topics=[("my_topic", [(0, error_type.errno, 0, -1)])],
            )
            return resp

        # Special case for DuplicateSequenceNumber
        resp = create_response(DuplicateSequenceNumber)
        send_handler.handle_response(resp)
        batch_mock.done.assert_called_with(0, -1, None)
        batch_mock.failure.assert_not_called()
        self.assertEqual(send_handler._to_reenqueue, [])

        batch_mock.reset_mock()

        # Special case for InvalidProducerEpoch
        resp = create_response(InvalidProducerEpoch)
        send_handler.handle_response(resp)
        batch_mock.done.assert_not_called()
        self.assertNotEqual(batch_mock.failure.call_count, 0)
        self.assertEqual(send_handler._to_reenqueue, [])
