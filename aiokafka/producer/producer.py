from __future__ import annotations

import asyncio
import logging
import sys
import traceback
import warnings
from ssl import SSLContext
from types import ModuleType, TracebackType
from typing import Callable, Generic, Iterable, Literal, TypeVar

from aiokafka.abc import AbstractTokenProvider
from aiokafka.client import AIOKafkaClient
from aiokafka.codec import has_gzip, has_lz4, has_snappy, has_zstd
from aiokafka.errors import (
    IllegalOperation,
    MessageSizeTooLargeError,
    UnsupportedVersionError,
)
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.record.default_records import DefaultRecordBatch
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.structs import OffsetAndMetadata, RecordMetadata, TopicPartition
from aiokafka.util import (
    INTEGER_MAX_VALUE,
    commit_structure_validate,
    create_task,
    get_running_loop,
)

from .message_accumulator import BatchBuilder, MessageAccumulator
from .sender import Sender
from .transaction_manager import TransactionManager

log = logging.getLogger(__name__)

_missing = object()

def _identity(data: bytes) -> bytes:
    return data


_DEFAULT_PARTITIONER = DefaultPartitioner()


KT = TypeVar("KT", contravariant=True)
VT = TypeVar("VT", contravariant=True)
ET = TypeVar("ET", bound=BaseException)


class AIOKafkaProducer(Generic[KT, VT]):
    """A Kafka client that publishes records to the Kafka cluster.

    The producer consists of a pool of buffer space that holds records that
    haven't yet been transmitted to the server as well as a background task
    that is responsible for turning these records into requests and
    transmitting them to the cluster.

    The :meth:`send` method is asynchronous. When called it adds the record to a
    buffer of pending record sends and immediately returns. This allows the
    producer to batch together individual records for efficiency.

    The `acks` config controls the criteria under which requests are considered
    complete. The ``all`` setting will result in waiting for all replicas to
    respond, the slowest but most durable setting.

    The `key_serializer` and `value_serializer` instruct how to turn the key and
    value objects the user provides into :class:`bytes`.

    Arguments:
        bootstrap_servers (str, list(str)): a ``host[:port]`` string or list of
            ``host[:port]`` strings that the producer should contact to
            bootstrap initial cluster metadata. This does not have to be the
            full node list.  It just needs to have at least one broker that will
            respond to a Metadata API Request. Default port is 9092. If no
            servers are specified, will default to ``localhost:9092``.
        client_id (str or None): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client.
            If ``None`` ``aiokafka-producer-#`` (appended with a unique number
            per instance) is used.
            Default: :data:`None`
        key_serializer (Callable[[KT], bytes]): used to convert user-supplied keys
            to bytes. If not :data:`None`, called as ``f(key),`` should return
            :class:`bytes`.
            Default: :data:`None`.
        value_serializer (Callable[[VT], bytes]): used to convert user-supplied
            message values to :class:`bytes`. If not :data:`None`, called as
            ``f(value)``, should return :class:`bytes`.
            Default: :data:`None`.
        acks (Any): one of ``0``, ``1``, ``all``. The number of acknowledgments
            the producer requires the leader to have received before considering a
            request complete. This controls the durability of records that are
            sent. The following settings are common:

            * ``0``: Producer will not wait for any acknowledgment from the server
              at all. The message will immediately be added to the socket
              buffer and considered sent. No guarantee can be made that the
              server has received the record in this case, and the retries
              configuration will not take effect (as the client won't
              generally know of any failures). The offset given back for each
              record will always be set to -1.
            * ``1``: The broker leader will write the record to its local log but
              will respond without awaiting full acknowledgement from all
              followers. In this case should the leader fail immediately
              after acknowledging the record but before the followers have
              replicated it then the record will be lost.
            * ``all``: The broker leader will wait for the full set of in-sync
              replicas to acknowledge the record. This guarantees that the
              record will not be lost as long as at least one in-sync replica
              remains alive. This is the strongest available guarantee.

            If unset, defaults to ``acks=1``. If `enable_idempotence` is
            :data:`True` defaults to ``acks=all``
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are ``gzip``, ``snappy``, ``lz4``, ``zstd``
            or :data:`None`.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: :data:`None`.
        max_batch_size (int): Maximum size of buffered data per partition.
            After this amount :meth:`send` coroutine will block until batch is
            drained.
            Default: 16384
        linger_ms (int): The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, if first request is processed faster,
            than `linger_ms`, producer will wait ``linger_ms - process_time``.
            Default: 0 (i.e. no delay).
        partitioner (Callable): Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            ``partitioner(key_bytes, all_partitions, available_partitions)``.
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the Java client so that
            messages with the same key are assigned to the same partition.
            When a key is :data:`None`, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        request_timeout_ms (int): Produce request timeout in milliseconds.
            As it's sent as part of
            :class:`~aiokafka.protocol.produce.ProduceRequest` (it's a blocking
            call), maximum waiting time can be up to ``2 *
            request_timeout_ms``.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        api_version (str): specify which kafka API version to use.
            If set to ``auto``, will attempt to infer the broker version by
            probing various APIs. Default: ``auto``
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``,
            ``SASL_SSL``. Default: ``PLAINTEXT``.
        ssl_context (ssl.SSLContext): pre-configured :class:`~ssl.SSLContext`
            for wrapping socket connections. Directly passed into asyncio's
            :meth:`~asyncio.loop.create_connection`. For more
            information see :ref:`ssl_auth`.
            Default: :data:`None`
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying :data:`None` will
            disable idle checks. Default: 540000 (9 minutes).
        enable_idempotence (bool): When set to :data:`True`, the producer will
            ensure that exactly one copy of each message is written in the
            stream. If :data:`False`, producer retries due to broker failures,
            etc., may write duplicates of the retried message in the stream.
            Note that enabling idempotence acks to set to ``all``. If it is not
            explicitly set by the user it will be chosen. If incompatible
            values are set, a :exc:`ValueError` will be thrown.
            New in version 0.5.0.
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for ``SASL_PLAINTEXT`` or ``SASL_SSL``. Valid values
            are: ``PLAIN``, ``GSSAPI``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``,
            ``OAUTHBEARER``.
            Default: ``PLAIN``
        sasl_plain_username (str): username for SASL ``PLAIN`` authentication.
            Default: :data:`None`
        sasl_plain_password (str): password for SASL ``PLAIN`` authentication.
            Default: :data:`None`
        sasl_oauth_token_provider (:class:`~aiokafka.abc.AbstractTokenProvider`):
            OAuthBearer token provider instance.
            Default: :data:`None`

    Note:
        Many configuration parameters are taken from the Java client:
        https://kafka.apache.org/documentation.html#producerconfigs
    """

    _PRODUCER_CLIENT_ID_SEQUENCE = 0

    _COMPRESSORS = {
        "gzip": (has_gzip, DefaultRecordBatch.CODEC_GZIP),
        "snappy": (has_snappy, DefaultRecordBatch.CODEC_SNAPPY),
        "lz4": (has_lz4, DefaultRecordBatch.CODEC_LZ4),
        "zstd": (has_zstd, DefaultRecordBatch.CODEC_ZSTD),
    }

    _closed = None  # Serves as an uninitialized flag for __del__
    _source_traceback = None

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop | None=None,
        bootstrap_servers: str | list[str]="localhost",
        client_id: str | None=None,
        metadata_max_age_ms: int=300000,
        request_timeout_ms: int=40000,
        api_version: str="auto",
        acks: Literal[0] | Literal[1] | Literal["all"] | object=_missing,
        key_serializer: Callable[[KT], bytes]=_identity,
        value_serializer: Callable[[VT], bytes]=_identity,
        compression_type: Literal["gzip"] | Literal["snappy"] | Literal["lz4"] | Literal["zstd"] | None=None,
        max_batch_size: int=16384,
        partitioner: Callable[[bytes, list[int], list[int]], int]=_DEFAULT_PARTITIONER,
        max_request_size: int=1048576,
        linger_ms: int=0,
        retry_backoff_ms: int=100,
        security_protocol: Literal["PLAINTEXT"] | Literal["SSL"] | Literal["SASL_PLAINTEXT"] | Literal["SASL_SSL"]="PLAINTEXT",
        ssl_context: SSLContext | None=None,
        connections_max_idle_ms: int=540000,
        enable_idempotence: bool=False,
        transactional_id: int | str | None=None, # In theory, this could be any unique object
        transaction_timeout_ms: int=60000,
        sasl_mechanism: Literal["PLAIN"] | Literal["GSSAPI"] | Literal["SCRAM-SHA-256"] | Literal["SCRAM-SHA-512"] | Literal["OAUTHBEARER"]="PLAIN",
        sasl_plain_password: str | None=None,
        sasl_plain_username: str | None=None,
        sasl_kerberos_service_name: str="kafka",
        sasl_kerberos_domain_name: str | None=None,
        sasl_oauth_token_provider: AbstractTokenProvider | None=None,
    ):
        if loop is None:
            loop = get_running_loop()
        else:
            warnings.warn(
                "The loop argument is deprecated since 0.7.1, "
                "and scheduled for removal in 0.9.0",
                DeprecationWarning,
                stacklevel=2,
            )
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._loop = loop

        if acks not in (0, 1, -1, "all", _missing):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ("gzip", "snappy", "lz4", "zstd", None):
            raise ValueError("Invalid compression type!")
        if compression_type:
            checker, compression_attrs = self._COMPRESSORS[compression_type]
            if not checker():
                raise RuntimeError(
                    f"Compression library for {compression_type} not found"
                )
        else:
            compression_attrs = 0

        if transactional_id is not None:
            enable_idempotence = True
        else:
            transaction_timeout_ms = INTEGER_MAX_VALUE

        if enable_idempotence:
            if acks is _missing:
                acks = -1
            elif acks not in ("all", -1):
                raise ValueError(
                    f"acks={acks} not supported if enable_idempotence=True"
                )
            self._txn_manager = TransactionManager(
                transactional_id, transaction_timeout_ms
            )
        else:
            self._txn_manager = None

        if acks is _missing:
            acks = 1
        elif acks == "all":
            acks = -1

        AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE += 1
        if client_id is None:
            client_id = (
                f"aiokafka-producer-{AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE}"
            )

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._compression_type = compression_type
        self._partitioner = partitioner
        self._max_request_size = max_request_size
        self._request_timeout_ms = request_timeout_ms

        self.client = AIOKafkaClient(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version,
            security_protocol=security_protocol,
            ssl_context=ssl_context,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider,
        )
        self._metadata = self.client.cluster
        self._message_accumulator = MessageAccumulator(
            self._metadata,
            max_batch_size,
            compression_attrs,
            self._request_timeout_ms / 1000,
            txn_manager=self._txn_manager,
            loop=loop,
        )
        self._sender = Sender(
            self.client,
            acks=acks,
            txn_manager=self._txn_manager,
            retry_backoff_ms=retry_backoff_ms,
            linger_ms=linger_ms,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=request_timeout_ms,
        )

        self._closed = False

    # Warn if producer was not closed properly
    # We don't attempt to close the Consumer, as __del__ is synchronous
    def __del__(self, _warnings: ModuleType=warnings) -> None:
        if self._closed is False:
            _warnings.warn(
                f"Unclosed AIOKafkaProducer {self!r}",
                ResourceWarning,
                source=self,
            )
            context = {
                "producer": self,
                "message": "Unclosed AIOKafkaProducer",
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def start(self) -> None:
        """Connect to Kafka cluster and check server version"""
        assert (
            self._loop is get_running_loop()
        ), "Please create objects with the same loop as running with"
        log.debug("Starting the Kafka producer")  # trace
        await self.client.bootstrap()

        if self._compression_type == "lz4":
            assert self.client.api_version >= (0, 8, 2), (
                "LZ4 Requires >= Kafka 0.8.2 Brokers"
            )  # fmt: skip
        elif self._compression_type == "zstd":
            assert self.client.api_version >= (2, 1, 0), (
                "Zstd Requires >= Kafka 2.1.0 Brokers"
            )  # fmt: skip

        if self._txn_manager is not None and self.client.api_version < (0, 11):
            raise UnsupportedVersionError(
                "Idempotent producer available only for Broker version 0.11"
                " and above"
            )

        await self._sender.start()
        self._message_accumulator.set_api_version(self.client.api_version)
        self._producer_magic = 0 if self.client.api_version < (0, 10) else 1
        log.debug("Kafka producer started")

    async def flush(self) -> None:
        """Wait until all batches are Delivered and futures resolved"""
        await self._message_accumulator.flush()

    async def stop(self) -> None:
        """Flush all pending data and close all connections to kafka cluster"""
        if self._closed:
            return
        self._closed = True

        # If the sender task is down there is no way for accumulator to flush
        if self._sender is not None and self._sender.sender_task is not None:
            await asyncio.wait(
                [
                    create_task(self._message_accumulator.close()),
                    self._sender.sender_task,
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            await self._sender.close()

        await self.client.close()
        log.debug("The Kafka producer has closed.")

    async def partitions_for(self, topic: str) -> set[int]:
        """Returns set of all known partitions for the topic."""
        return await self.client._wait_on_metadata(topic)

    def _serialize(self, topic: str, key: KT, value: VT):
        if self._key_serializer is None:
            serialized_key = key
        else:
            serialized_key = self._key_serializer(key)
        if self._value_serializer is None:
            serialized_value = value
        else:
            serialized_value = self._value_serializer(value)

        message_size = LegacyRecordBatchBuilder.record_overhead(self._producer_magic)
        if serialized_key is not None:
            message_size += len(serialized_key)
        if serialized_value is not None:
            message_size += len(serialized_value)
        if message_size > self._max_request_size:
            raise MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % message_size
            )

        return serialized_key, serialized_value

    def _partition(
        self, topic: str, partition: int, key: KT, value: VT, serialized_key: bytes, serialized_value: bytes
    ) -> int:
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(
                topic
            ), "Unrecognized partition"
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self._partitioner(serialized_key, all_partitions, available)

    async def send(
        self,
        topic: str,
        value: VT | None=None,
        key: KT | None=None,
        partition: int | None=None,
        timestamp_ms: int | None=None,
        headers: Iterable[tuple[str, bytes]] | None=None,
    ) -> asyncio.Future[RecordMetadata]:
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (Optional): message value. Must be type :class:`bytes`, or be
                serializable to :class:`bytes` via configured `value_serializer`. If
                value is :data:`None`, key is required and message acts as a
                ``delete``.

                See `Kafka compaction documentation
                <https://kafka.apache.org/documentation.html#compaction>`__ for
                more details. (compaction requires kafka >= 0.8.1)
            partition (int, Optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                `partitioner`.
            key (Optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is :data:`None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is :data:`None`, partition is chosen randomly).
                Must be type :class:`bytes`, or be serializable to bytes via configured
                `key_serializer`.
            timestamp_ms (int, Optional): epoch milliseconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.
            headers (Optional): Kafka headers to be included in the message using
                the format ``[("key", b"value")]``. Iterable of tuples where key
                is a normal string and value is a byte string.

        Returns:
            asyncio.Future: object that will be set when message is
            processed

        Raises:
            ~aiokafka.errors.KafkaTimeoutError: if we can't schedule this record
                (pending buffer is full) in up to `request_timeout_ms`
                milliseconds.

        Note:
            The returned future will wait based on `request_timeout_ms`
            setting. Cancelling the returned future **will not** stop event
            from being sent, but cancelling the :meth:`send` coroutine itself
            **will**.
        """
        assert value is not None or self.client.api_version >= (0, 8, 1), (
            "Null messages require kafka >= 0.8.1"
        )  # fmt: skip
        assert not (value is None and key is None), "Need at least one: key or value"

        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)

        # Ensure transaction is started and not committing
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if (
                txn_manager.transactional_id is not None
                and not self._txn_manager.is_in_transaction()
            ):
                raise IllegalOperation("Can't send messages while not in transaction")

        if headers is not None:
            if self.client.api_version < (0, 11):
                raise UnsupportedVersionError("Headers not supported before Kafka 0.11")
        else:
            # Record parser/builder support only list type, no explicit None
            headers = []

        key_bytes, value_bytes = self._serialize(topic, key, value)
        partition = self._partition(
            topic, partition, key, value, key_bytes, value_bytes
        )

        tp = TopicPartition(topic, partition)
        log.debug("Sending (key=%s value=%s) to %s", key, value, tp)

        fut = await self._message_accumulator.add_message(
            tp,
            key_bytes,
            value_bytes,
            self._request_timeout_ms / 1000,
            timestamp_ms=timestamp_ms,
            headers=headers,
        )
        return fut

    async def send_and_wait(
        self,
        topic: str,
        value: VT | None=None,
        key: KT | None=None,
        partition: int | None=None,
        timestamp_ms: int | None=None,
        headers: Iterable[tuple[str, bytes]] | None=None,
    ) -> RecordMetadata:
        """Publish a message to a topic and wait the result"""
        future = await self.send(topic, value, key, partition, timestamp_ms, headers)
        return await future

    def create_batch(self) -> BatchBuilder[KT, VT]:
        """Create and return an empty :class:`.BatchBuilder`.

        The batch is not queued for send until submission to :meth:`send_batch`.

        Returns:
            BatchBuilder: empty batch to be filled and submitted by the caller.
        """
        return self._message_accumulator.create_builder(
            key_serializer=self._key_serializer, value_serializer=self._value_serializer
        )

    async def send_batch(self, batch: BatchBuilder, topic: str, *, partition: int) -> asyncio.Future[RecordMetadata]:
        """Submit a BatchBuilder for publication.

        Arguments:
            batch (BatchBuilder): batch object to be published.
            topic (str): topic where the batch will be published.
            partition (int): partition where this batch will be published.

        Returns:
            asyncio.Future: object that will be set when the batch is
                delivered.
        """
        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)
        # We only validate we have the partition in the metadata here
        partition = self._partition(topic, partition, None, None, None, None)

        # Ensure transaction is started and not committing
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if (
                txn_manager.transactional_id is not None
                and not self._txn_manager.is_in_transaction()
            ):
                raise IllegalOperation("Can't send messages while not in transaction")

        tp = TopicPartition(topic, partition)
        log.debug("Sending batch to %s", tp)
        future = await self._message_accumulator.add_batch(
            batch, tp, self._request_timeout_ms / 1000
        )
        return future

    def _ensure_transactional(self) -> None:
        if self._txn_manager is None or self._txn_manager.transactional_id is None:
            raise IllegalOperation(
                "You need to configure transaction_id to use transactions"
            )

    async def begin_transaction(self) -> None:
        self._ensure_transactional()
        log.debug(
            "Beginning a new transaction for id %s", self._txn_manager.transactional_id
        )
        await asyncio.shield(self._txn_manager.wait_for_pid())
        self._txn_manager.begin_transaction()

    async def commit_transaction(self) -> None:
        self._ensure_transactional()
        log.debug(
            "Committing transaction for id %s", self._txn_manager.transactional_id
        )
        self._txn_manager.committing_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
        )

    async def abort_transaction(self) -> None:
        self._ensure_transactional()
        log.debug("Aborting transaction for id %s", self._txn_manager.transactional_id)
        self._txn_manager.aborting_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
        )

    def transaction(self) -> TransactionContext:
        """Start a transaction context"""

        return TransactionContext(self)

    async def send_offsets_to_transaction(self, offsets: dict[TopicPartition, int | tuple[int, str] | OffsetAndMetadata], group_id: str) -> None:
        self._ensure_transactional()

        if not self._txn_manager.is_in_transaction():
            raise IllegalOperation("Not in the middle of a transaction")

        if not group_id or not isinstance(group_id, str):
            raise ValueError(group_id)

        # validate `offsets` structure
        formatted_offsets = commit_structure_validate(offsets)

        log.debug(
            "Begin adding offsets %s for consumer group %s to transaction",
            formatted_offsets,
            group_id,
        )
        fut = self._txn_manager.add_offsets_to_txn(formatted_offsets, group_id)
        await asyncio.shield(fut)

    async def __aenter__(self) -> AIOKafkaProducer[KT, VT]:
        await self.start()
        return self

    async def __aexit__(self, exc_type: type[ET] | None, exc: ET | None, tb: TracebackType | None) -> None:
        await self.stop()


class TransactionContext:
    def __init__(self, producer: AIOKafkaProducer[KT, VT]):
        self._producer = producer

    async def __aenter__(self) -> TransactionContext:
        await self._producer.begin_transaction()
        return self

    async def __aexit__(self, exc_type: type[ET] | None, exc: ET | None, tb: TracebackType | None) -> None:
        if exc_type is not None:
            # If called directly we want the API to raise a InvalidState error,
            # but when exiting a context manager we should just let it out
            if self._producer._txn_manager.is_fatal_error():
                return
            await self._producer.abort_transaction()
        else:
            await self._producer.commit_transaction()
