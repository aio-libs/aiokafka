import logging
import asyncio
from collections import defaultdict
from ssl import SSLContext
from typing import List, Optional, Dict, Tuple, Any

from aiokafka import __version__
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import IncompatibleBrokerVersion, for_code
from aiokafka.protocol.api import Request, Response
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.protocol.commit import OffsetFetchRequest, GroupCoordinatorRequest
from aiokafka.protocol.admin import (
    CreatePartitionsRequest,
    CreateTopicsRequest,
    DeleteTopicsRequest,
    DescribeGroupsRequest,
    DescribeConfigsRequest,
    AlterConfigsRequest,
    ListGroupsRequest,
    ApiVersionRequest_v0)
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from .config_resource import ConfigResourceType, ConfigResource
from .new_topic import NewTopic

log = logging.getLogger(__name__)


class AIOKafkaAdminClient:
    """A class for administering the Kafka cluster.

    .. note::

        This class is considered **experimental**, so beware that it is subject
        to changes even in patch releases.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'kafka-python-{version}'
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        connections_max_idle_ms: Close idle connections after the number of
            milliseconds specified by this config. The broker closes idle
            connections after connections.max.idle.ms, so this avoids hitting
            unexpected socket disconnected errors on the client.
            Default: 540000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): Pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        api_version (str): Specify which kafka API version to use.
            AIOKafka supports Kafka API versions >=0.9 only.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto
    """

    def __init__(self, *, loop=None,
                 bootstrap_servers: str = 'localhost',
                 client_id: str = 'aiokafka-' + __version__,
                 request_timeout_ms: int = 40000,
                 connections_max_idle_ms: int = 540000,
                 retry_backoff_ms: int = 100,
                 metadata_max_age_ms: int = 300000,
                 security_protocol: str = "PLAINTEXT",
                 ssl_context: Optional[SSLContext] = None,
                 api_version: str = "auto",
                 sasl_mechanism: str = 'PLAIN',
                 sasl_plain_username: Optional[str] = None,
                 sasl_plain_password: Optional[str] = None,
                 sasl_kerberos_service_name: str = 'kafka',
                 sasl_kerberos_domain_name: Optional[str] = None,
                 sasl_oauth_token_provider: Optional[str] = None):
        self._closed = False
        self._started = False
        self._version_info = {}
        self._request_timeout_ms = request_timeout_ms
        self._client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider)

    async def close(self):
        """Close the AIOKafkaAdminClient connection to the Kafka broker."""
        if not hasattr(self, '_closed') or self._closed:
            log.info("AIOKafkaAdminClient already closed.")
            return

        await self._client.close()
        self._closed = True
        log.debug("AIOKafkaAdminClient is now closed.")

    async def _send_request(
            self,
            request: Request,
            node_id: Optional[int] = None) -> Response:
        if node_id is None:
            node_id = self._client.get_random_node()
        return await self._client.send(node_id, request)

    async def _get_version_info(self):
        resp = await self._send_request(ApiVersionRequest_v0())
        for api_key, min_version, max_version in resp.api_versions:
            self._version_info[api_key] = (min_version, max_version)

    async def start(self):
        if self._started:
            return
        await self._client.bootstrap()
        await self._get_version_info()
        log.debug("AIOKafkaAdminClient started")
        self._started = True

    def _matching_api_version(self, operation: List[Request]) -> int:
        """Find the latest version of the protocol operation
        supported by both this library and the broker.

        This resolves to the lesser of either the latest api
        version this library supports, or the max version
        supported by the broker.

        :param operation: A list of protocol operation versions from
        aiokafka.protocol.
        :return: The max matching version number between client and broker.
        """
        api_key = operation[0].API_KEY
        if not self._version_info or api_key not in self._version_info:
            raise IncompatibleBrokerVersion(
                "Kafka broker does not support the '{}' Kafka protocol."
                .format(operation[0].__name__))
        min_version, max_version = self._version_info[api_key]
        version = min(len(operation) - 1, max_version)
        if version < min_version:
            raise IncompatibleBrokerVersion(
                "No version of the '{}' Kafka protocol is supported by "
                "both the client and broker."
                .format(operation[0].__name__))
        return version

    @staticmethod
    def _convert_new_topic_request(new_topic):
        return (
            new_topic.name,
            new_topic.num_partitions,
            new_topic.replication_factor,
            [
                (partition_id, replicas)
                for partition_id, replicas in new_topic.replica_assignments.items()
            ],
            [
                (config_key, config_value)
                for config_key, config_value in new_topic.topic_configs.items()
            ]
        )

    async def create_topics(
            self,
            new_topics: List[NewTopic],
            timeout_ms: Optional[int] = None,
            validate_only: bool = False
    ) -> Response:
        """Create new topics in the cluster.

        :param new_topics: A list of NewTopic objects.
        :param timeout_ms: Milliseconds to wait for new topics to be created
            before the broker returns.
        :param validate_only: If True, don't actually create new topics.
            Not supported by all versions. Default: False
        :return: Appropriate version of CreateTopicResponse class.
        """
        version = self._matching_api_version(CreateTopicsRequest)
        topics = [self._convert_new_topic_request(nt) for nt in new_topics]
        log.debug("Attempting to send create topic request for %r", new_topics)
        timeout_ms = timeout_ms or self._request_timeout_ms
        if version == 0:
            if validate_only:
                raise IncompatibleBrokerVersion(
                    "validate_only requires CreateTopicsRequest >= v1, "
                    "which is not supported by Kafka {}."
                    .format(self._client.api_version))
            request = CreateTopicsRequest[version](
                create_topic_requests=topics,
                timeout=timeout_ms
            )
        elif version <= 3:
            request = CreateTopicsRequest[version](
                create_topic_requests=topics,
                timeout=timeout_ms,
                validate_only=validate_only
            )
        else:
            raise NotImplementedError(
                "Support for CreateTopics v{} has not yet been added "
                "to AIOKafkaAdminClient."
                .format(version))
        response = await self._client.send(
            self._client.get_random_node(),
            request)
        return response

    async def delete_topics(
            self,
            topics: List[str],
            timeout_ms: Optional[int] = None) -> Response:
        """Delete topics from the cluster.

        :param topics: A list of topic name strings.
        :param timeout_ms: Milliseconds to wait for topics to be deleted
            before the broker returns.
        :return: Appropriate version of DeleteTopicsResponse class.
        """
        version = self._matching_api_version(DeleteTopicsRequest)
        req_cls = DeleteTopicsRequest[version]
        request = req_cls(topics, timeout_ms or self._request_timeout_ms)
        response = await self._send_request(request)
        return response

    async def _get_cluster_metadata(
            self,
            topics: Optional[List[str]] = None) -> Response:
        """
        Retrieve cluster metadata
        :param topics List of topic names, None means "get all topics"
        :return MetadataResponse
        """
        req_cls = MetadataRequest[self._matching_api_version(MetadataRequest)]
        request = req_cls(topics=topics)
        response = await self._send_request(request)
        return response

    async def list_topics(self) -> List[str]:
        metadata = await self._get_cluster_metadata(topics=None)
        obj = metadata.to_object()
        return [t['topic'] for t in obj['topics']]

    async def describe_topics(
            self,
            topics: Optional[List[str]] = None) -> List[Any]:
        metadata = await self._get_cluster_metadata(topics=topics)
        obj = metadata.to_object()
        return obj['topics']

    async def describe_cluster(self) -> Dict[str, Any]:
        metadata = await self._get_cluster_metadata()
        obj = metadata.to_object()
        obj.pop('topics')  # We have 'describe_topics' for this
        return obj

    async def describe_configs(
            self,
            config_resources: List[ConfigResource],
            include_synonyms: bool = False) -> List[Response]:
        """Fetch configuration parameters for one or more Kafka resources.

        :param config_resources: An list of ConfigResource objects.
            Any keys in ConfigResource.configs dict will be used to filter the
            result. Setting the configs dict to None will get all values. An
            empty dict will get zero values (as per Kafka protocol).
        :param include_synonyms: If True, return synonyms in response. Not
            supported by all versions. Default: False.
        :return: List of appropriate version of DescribeConfigsResponse class.
        """

        futures = []
        version = self._matching_api_version(DescribeConfigsRequest)
        if version == 0 and include_synonyms:
            raise IncompatibleBrokerVersion(
                "include_synonyms requires DescribeConfigsRequest >= v1,"
                " which is not supported by Kafka {}.".format(
                    self._client.api_version))
        broker_res, topic_res = self._convert_config_resources(
            config_resources,
            "describe"
        )
        req_cls = DescribeConfigsRequest[version]
        for broker_id in broker_res:
            if version == 0:
                req = req_cls(resources=broker_res[broker_id])
            else:
                req = req_cls(
                    resources=broker_res[broker_id],
                    include_synonyms=include_synonyms)
            futures.append(self._send_request(req, broker_id))
        if topic_res:
            if version == 0:
                req = req_cls(topic_res)
            else:
                req = req_cls(topic_res, include_synonyms)
            futures.append(self._send_request(req))
        return await asyncio.gather(*futures)

    async def alter_configs(self, config_resources: List[ConfigResource]) -> Response:
        """Alter configuration parameters of one or more Kafka resources.
        :param config_resources: A list of ConfigResource objects.
        :return: Appropriate version of AlterConfigsResponse class.
        """
        futures = []
        version = self._matching_api_version(AlterConfigsRequest)
        broker_resources, topic_resources = self._convert_config_resources(
            config_resources,
            "alter"
        )
        req_cls = AlterConfigsRequest[version]
        futures.append(self._send_request(req_cls(resources=topic_resources)))
        for broker_id in broker_resources:
            req = req_cls(resources=broker_resources[broker_id])
            futures.append(self._send_request(req, broker_id))
        return await asyncio.gather(*futures)

    @staticmethod
    def _convert_describe_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            list(config_resource.configs.keys()) if config_resource.configs else None
        )

    @staticmethod
    def _convert_alter_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            list(config_resource.configs.items())
        )

    @classmethod
    def _convert_config_resources(
            cls,
            config_resources: List[ConfigResource],
            op_type: str = "describe") -> Tuple[Dict[int, Any], List[Any]]:
        broker_resources = defaultdict(list)
        topic_resources = []
        if op_type == "describe":
            convert_func = cls._convert_describe_config_resource_request
        else:
            convert_func = cls._convert_alter_config_resource_request
        for config_resource in config_resources:
            resource = convert_func(config_resource)
            if config_resource.resource_type == ConfigResourceType.BROKER:
                broker_resources[int(resource[1])].append(resource)
            else:
                topic_resources.append(resource)
        return broker_resources, topic_resources

    @staticmethod
    def _convert_topic_partitions(topic_partitions: Dict[str, TopicPartition]):
        return [(topic_name, (new_part.total_count, new_part.new_assignments))
                for topic_name, new_part in topic_partitions.items()]

    async def create_partitions(
            self,
            topic_partitions: Dict[str, TopicPartition],
            timeout_ms: Optional[int] = None,
            validate_only: bool = False) -> Response:
        """Create additional partitions for an existing topic.

        :param topic_partitions: A map of topic name strings to NewPartition
         objects.
        :param timeout_ms: Milliseconds to wait for new partitions to be
            created before the broker returns.
        :param validate_only: If True, don't actually create new partitions.
            Default: False
        :return: Appropriate version of CreatePartitionsResponse class.
        """
        version = self._matching_api_version(CreatePartitionsRequest)
        req_class = CreatePartitionsRequest[version]
        converted_partitions = self._convert_topic_partitions(topic_partitions)
        req = req_class(
            topic_partitions=converted_partitions,
            timeout=timeout_ms or self._request_timeout_ms,
            validate_only=validate_only
        )
        resp = await self._send_request(req)
        for topic, code, message in resp.topic_errors:
            if code:
                err_cls = for_code(code)
                raise err_cls(f"Could not create partitions for {topic}: {message}")
        return resp

    async def describe_consumer_groups(
            self,
            group_ids: List[str],
            group_coordinator_id: Optional[int] = None,
            include_authorized_operations: bool = False) -> List[Response]:
        """Describe a set of consumer groups.

        Any errors are immediately raised.

        :param group_ids: A list of consumer group IDs. These are typically the
            group names as strings.
        :param group_coordinator_id: The node_id of the groups' coordinator
            broker. If set to None, it will query the cluster for each group to
            find that group's coordinator. Explicitly specifying this can be
            useful for avoiding extra network round trips if you already know
            the group coordinator. This is only useful when all the group_ids
            have the same coordinator, otherwise it will error. Default: None.
        :param include_authorized_operations: Whether or not to include
            information about the operations a group is allowed to perform.
            Only supported on API version >= v3. Default: False.
        :return: A list of group descriptions. For now the group descriptions
            are the raw results from the DescribeGroupsResponse.
        """
        version = self._matching_api_version(DescribeGroupsRequest)
        if version < 3 and include_authorized_operations:
            raise IncompatibleBrokerVersion(
                "include_authorized_operations requests "
                "DescribeGroupsRequest >= v3, which is not "
                "supported by Kafka {}".format(version)
            )
        req_class = DescribeGroupsRequest[version]
        futures = []
        node_to_groups = defaultdict(set)
        for group_id in group_ids:
            if group_coordinator_id is None:
                node_id = await self.find_coordinator(group_id)
            else:
                node_id = group_coordinator_id
            node_to_groups[node_id].add(group_id)
        for node_id, groups in node_to_groups.items():
            if include_authorized_operations:
                req = req_class(
                    groups=list(groups),
                    include_authorized_operations=include_authorized_operations
                )
            else:
                req = req_class(groups=list(groups))
            future = self._send_request(req, node_id)
            futures.append(future)
        results = await asyncio.gather(*futures)
        return results

    async def list_consumer_groups(
            self,
            broker_ids: Optional[List[int]] = None) -> List[Tuple[Any, ...]]:
        """List all consumer groups known to the cluster.

        This returns a list of Consumer Group tuples. The tuples are
        composed of the consumer group name and the consumer group protocol
        type.

        Only consumer groups that store their offsets in Kafka are returned.
        The protocol type will be an empty string for groups created using
        Kafka < 0.9 APIs because, although they store their offsets in Kafka,
        they don't use Kafka for group coordination. For groups created using
        Kafka >= 0.9, the protocol type will typically be "consumer".

        As soon as any error is encountered, it is immediately raised.

        :param broker_ids: A list of broker node_ids to query for consumer
            groups. If set to None, will query all brokers in the cluster.
            Explicitly specifying broker(s) can be useful for determining which
            consumer groups are coordinated by those broker(s). Default: None
        :return list: List of tuples of Consumer Groups.
        :exception GroupCoordinatorNotAvailableError: The coordinator is not
            available, so cannot process requests.
        :exception GroupLoadInProgressError: The coordinator is loading and
            hence can't process requests.
        """
        if broker_ids is None:
            metadata = await self._get_cluster_metadata()
            broker_ids = [broker[0] for broker in metadata.brokers]
        consumer_groups = set()
        for broker_id in broker_ids:
            response = await self._send_request(
                ListGroupsRequest[self._matching_api_version(ListGroupsRequest)](),
                broker_id
            )
            if response.error_code:
                raise for_code(response.error_code)("Error listing consumer groups")
            consumer_groups.update(response.groups)
        return list(consumer_groups)

    async def find_coordinator(self, group_id: str, coordinator_type: int = 0) -> int:
        """Find the broker id for a given consumer group

        :param group_id: str the group id
        :param coordinator_type: int the type of coordinator:
        0 for group, 1 for transaction. Defaults to group.
        Only supported by version 1 and up

        :return int: the acting coordinator broker id
        """
        # FIXME GroupCoordinatorRequest_v1 in kafka-python 2.0.2 doesn't match
        # spec causing "ValueError: Buffer underrun decoding string"
        # version = self._matching_api_version(GroupCoordinatorRequest)
        version = self._matching_api_version(GroupCoordinatorRequest[:1])
        if version == 0 and coordinator_type:
            raise IncompatibleBrokerVersion(
                "Cannot query for transaction id on current broker version"
            )
        req_class = GroupCoordinatorRequest[version]
        if version == 0:
            request = req_class(consumer_group=group_id)
        else:
            request = req_class(group_id, coordinator_type)
        response = await self._send_request(request)
        if response.error_code:
            err = for_code(response.error_code)
            raise err(f"Unable to get coordinator id for {group_id}")
        return response.coordinator_id

    async def list_consumer_group_offsets(
            self,
            group_id: str,
            group_coordinator_id: Optional[int] = None,
            partitions: Optional[List[TopicPartition]] = None
    ) -> Dict[TopicPartition, OffsetAndMetadata]:
        """Fetch Consumer Offsets for a single consumer group.

        Note:
        This does not verify that the group_id or partitions actually exist
        in the cluster.

        As soon as any error is encountered, it is immediately raised.

        :param group_id: The consumer group id name for which to fetch offsets.
        :param group_coordinator_id: The node_id of the group's coordinator
            broker. If set to None, will query the cluster to find the group
            coordinator. Explicitly specifying this can be useful to prevent
            that extra network round trip if you already know the group
            coordinator. Default: None.
        :param partitions: A list of TopicPartitions for which to fetch
            offsets. On brokers >= 0.10.2, this can be set to None to fetch all
            known offsets for the consumer group. Default: None.
        :return dictionary: A dictionary with TopicPartition keys and
            OffsetAndMetada values. Partitions that are not specified and for
            which the group_id does not have a recorded offset are omitted. An
            offset value of `-1` indicates the group_id has no offset for that
            TopicPartition. A `-1` can only happen for partitions that are
            explicitly specified.
        """
        version = self._matching_api_version(OffsetFetchRequest)
        if version <= 1 and partitions is None:
            raise ValueError(
               f"""OffsetFetchRequest_v{version} requires specifying the
                partitions for which to fetch offsets. Omitting the
                partitions is only supported on brokers >= 0.10.2""")
        if partitions:
            topics_partitions_dict = defaultdict(set)
            for topic, partition in partitions:
                topics_partitions_dict[topic].add(partition)
            partitions = [(topic, list(partitions)) for
                          topic, partitions in topics_partitions_dict.items()]
        request = OffsetFetchRequest[version](group_id, partitions)
        if group_coordinator_id is None:
            group_coordinator_id = await self.find_coordinator(group_id)
        response = await self._send_request(request, group_coordinator_id)
        response_dict = {}
        for topic, partitions in response.topics:
            for partition, offset, metadata, error_code in partitions:
                if error_code:
                    err = for_code(response.error_code)
                    raise err(f"Unable to get offset info for {topic} and {partition}")
                tp = TopicPartition(topic, partition)
                offset_plus_meta = OffsetAndMetadata(offset, metadata)
                response_dict[tp] = offset_plus_meta
        return response_dict
