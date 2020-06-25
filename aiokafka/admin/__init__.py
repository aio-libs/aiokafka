import logging
import asyncio
from aiokafka import __version__
from aiokafka.client import AIOKafkaClient
from kafka.errors import IncompatibleBrokerVersion
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.admin import (
    CreateTopicsRequest,
    DeleteTopicsRequest,
    ApiVersionRequest_v0)
from kafka.admin import KafkaAdminClient
log = logging.getLogger(__name__)


class AIOKafkaAdminClient(object):
    """A class for administering the Kafka cluster.

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
        api_version (tuple): Specify which Kafka API version to use. If set
            to None, KafkaClient will attempt to infer the broker version by
            probing various APIs. Example: (0, 10, 2). Default: None
    """

    def __init__(self, loop=None,
                 bootstrap_servers='localhost',
                 client_id='aiokafka-' + __version__,
                 request_timeout_ms=40000,
                 connections_max_idle_ms=540000,
                 retry_backoff_ms=100,
                 metadata_max_age_ms=300000,
                 security_protocol="PLAINTEXT",
                 ssl_context=None, api_version=None):
        self.loop = loop or asyncio.get_running_loop()
        self._closed = False
        self._started = False
        self._version_info = {}
        self.request_timeout_ms = request_timeout_ms
        self.api_version = api_version
        self._client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms)

    async def close(self):
        """Close the KafkaAdminClient connection to the Kafka broker."""
        if not hasattr(self, '_closed') or self._closed:
            log.info("KafkaAdminClient already closed.")
            return

        await self._client.close()
        self._closed = True
        log.debug("AIOKafkaAdminClient is now closed.")

    async def _send_request(self, request):
        return await self._client.send(
            self._client.get_random_node(),
            request)

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

    def _matching_api_version(self, operation):
        """Find the latest version of the protocol operation
        supported by both this library and the broker.

        This resolves to the lesser of either the latest api
        version this library supports, or the max version
        supported by the broker.

        :param operation: A list of protocol operation versions from
        kafka.protocol.
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
            # max library version is less than min broker version. Currently,
            # no Kafka versions specify a min msg version. Maybe in the future?
            raise IncompatibleBrokerVersion(
                "No version of the '{}' Kafka protocol is supported by "
                "both the client and broker."
                .format(operation[0].__name__))
        return version

    async def create_topics(
            self,
            new_topics,
            timeout_ms=None,
            validate_only=False
    ):
        """Create new topics in the cluster.

        :param new_topics: A list of NewTopic objects.
        :param timeout_ms: Milliseconds to wait for new topics to be created
            before the broker returns.
        :param validate_only: If True, don't actually create new topics.
            Not supported by all versions. Default: False
        :return: Appropriate version of CreateTopicResponse class.
        """
        version = self._matching_api_version(CreateTopicsRequest)
        topics = [KafkaAdminClient._convert_new_topic_request(new_topic)
                  for new_topic in new_topics]
        timeout_ms = timeout_ms or self.request_timeout_ms
        if version == 0:
            if validate_only:
                raise IncompatibleBrokerVersion(
                    "validate_only requires CreateTopicsRequest >= v1, "
                    "which is not supported by Kafka {}."
                    .format(self.api_version))
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
                "to KafkaAdminClient."
                .format(version))
        response = await self._client.send(
            self._client.get_random_node(),
            request)
        return response

    async def delete_topics(self, topics, timeout_ms=None):
        """Delete topics from the cluster.

        :param topics: A list of topic name strings.
        :param timeout_ms: Milliseconds to wait for topics to be deleted
            before the broker returns.
        :return: Appropriate version of DeleteTopicsResponse class.
        """
        request = DeleteTopicsRequest[
            self._matching_api_version(DeleteTopicsRequest)
        ](topics, timeout_ms or self.request_timeout_ms)
        response = await self._send_request(request)
        return response

    async def _get_cluster_metadata(self, topics=None):
        """
        topics == None means "get all topics"
        """
        req_cls = MetadataRequest[self._matching_api_version(MetadataRequest)]
        request = req_cls(topics=topics)
        response = await self._send_request(request)
        return response

    async def list_topics(self):
        metadata = await self._get_cluster_metadata(topics=None)
        obj = metadata.to_object()
        return [t['topic'] for t in obj['topics']]

    async def describe_topics(self, topics=None):
        metadata = await self._get_cluster_metadata(topics=topics)
        obj = metadata.to_object()
        return obj['topics']

    async def describe_cluster(self):
        metadata = await self._get_cluster_metadata()
        obj = metadata.to_object()
        obj.pop('topics')  # We have 'describe_topics' for this
        return obj

    async def describe_acls(self, acl_filter):
        """Describe a set of ACLs

        Used to return a set of ACLs matching the supplied ACLFilter.
        The cluster must be configured with an authorizer for this to work, or
        you will get a SecurityDisabledError

        :param acl_filter: an ACLFilter object
        :return: tuple of a list of matching ACL objects and a KafkaError
        (NoError if successful)
        """
        pass

    def create_acls(self, acls):
        """Create a list of ACLs

        This endpoint only accepts a list of concrete ACL objects,
        no ACLFilters. Throws TopicAlreadyExistsError if topic is
        already present.

        :param acls: a list of ACL objects
        :return: dict of successes and failures
        """
        pass

    def delete_acls(self, acl_filters):
        """Delete a set of ACLs

        Deletes all ACLs matching the list of input ACLFilter

        :param acl_filters: a list of ACLFilter
        :return: a list of 3-tuples corresponding to the list of input filters.
                 The tuples hold (the input ACLFilter, list of affected ACLs,
                 KafkaError instance)
        """
        pass

    def describe_configs(self, config_resources, include_synonyms=False):
        """Fetch configuration parameters for one or more Kafka resources.

        :param config_resources: An list of ConfigResource objects.
            Any keys in ConfigResource.configs dict will be used to filter the
            result. Setting the configs dict to None will get all values. An
            empty dict will get zero values (as per Kafka protocol).
        :param include_synonyms: If True, return synonyms in response. Not
            supported by all versions. Default: False.
        :return: Appropriate version of DescribeConfigsResponse class.
        """
        pass

    def alter_configs(self, config_resources):
        """Alter configuration parameters of one or more Kafka resources.

        Warning:
            This is currently broken for BROKER resources because those must be
            sent to that specific broker, versus this always picks the
            least-loaded node. See the comment in the source code for details.
            We would happily accept a PR fixing this.

        :param config_resources: A list of ConfigResource objects.
        :return: Appropriate version of AlterConfigsResponse class.
        """
        pass

    def create_partitions(
            self,
            topic_partitions,
            timeout_ms=None,
            validate_only=False):
        """Create additional partitions for an existing topic.

        :param topic_partitions: A map of topic name strings to NewPartition
         objects.
        :param timeout_ms: Milliseconds to wait for new partitions to be
            created before the broker returns.
        :param validate_only: If True, don't actually create new partitions.
            Default: False
        :return: Appropriate version of CreatePartitionsResponse class.
        """
        pass

    def describe_consumer_groups(
            self,
            group_ids,
            group_coordinator_id=None,
            include_authorized_operations=False):
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
            are the raw results from the DescribeGroupsResponse. Long-term, we
            plan to change this to return namedtuples as well as decoding the
            partition assignments.
        """
        pass

    def list_consumer_groups(self, broker_ids=None):
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
        pass

    def list_consumer_group_offsets(self, group_id, group_coordinator_id=None,
                                    partitions=None):
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
        pass
