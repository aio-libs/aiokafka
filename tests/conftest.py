import asyncio
import gc
import logging
import os
import pathlib
import socket
import sys
import uuid
from dataclasses import dataclass

import pytest

import docker as libdocker
from aiokafka.record.default_records import (
    DefaultRecordBatchBuilder,
    _DefaultRecordBatchBuilderPy,
)
from aiokafka.util import NO_EXTENSIONS

from ._testutil import wait_kafka

if not NO_EXTENSIONS:
    assert DefaultRecordBatchBuilder is not _DefaultRecordBatchBuilderPy, (
        "Expected to run tests with C extension, but it was not imported. "
        "To run tests without a C extensions set env AIOKAFKA_NO_EXTENSIONS=1"
    )
    print("Running tests with C extension")
else:
    print("Running tests without C extension")


def pytest_addoption(parser):
    parser.addoption(
        "--docker-image", action="store", default=None, help="Kafka docker image to use"
    )
    parser.addoption(
        "--no-pull",
        action="store_true",
        help="Do not pull new docker image before test run",
    )


def pytest_configure(config):
    """Disable the loggers."""
    # Debug logs clobber output on CI
    for name in ["urllib3", "asyncio"]:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)


@pytest.fixture(scope="session")
def docker(request):
    image = request.config.getoption("--docker-image")
    if image:
        client = libdocker.from_env()
        yield client
        client.close()
    else:
        yield None


@pytest.fixture(scope="class")
def acl_manager(kafka_server, request):
    image = request.config.getoption("--docker-image")
    tag = image.split(":")[-1].replace("_", "-")

    from ._testutil import ACLManager

    manager = ACLManager(kafka_server.container, tag)
    return manager


@pytest.fixture(scope="class")
def kafka_config(kafka_server, request):
    image = request.config.getoption("--docker-image")
    tag = image.split(":")[-1].replace("_", "-")

    from ._testutil import KafkaConfig

    manager = KafkaConfig(kafka_server.container, tag)
    return manager


if sys.platform != "win32":

    @pytest.fixture(scope="class")
    def kerberos_utils(kafka_server):
        from ._testutil import KerberosUtils

        utils = KerberosUtils(kafka_server.container)
        utils.create_keytab()
        return utils
else:

    @pytest.fixture()
    def kerberos_utils():
        pytest.skip("Only unit tests on windows for now =(")


if sys.platform != "win32":

    @pytest.fixture(scope="session")
    def kafka_image(request, docker):
        image = request.config.getoption("--docker-image")
        if not image:
            pytest.skip("Skipping functional test as `--docker-image` not provided")
            return None
        if not request.config.getoption("--no-pull"):
            docker.images.pull(image)
        return image

else:

    @pytest.fixture(scope="session")
    def kafka_image():
        pytest.skip("Only unit tests on windows for now =(")


@pytest.fixture(scope="session")
def ssl_folder(docker, kafka_image):
    ssl_dir = pathlib.Path("tests/ssl_cert")
    if ssl_dir.is_dir():
        # Skip generating certificates when they already exist. Remove
        # directory to re-generate them.
        return ssl_dir

    ssl_dir.mkdir()

    container = docker.containers.run(
        image=kafka_image,
        command="sleep 300",
        volumes={
            pathlib.Path("gen-ssl-certs.sh").resolve(): {
                "bind": "/gen-ssl-certs.sh",
            },
            str(ssl_dir.resolve()): {
                "bind": "/ssl_cert",
            },
        },
        working_dir="/ssl_cert",
        tty=True,
        detach=True,
        remove=True,
    )

    try:
        exit_code, output = container.exec_run(
            ["bash", "/gen-ssl-certs.sh"],
            user=f"{os.getuid()}:{os.getgid()}",
        )
        if exit_code != 0:
            print(output.decode(), file=sys.stderr)
            pytest.exit("Could not generate certificates")

    finally:
        container.stop()

    return ssl_dir


@pytest.fixture(scope="session")
def docker_ip_address():
    """Returns IP address of the docker daemon service."""
    return "127.0.0.1"


@pytest.fixture(scope="session")
def unused_port():
    def factory():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return factory


@pytest.fixture(scope="session")
def session_id():
    return str(uuid.uuid4())


@dataclass
class KafkaServer:
    host: str
    port: int
    ssl_port: int
    sasl_plain_port: int
    sasl_ssl_port: int
    container: libdocker.models.containers.Container

    @property
    def hosts(self):
        return [f"{self.host}:{self.port}"]


if sys.platform != "win32":

    @pytest.fixture(scope="session")
    def kafka_server(
        kafka_image, docker, docker_ip_address, unused_port, session_id, ssl_folder
    ):
        kafka_host = docker_ip_address
        kafka_port = unused_port()
        kafka_ssl_port = unused_port()
        kafka_sasl_plain_port = unused_port()
        kafka_sasl_ssl_port = unused_port()
        environment = {
            "ADVERTISED_HOST": kafka_host,
            "ADVERTISED_PORT": kafka_port,
            "ADVERTISED_SSL_PORT": kafka_ssl_port,
            "ADVERTISED_SASL_PLAINTEXT_PORT": kafka_sasl_plain_port,
            "ADVERTISED_SASL_SSL_PORT": kafka_sasl_ssl_port,
            "NUM_PARTITIONS": 2,
        }
        kafka_version = kafka_image.split(":")[-1].split("_")[-1]
        kafka_version = tuple(int(x) for x in kafka_version.split("."))
        if kafka_version >= (0, 10, 2):
            environment["SASL_MECHANISMS"] = "PLAIN,GSSAPI,SCRAM-SHA-256,SCRAM-SHA-512"
            environment["SASL_JAAS_FILE"] = "kafka_server_jaas.conf"
        elif kafka_version >= (0, 10, 1):
            environment["SASL_MECHANISMS"] = "PLAIN,GSSAPI"
            environment["SASL_JAAS_FILE"] = "kafka_server_jaas_no_scram.conf"
        else:
            environment["SASL_MECHANISMS"] = "GSSAPI"
            environment["SASL_JAAS_FILE"] = "kafka_server_gssapi_jaas.conf"

        container = docker.containers.run(
            image=kafka_image,
            name="aiokafka-tests",
            ports={
                2181: 2181,
                "88/udp": 88,
                kafka_port: kafka_port,
                kafka_ssl_port: kafka_ssl_port,
                kafka_sasl_plain_port: kafka_sasl_plain_port,
                kafka_sasl_ssl_port: kafka_sasl_ssl_port,
            },
            volumes={str(ssl_folder.resolve()): {"bind": "/ssl_cert", "mode": "ro"}},
            environment=environment,
            tty=True,
            detach=True,
            remove=True,
        )

        try:
            if not wait_kafka(kafka_host, kafka_port):
                exit_code, output = container.exec_run(
                    ["supervisorctl", "tail", "-20000", "kafka"]
                )
                print("Kafka failed to start. \n--- STDOUT:")
                print(output.decode(), file=sys.stdout)
                exit_code, output = container.exec_run(
                    ["supervisorctl", "tail", "-20000", "kafka", "stderr"]
                )
                print("--- STDERR:")
                print(output.decode(), file=sys.stderr)
                pytest.exit("Could not start Kafka Server")

            yield KafkaServer(
                kafka_host,
                kafka_port,
                kafka_ssl_port,
                kafka_sasl_plain_port,
                kafka_sasl_ssl_port,
                container,
            )
        finally:
            container.stop()

else:

    @pytest.fixture(scope="session")
    def kafka_server():
        pytest.skip("Only unit tests on windows for now =(")


@pytest.fixture(scope="class")
def loop(request):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.fixture(autouse=True)
def collect_garbage():
    # This is used to have a better report on ResourceWarnings. Without it
    # all warnings will be filled in the end of last test-case.
    yield
    gc.collect()


@pytest.fixture(scope="class")
def setup_test_class_serverless(request, loop):
    request.cls.loop = loop


@pytest.fixture(scope="class")
def setup_test_class(
    request, loop, kafka_server, ssl_folder, acl_manager, kerberos_utils, kafka_config
):
    request.cls.loop = loop
    request.cls.kafka_host = kafka_server.host
    request.cls.kafka_port = kafka_server.port
    request.cls.kafka_ssl_port = kafka_server.ssl_port
    request.cls.kafka_sasl_plain_port = kafka_server.sasl_plain_port
    request.cls.kafka_sasl_ssl_port = kafka_server.sasl_ssl_port
    request.cls.ssl_folder = ssl_folder
    request.cls.acl_manager = acl_manager
    request.cls.kerberos_utils = kerberos_utils
    request.cls.kafka_config = kafka_config
    request.cls.hosts = kafka_server.hosts

    docker_image = request.config.getoption("--docker-image")
    kafka_version = docker_image.split(":")[-1].split("_")[-1]
    request.cls.kafka_version = kafka_version
