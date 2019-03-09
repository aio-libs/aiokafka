import asyncio
import gc
import docker as libdocker
import pytest
import socket
import uuid
import sys
import pathlib
import shutil
import subprocess

from aiokafka.record.legacy_records import (
    LegacyRecordBatchBuilder, _LegacyRecordBatchBuilderPy)
from aiokafka.record.default_records import (
    DefaultRecordBatchBuilder, _DefaultRecordBatchBuilderPy)
from aiokafka.util import NO_EXTENSIONS

if not NO_EXTENSIONS:
    assert LegacyRecordBatchBuilder is not _LegacyRecordBatchBuilderPy and \
        DefaultRecordBatchBuilder is not _DefaultRecordBatchBuilderPy, \
        "Expected to run tests with C extension, but it was not imported. "\
        "To run tests without a C extensions set env AIOKAFKA_NO_EXTENSIONS=1"
    print("Running tests with C extension")
else:
    print("Running tests without C extension")


def pytest_addoption(parser):
    parser.addoption('--docker-image',
                     action='store',
                     default=None,
                     help='Kafka docker image to use')
    parser.addoption('--no-pull',
                     action='store_true',
                     help='Do not pull new docker image before test run')


@pytest.fixture(scope='session')
def docker(request):
    image = request.config.getoption('--docker-image')
    if not image:
        return None
    return libdocker.from_env()


@pytest.fixture(scope='class')
def acl_manager(kafka_server, request):
    image = request.config.getoption('--docker-image')
    tag = image.split(":")[-1].replace('_', '-')

    from ._testutil import ACLManager
    manager = ACLManager(kafka_server[-1], tag)
    return manager


@pytest.yield_fixture(autouse=True)
def clean_acl(acl_manager):
    # This is used to have a better report on ResourceWarnings. Without it
    # all warnings will be filled in the end of last test-case.
    yield
    acl_manager.cleanup()


if sys.platform != 'win32':

    @pytest.fixture(scope='class')
    def kerberos_utils(kafka_server):
        from ._testutil import KerberosUtils
        utils = KerberosUtils(kafka_server[-1])
        utils.create_keytab()
        return utils
else:

    @pytest.fixture()
    def kerberos_utils():
        pytest.skip("Only unit tests on windows for now =(")
        return


@pytest.fixture(scope='session')
def ssl_folder(docker_ip_address):
    ssl_dir = pathlib.Path('tests/ssl_cert')
    if ssl_dir.exists():
        shutil.rmtree(str(ssl_dir))

    ssl_dir.mkdir()
    p = subprocess.Popen(
        "bash ../../gen-ssl-certs.sh ca ca-cert {}".format(docker_ip_address),
        shell=True, stdout=subprocess.DEVNULL,
        cwd=str(ssl_dir), stderr=subprocess.DEVNULL)
    p.wait()
    p = subprocess.Popen(
        "bash ../../gen-ssl-certs.sh -k server ca-cert br_ {}".format(
            docker_ip_address),
        shell=True, stdout=subprocess.DEVNULL,
        cwd=str(ssl_dir), stderr=subprocess.DEVNULL,)
    p.wait()
    p = subprocess.Popen(
        "bash ../../gen-ssl-certs.sh client ca-cert cl_ {}".format(
            docker_ip_address),
        shell=True, stdout=subprocess.DEVNULL,
        cwd=str(ssl_dir), stderr=subprocess.DEVNULL,)
    p.wait()

    return ssl_dir


@pytest.fixture(scope='session')
def docker_ip_address():
    """Returns IP address of the docker daemon service."""
    return '127.0.0.1'


@pytest.fixture(scope='session')
def unused_port():
    def factory():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return factory


@pytest.fixture(scope='session')
def session_id():
    return str(uuid.uuid4())


if sys.platform != 'win32':

    @pytest.yield_fixture(scope='session')
    def kafka_server(request, docker, docker_ip_address,
                     unused_port, session_id, ssl_folder):
        image = request.config.getoption('--docker-image')
        if not image:
            pytest.skip(
                "Skipping functional test as `--docker-image` not provided")
            return
        if not request.config.getoption('--no-pull'):
            docker.images.pull(image)
        kafka_host = docker_ip_address
        kafka_port = unused_port()
        kafka_ssl_port = unused_port()
        kafka_sasl_plain_port = unused_port()
        kafka_sasl_ssl_port = unused_port()
        environment = {
            'ADVERTISED_HOST': kafka_host,
            'ADVERTISED_PORT': kafka_port,
            'ADVERTISED_SSL_PORT': kafka_ssl_port,
            'ADVERTISED_SASL_PLAINTEXT_PORT': kafka_sasl_plain_port,
            'ADVERTISED_SASL_SSL_PORT': kafka_sasl_ssl_port,
            'NUM_PARTITIONS': 2
        }
        kafka_version = image.split(":")[-1].split("_")[-1]
        if not kafka_version == "0.9.0.1":
            environment['SASL_MECHANISMS'] = "PLAIN,GSSAPI"
            environment['SASL_JAAS_FILE'] = "kafka_server_jaas.conf"
        else:
            environment['SASL_MECHANISMS'] = "GSSAPI"
            environment['SASL_JAAS_FILE'] = "kafka_server_gssapi_jaas.conf"

        container = docker.containers.run(
            image=image,
            name='aiokafka-tests',
            ports={
                2181: 2181,
                "88/udp": 88,
                kafka_port: kafka_port,
                kafka_ssl_port: kafka_ssl_port,
                kafka_sasl_plain_port: kafka_sasl_plain_port,
                kafka_sasl_ssl_port: kafka_sasl_ssl_port
            },
            volumes={
                str(ssl_folder.resolve()): {
                    "bind": "/ssl_cert",
                    "mode": "ro"
                }
            },
            environment=environment,
            tty=True,
            detach=True)

        yield (
            kafka_host, kafka_port, kafka_ssl_port, kafka_sasl_plain_port,
            kafka_sasl_ssl_port, container
        )
        container.remove(force=True)

else:

    @pytest.fixture(scope='session')
    def kafka_server():
        pytest.skip("Only unit tests on windows for now =(")
        return


@pytest.yield_fixture(scope='class')
def loop(request):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.yield_fixture(autouse=True)
def collect_garbage():
    # This is used to have a better report on ResourceWarnings. Without it
    # all warnings will be filled in the end of last test-case.
    yield
    gc.collect()


@pytest.fixture(scope='class')
def setup_test_class_serverless(request, loop, ssl_folder):
    request.cls.loop = loop
    request.cls.ssl_folder = ssl_folder


@pytest.fixture(scope='class')
def setup_test_class(request, loop, kafka_server, ssl_folder, acl_manager,
                     kerberos_utils):
    request.cls.loop = loop
    request.cls.kafka_host = kafka_server[0]
    request.cls.kafka_port = kafka_server[1]
    request.cls.kafka_ssl_port = kafka_server[2]
    request.cls.kafka_sasl_plain_port = kafka_server[3]
    request.cls.kafka_sasl_ssl_port = kafka_server[4]
    request.cls.ssl_folder = ssl_folder
    request.cls.acl_manager = acl_manager
    request.cls.kerberos_utils = kerberos_utils

    docker_image = request.config.getoption('--docker-image')
    kafka_version = docker_image.split(":")[-1].split("_")[-1]
    request.cls.kafka_version = kafka_version

    if hasattr(request.cls, 'wait_kafka'):
        request.cls.wait_kafka()


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True
