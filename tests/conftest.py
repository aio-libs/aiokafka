import asyncio
import gc
import docker as libdocker
import pytest
import socket
import struct
import uuid
import sys
import pathlib
import shutil
import subprocess

from aiokafka.record.legacy_records import (
    LegacyRecordBatchBuilder, _LegacyRecordBatchBuilderPy)
from aiokafka.util import NO_EXTENSIONS

if not NO_EXTENSIONS:
    assert LegacyRecordBatchBuilder is not _LegacyRecordBatchBuilderPy, \
        "Expected to run tests with C extension, but it was not imported. "\
        "To run tests without a C extensions set env AIOKAFKA_NO_EXTENSIONS=1"
    print("Running tests with C extension")
else:
    print("Running tests without C extension")


def pytest_addoption(parser):
    parser.addoption('--docker-image',
                     action='store',
                     default='pygo/kafka:2.11_0.9.0.1',
                     help='Kafka docker image to use')
    parser.addoption('--no-pull',
                     action='store_true',
                     help='Do not pull new docker image before test run')


@pytest.fixture(scope='session')
def docker():
    return libdocker.Client(version='auto')


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


if sys.platform == 'darwin' or sys.platform == 'win32':

    @pytest.fixture(scope='session')
    def docker_ip_address():
        """Returns IP address of the docker daemon service."""
        # docker for mac publishes ports on localhost
        return '127.0.0.1'

else:

    @pytest.fixture(scope='session')
    def docker_ip_address(docker):
        """Returns IP address of the docker daemon service."""
        # Fallback docker daemon bridge name
        ifname = 'docker0'
        try:
            for network in docker.networks():
                _ifname = network['Options'].get(
                    'com.docker.network.bridge.name')
                if _ifname is not None:
                    ifname = _ifname
                    break
        except libdocker.errors.InvalidVersion:
            pass
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        import fcntl
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15].encode('utf-8')))[20:24])


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
        if not request.config.getoption('--no-pull'):
            docker.pull(image)
        kafka_host = docker_ip_address
        kafka_port = unused_port()
        kafka_ssl_port = unused_port()
        container = docker.create_container(
            image=image,
            name='aiokafka-tests',
            # name='aiokafka-tests-{}'.format(session_id),
            ports=[2181, kafka_port, kafka_ssl_port],
            volumes=['/ssl_cert'],
            environment={
                'ADVERTISED_HOST': kafka_host,
                'ADVERTISED_PORT': kafka_port,
                'ADVERTISED_SSL_PORT': kafka_ssl_port,
                'NUM_PARTITIONS': 2
            },
            host_config=docker.create_host_config(
                port_bindings={
                    2181: (kafka_host, unused_port()),
                    kafka_port: (kafka_host, kafka_port),
                    kafka_ssl_port: (kafka_host, kafka_ssl_port)
                },
                binds={
                    str(ssl_folder.resolve()): {
                        "bind": "/ssl_cert",
                        "mode": "ro"
                    }
                }))
        docker.start(container=container['Id'])
        yield kafka_host, kafka_port, kafka_ssl_port
        docker.kill(container=container['Id'])
        docker.remove_container(container['Id'])

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


@pytest.fixture(scope='class')
def setup_test_class_serverless(request, loop, ssl_folder):
    request.cls.loop = loop
    request.cls.ssl_folder = ssl_folder


@pytest.fixture(scope='class')
def setup_test_class(request, loop, kafka_server, ssl_folder):
    request.cls.loop = loop
    khost, kport, ksslport = kafka_server
    request.cls.kafka_host = khost
    request.cls.kafka_port = kport
    request.cls.kafka_ssl_port = ksslport
    request.cls.ssl_folder = ssl_folder

    docker_image = request.config.getoption('--docker-image')
    kafka_version = docker_image.split(":")[-1].split("_")[-1]
    request.cls.kafka_version = kafka_version

    if hasattr(request.cls, 'wait_kafka'):
        request.cls.wait_kafka()


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True
