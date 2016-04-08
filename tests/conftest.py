import asyncio
import fcntl
import gc
import docker as libdocker
import pytest
import socket
import struct
import uuid
import sys


def pytest_addoption(parser):
    parser.addoption('--docker-image',
                     action='store',
                     default='pygo/kafka:2.11_0.9.0.1',
                     help='Kafka docker image to use')


@pytest.fixture(scope='session')
def docker():
    return libdocker.Client(version='auto')


@pytest.fixture(scope='session')
def docker_ip_address(docker):
    """Returns IP address of the docker daemon service."""
    # Fallback docker daemon bridge name
    ifname = 'docker0'
    try:
        for network in docker.networks():
            _ifname = docker.networks()[0]['Options'].get(
                'com.docker.network.bridge.name')
            if _ifname is not None:
                ifname = _ifname
                break
    except libdocker.errors.InvalidVersion:
        pass
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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


@pytest.yield_fixture(scope='session')
def kafka_server(request, docker, docker_ip_address, unused_port, session_id):
    image = request.config.getoption('--docker-image')
    docker.pull(image)
    kafka_host = docker_ip_address
    kafka_port = unused_port()
    container = docker.create_container(
        image=image,
        name='aiokafka-tests-{}'.format(session_id),
        ports=[2181, 9092],
        environment={
            'ADVERTISED_HOST': kafka_host,
            'ADVERTISED_PORT': kafka_port,
            'NUM_PARTITIONS': 2
        },
        host_config=docker.create_host_config(
            port_bindings={
                2181: (kafka_host, unused_port()),
                9092: (kafka_host, kafka_port)
            }))
    docker.start(container=container['Id'])
    yield kafka_host, kafka_port
    docker.kill(container=container['Id'])
    docker.remove_container(container['Id'])


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
def setup_test_class_serverless(request, loop):
    request.cls.loop = loop


@pytest.fixture(scope='class')
def setup_test_class(request, loop, kafka_server):
    request.cls.loop = loop
    request.cls.kafka_host, request.cls.kafka_port = kafka_server


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True
