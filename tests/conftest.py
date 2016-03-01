import asyncio
import fcntl
import gc
import docker as libdocker
import pytest
import socket
import struct
import uuid


def pytest_addoption(parser):
    parser.addoption('--docker-image-name',
                     action='store',
                     default='pygo/kafka',
                     help='Kafka docker image name')
    parser.addoption('--kafka-version',
                     action='store',
                     default='0.9.0.1',
                     help='Kafka version')
    parser.addoption('--scala-version',
                     action='store',
                     default='2.11',
                     help='Scala version')


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
    image_name = request.config.getoption('--docker-image-name')
    skala_version = request.config.getoption('--scala-version')
    kafka_version = request.config.getoption('--kafka-version')
    image_tag = '{}:{}_{}'.format(image_name, skala_version, kafka_version)
    docker.pull(image_tag)
    kafka_host = docker_ip_address
    kafka_port = unused_port()
    container = docker.create_container(
        image=image_tag,
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
def setup_test_class(request, loop):
    request.cls.loop = loop
