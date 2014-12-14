import logging
import os
import os.path
import shutil
import subprocess
import tempfile
import uuid

from .service import SpawnedService
from ._testutil import get_open_port


__all__ = ['Fixture', 'ZookeeperFixture', 'KafkaFixture']


class Fixture:

    kafka_version = os.environ.get('KAFKA_VERSION', '0.8.1.1')
    scala_version = os.environ.get("SCALA_VERSION", '2.10')

    tests_root = os.environ.get('TESTS_ROOT',
                                os.path.abspath(os.path.dirname(__file__)))
    kafka_dir = 'kafka_{}-{}'.format(scala_version, kafka_version)
    kafka_root = os.environ.get(
        "KAFKA_ROOT", os.path.join(tests_root, '..', kafka_dir))

    @classmethod
    def test_resource(cls, filename):
        return os.path.join(cls.tests_root, "servers", cls.kafka_version,
                            "resources", filename)

    @classmethod
    def kafka_run_class_args(cls, *args):
        result = [os.path.join(cls.kafka_root, 'bin', 'kafka-run-class.sh')]
        result.extend(args)
        return result

    @classmethod
    def kafka_run_class_env(cls):
        env = os.environ.copy()
        env[
            'KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % \
                                  cls.test_resource("log4j.properties")
        return env

    @classmethod
    def render_template(cls, source_file, target_file, binding):
        with open(source_file, "r") as handle:
            template = handle.read()
        with open(target_file, "w") as handle:
            handle.write(template.format(**binding))


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls):
        (host, port) = ("127.0.0.1", get_open_port())
        fixture = cls(host, port)
        fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def out(self, message):
        logging.info("*** Zookeeper [%s:%d]: %s", self.host, self.port,
                     message)

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        logging.info("  host    = %s", self.host)
        logging.info("  port    = %s", self.port)
        logging.info("  tmp_dir = %s", self.tmp_dir)

        # Generate configs
        template = self.test_resource("zookeeper.properties")
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        self.render_template(template, properties, vars(self))

        # Configure Zookeeper child process
        args = self.kafka_run_class_args(
            "org.apache.zookeeper.server.quorum.QuorumPeerMain", properties)
        env = self.kafka_run_class_env()
        self.child = SpawnedService(args, env)

        # Party!
        self.out("Starting...")
        self.child.start()
        self.child.wait_for(r"Snapshotting")
        self.out("Done!")

    def close(self):
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)


class KafkaFixture(Fixture):

    @classmethod
    def instance(cls, broker_id, zk_host, zk_port, zk_chroot=None, replicas=1,
                 partitions=2):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")

        (host, port) = ("127.0.0.1", get_open_port())
        fixture = KafkaFixture(host, port, broker_id, zk_host, zk_port,
                               zk_chroot, replicas, partitions)
        fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot,
                 replicas=1, partitions=2):
        self.host = host
        self.port = port

        self.broker_id = broker_id

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.replicas = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None
        self.running = False

    def out(self, message):
        logging.info("*** Kafka [%s:%d]: %s", self.host, self.port, message)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        logging.info("  host       = %s", self.host)
        logging.info("  port       = %s", self.port)
        logging.info("  broker_id  = %s", self.broker_id)
        logging.info("  zk_host    = %s", self.zk_host)
        logging.info("  zk_port    = %s", self.zk_port)
        logging.info("  zk_chroot  = %s", self.zk_chroot)
        logging.info("  replicas   = %s", self.replicas)
        logging.info("  partitions = %s", self.partitions)
        logging.info("  tmp_dir    = %s", self.tmp_dir)

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Generate configs
        template = self.test_resource("kafka.properties")
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        self.render_template(template, properties, vars(self))

        # Configure Kafka child process
        args = self.kafka_run_class_args("kafka.Kafka", properties)
        env = self.kafka_run_class_env()
        self.child = SpawnedService(args, env)

        # Party!
        self.out("Creating Zookeeper chroot node...")
        args = self.kafka_run_class_args("org.apache.zookeeper.ZooKeeperMain",
                                         "-server",
                                         "%s:%d" % (self.zk_host,
                                                    self.zk_port),
                                         "create",
                                         "/%s" % self.zk_chroot,
                                         "kafka-python")
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        if proc.wait() != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout)
            self.out(proc.stderr)
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        self.out("Starting...")
        self.child.start()
        self.child.wait_for(r"\[Kafka Server %d\], Started" % self.broker_id)
        self.out("Done!")
        self.running = True

    def close(self):
        if not self.running:
            self.out("Instance already stopped")
            return

        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)
        self.running = False
