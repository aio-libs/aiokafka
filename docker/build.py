import asyncio
import shlex
import sys
import yaml


@asyncio.coroutine
def build(versions_file, *, loop):

    with open(versions_file) as f:
        config = yaml.load(f.read())

    procs = []

    for version_map in config['versions']:
        args = shlex.split('make docker-build '
                           'IMAGE_NAME={image_name} '
                           'KAFKA_VERSION={kafka} '
                           'SCALA_VERSION={scala}'.format(
                               image_name=config['image_name'],
                               **version_map))
        proc = yield from asyncio.create_subprocess_exec(*args, loop=loop)
        procs.append(proc.wait())

    return (yield from asyncio.gather(*procs, loop=loop))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    statuses = loop.run_until_complete(build('config.yml', loop=loop))
    loop.close()
    sys.exit(max(statuses))
