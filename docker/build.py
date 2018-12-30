import asyncio
import shlex
import sys
import yaml
import argparse


@asyncio.coroutine
def build(versions_file, args, *, loop):

    with open(versions_file) as f:
        config = yaml.load(f.read())

    for action in args.actions:
        procs = []
        for version_map in config['versions']:
            args = shlex.split('make docker-{action} '
                               'IMAGE_NAME={image_name} '
                               'KAFKA_VERSION={kafka} '
                               'SCALA_VERSION={scala}'.format(
                                   action=action,
                                   image_name=config['image_name'],
                                   **version_map))
            proc = yield from asyncio.create_subprocess_exec(*args, loop=loop)
            procs.append(proc.wait())

        res = yield from asyncio.gather(*procs, loop=loop)
        if any(res):  # If any of statuses are not 0 return right away
            return res
    return res


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(
        description='Build and push images in parallel.')
    parser.add_argument(
        'actions', metavar='action',
        nargs='+', help='Actions to take: build, push')

    args = parser.parse_args()

    statuses = loop.run_until_complete(build('config.yml', args, loop=loop))
    loop.close()
    sys.exit(max(statuses))
