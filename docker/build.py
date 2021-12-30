import asyncio
import shlex
import sys
import yaml
import argparse


async def build(versions_file, args):

    with open(versions_file) as f:
        config = yaml.load(f.read(), yaml.Loader)

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
            proc = await asyncio.create_subprocess_exec(*args)
            procs.append(proc.wait())

        res = await asyncio.gather(*procs)
        if any(res):  # If any of statuses are not 0 return right away
            return res
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build and push images in parallel.')
    parser.add_argument(
        'actions', metavar='action',
        nargs='+', help='Actions to take: build, push')

    args = parser.parse_args()

    statuses = asyncio.run(build('config.yml', args))
    sys.exit(max(statuses))
