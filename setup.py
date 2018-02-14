import os
import re
import platform
import sys
from distutils.command.build_ext import build_ext
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)

from setuptools import setup, Extension

# Those are needed to build _hton for windows

CFLAGS = ['-O2']
LDFLAGS = []
LIBRARIES = []

if platform.uname().system == 'Windows':
    LDFLAGS.append('ws2_32.lib')
else:
    CFLAGS.extend(['-Wall', '-Wsign-compare', '-Wconversion'])
    LIBRARIES.append('z')

# The extension part is copied from aiohttp's setup.py

try:
    from Cython.Build import cythonize
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False

ext = '.pyx' if USE_CYTHON else '.c'

extensions = [
    Extension(
        'aiokafka.record._legacy_records',
        ['aiokafka/record/_legacy_records' + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS
    ),
    Extension(
        'aiokafka.record._memory_records',
        ['aiokafka/record/_memory_records' + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS
    ),
]


if USE_CYTHON:
    extensions = cythonize(extensions)


class BuildFailed(Exception):
    pass


class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except (DistutilsPlatformError, FileNotFoundError):
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, ValueError):
            raise BuildFailed()


install_requires = ['kafka-python==1.4.1']

PY_VER = sys.version_info

if PY_VER >= (3, 5):
    pass
elif PY_VER >= (3, 4):
    install_requires.append('typing')
elif PY_VER >= (3, 3):
    install_requires.append('typing')
    install_requires.append('asyncio')
else:
    raise RuntimeError("aiokafka doesn't suppport Python earlier than 3.3")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()

extras_require = {'snappy': ['python-snappy>=0.5'], }


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrcdev]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aiokafka', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aiokafka/__init__.py')

classifiers = [
    'License :: OSI Approved :: Apache Software License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: OS Independent',
    'Topic :: System :: Networking',
    'Topic :: System :: Distributed Computing',
    'Framework :: AsyncIO',
    'Development Status :: 4 - Beta',
]


args = dict(
    name='aiokafka',
    version=read_version(),
    description=('Kafka integration with asyncio.'),
    long_description='\n\n'.join((read('README.rst'), read('CHANGES.rst'))),
    classifiers=classifiers,
    platforms=['POSIX'],
    author='Andrew Svetlov',
    author_email='andrew.svetlov@gmail.com',
    url='http://aiokafka.readthedocs.org',
    download_url='https://pypi.python.org/pypi/aiokafka',
    license='Apache 2',
    packages=['aiokafka'],
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
    ext_modules=extensions,
    cmdclass=dict(build_ext=ve_build_ext)
)

try:
    setup(**args)
except BuildFailed:
    print("************************************************************")
    print("Cannot compile C accelerator module, use pure python version")
    print("************************************************************")
    del args['ext_modules']
    del args['cmdclass']
    setup(**args)
