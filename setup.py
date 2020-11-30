import os
import platform
import re
import sys
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError

from setuptools import Extension, setup


# Those are needed to build _hton for windows

CFLAGS = ["-O2"]
LDFLAGS = []
LIBRARIES = []

if platform.uname().system == "Windows":
    LDFLAGS.append("ws2_32.lib")
else:
    CFLAGS.extend(["-Wall", "-Wsign-compare", "-Wconversion"])
    LIBRARIES.append("z")

# The extension part is copied from aiohttp's setup.py

try:
    from Cython.Build import cythonize

    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False

ext = ".pyx" if USE_CYTHON else ".c"

extensions = [
    Extension(
        "aiokafka.record._crecords.legacy_records",
        ["aiokafka/record/_crecords/legacy_records" + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.default_records",
        [
            "aiokafka/record/_crecords/crc32c.c",
            "aiokafka/record/_crecords/default_records" + ext,
        ],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.memory_records",
        ["aiokafka/record/_crecords/memory_records" + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.cutil",
        ["aiokafka/record/_crecords/crc32c.c", "aiokafka/record/_crecords/cutil" + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
]


if USE_CYTHON:
    extensions = cythonize(extensions)


class bdist_rpm(_bdist_rpm):
    def _make_spec_file(self):
        orig = super()._make_spec_file()
        orig.insert(0, "%define debug_package %{nil}")
        return orig


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
        except (CCompilerError, DistutilsExecError, DistutilsPlatformError, ValueError):
            raise BuildFailed()


install_requires = [
    "kafka-python>=2.0.0",
    "dataclasses>=0.5; python_version<'3.7'",
]

PY_VER = sys.version_info

if PY_VER < (3, 6):
    raise RuntimeError("aiokafka doesn't support Python earlier than 3.6")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


extras_require = {
    "snappy": ["python-snappy>=0.5"],
}


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrcdev]+)'")
    init_py = os.path.join(os.path.dirname(__file__), "aiokafka", "__init__.py")
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError("Cannot find version in aiokafka/__init__.py")


classifiers = [
    "License :: OSI Approved :: Apache Software License",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Operating System :: OS Independent",
    "Topic :: System :: Networking",
    "Topic :: System :: Distributed Computing",
    "Framework :: AsyncIO",
    "Development Status :: 4 - Beta",
]


args = dict(
    name="aiokafka",
    version=read_version(),
    description=("Kafka integration with asyncio."),
    long_description="\n\n".join((read("README.rst"), read("CHANGES.rst"))),
    classifiers=classifiers,
    platforms=["POSIX"],
    author="Andrew Svetlov",
    author_email="andrew.svetlov@gmail.com",
    url="http://aiokafka.readthedocs.org",
    download_url="https://pypi.python.org/pypi/aiokafka",
    license="Apache 2",
    packages=["aiokafka"],
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
    ext_modules=extensions,
    cmdclass=dict(build_ext=ve_build_ext, bdist_rpm=bdist_rpm),
)

try:
    setup(**args)
except BuildFailed:
    print("************************************************************")
    print("Cannot compile C accelerator module, use pure python version")
    print("************************************************************")
    del args["ext_modules"]
    del args["cmdclass"]
    setup(**args)
