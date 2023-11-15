import platform

from Cython.Build import cythonize
from setuptools import Extension, setup
from setuptools.command.bdist_rpm import bdist_rpm as _bdist_rpm
from setuptools.command.build_ext import build_ext
from setuptools.errors import CCompilerError, ExecError, PlatformError


# Those are needed to build _hton for windows

CFLAGS = ["-O2"]
LDFLAGS = []
LIBRARIES = []

if platform.uname().system == "Windows":
    LDFLAGS.append("ws2_32.lib")
else:
    CFLAGS.extend(["-Wall", "-Wsign-compare", "-Wconversion"])
    LIBRARIES.append("z")


extensions = [
    Extension(
        "aiokafka.record._crecords.legacy_records",
        ["aiokafka/record/_crecords/legacy_records.pyx"],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.default_records",
        [
            "aiokafka/record/_crecords/crc32c.c",
            "aiokafka/record/_crecords/default_records.pyx",
        ],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.memory_records",
        ["aiokafka/record/_crecords/memory_records.pyx"],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        "aiokafka.record._crecords.cutil",
        ["aiokafka/record/_crecords/crc32c.c", "aiokafka/record/_crecords/cutil.pyx"],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
]


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
        except (PlatformError, FileNotFoundError):
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, ExecError, PlatformError, ValueError):
            raise BuildFailed()


setup(
    ext_modules=cythonize(extensions),
    cmdclass=dict(build_ext=ve_build_ext, bdist_rpm=bdist_rpm),
)
