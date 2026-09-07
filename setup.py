import platform

from Cython.Build import cythonize
from setuptools import Extension, setup
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
        optional=True,
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
        optional=True,
    ),
    Extension(
        "aiokafka.record._crecords.memory_records",
        ["aiokafka/record/_crecords/memory_records.pyx"],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
        optional=True,
    ),
    Extension(
        "aiokafka.record._crecords.cutil",
        ["aiokafka/record/_crecords/crc32c.c", "aiokafka/record/_crecords/cutil.pyx"],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
        optional=True,
    ),
]


class optional_build_ext(build_ext):
    """Allow installation to fall back to the pure-Python implementation."""

    def run(self):
        try:
            super().run()
        except (CCompilerError, ExecError, OSError, PlatformError) as exc:
            self.warn(f"building C extensions failed: {exc}")

    def build_extension(self, ext):
        try:
            super().build_extension(ext)
        except (CCompilerError, ExecError, OSError, PlatformError, ValueError) as exc:
            self.warn(f"building extension {ext.name!r} failed: {exc}")


setup(
    ext_modules=cythonize(extensions),
    cmdclass={"build_ext": optional_build_ext},
)
