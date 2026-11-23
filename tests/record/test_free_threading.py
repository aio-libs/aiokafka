import contextlib
import subprocess
import sys
import sysconfig
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import ModuleType
from typing import Any

import pytest

from aiokafka.codec import has_gzip

_crecords: ModuleType | None
try:
    from aiokafka.record import _crecords
except ImportError:  # pragma: no cover
    _crecords = None

requires_extensions = pytest.mark.skipif(
    _crecords is None, reason="C extensions are not available"
)

THREADS = 8
APPENDS_PER_THREAD = 100
KEY = b"k" * 50
# Small values, and values above the 5 KiB at which the CRC helpers detach
# the thread state.
VALUES = [b"v" * 300, b"v" * 8000]


def _run_concurrently(fn: Callable[[], object], threads: int = THREADS) -> None:
    barrier = threading.Barrier(threads)

    def worker() -> None:
        barrier.wait()
        fn()

    with ThreadPoolExecutor(threads) as pool:
        futures = [pool.submit(worker) for _ in range(threads)]
    for future in futures:
        future.result()


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="Requires a free-threaded Python build",
)
@requires_extensions
def test_extensions_keep_the_gil_disabled() -> None:
    # -I: ignore PYTHON_GIL and don't import a source checkout from the CWD.
    out = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys, aiokafka.record._crecords; print(sys._is_gil_enabled())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "False", out.stderr


@requires_extensions
@pytest.mark.parametrize("value", VALUES, ids=len)
def test_default_builder_shared_between_threads(value: bytes) -> None:
    assert _crecords is not None
    builder = _crecords.DefaultRecordBatchBuilder(2, 0, 0, -1, -1, -1, 1 << 30)

    def append() -> None:
        for _ in range(APPENDS_PER_THREAD):
            builder.append(1, 0, KEY, value, [("h", b"x")])

    _run_concurrently(append)

    batch = _crecords.DefaultRecordBatch(bytes(builder.build()))
    assert batch.validate_crc()
    records = list(batch)
    assert len(records) == THREADS * APPENDS_PER_THREAD
    assert all(
        r.key == KEY and r.value == value and r.headers == [("h", b"x")]
        for r in records
    )


@requires_extensions
@pytest.mark.parametrize("value", VALUES, ids=len)
def test_legacy_builder_shared_between_threads(value: bytes) -> None:
    assert _crecords is not None
    builder = _crecords.LegacyRecordBatchBuilder(1, 0, 1 << 30)

    def append() -> None:
        for _ in range(APPENDS_PER_THREAD):
            builder.append(1, 0, KEY, value)

    _run_concurrently(append)

    records = _crecords.MemoryRecords(bytes(builder.build()))
    count = 0
    while records.has_next():
        batch = records.next_batch()
        assert batch.validate_crc()
        for record in batch:
            assert record.key == KEY
            assert record.value == value
            count += 1
    assert count == THREADS * APPENDS_PER_THREAD


@requires_extensions
@pytest.mark.skipif(not has_gzip(), reason="Gzip not available")
def test_compressed_legacy_batch_shared_between_threads() -> None:
    assert _crecords is not None
    builder = _crecords.LegacyRecordBatchBuilder(1, 1, 1 << 30)  # gzip
    for offset in range(50):
        builder.append(offset, 0, KEY, VALUES[1])
    data = bytes(builder.build())

    def iterate_and_validate(batch: Any) -> None:
        batch.validate_crc()
        assert [r.value for r in batch] == [VALUES[1]] * 50

    for _ in range(20):
        batch = _crecords.LegacyRecordBatch(data, 1)
        _run_concurrently(partial(iterate_and_validate, batch))


@requires_extensions
@pytest.mark.parametrize("kind", ["default", "legacy"])
def test_value_resized_during_append(kind: str) -> None:
    assert _crecords is not None
    short, long = b"v" * 100, b"v" * 200
    value = bytearray(short)
    stop = threading.Event()

    def resize() -> None:
        while not stop.is_set():
            # The builder holds the value's buffer while it measures and
            # copies it, so a resize then fails instead of racing the copy.
            with contextlib.suppress(BufferError):
                if len(value) == len(short):
                    value.extend(short)
                else:
                    del value[len(short) :]

    resizer = threading.Thread(target=resize)
    resizer.start()
    try:
        for _ in range(200):
            if kind == "default":
                default = _crecords.DefaultRecordBatchBuilder(
                    2, 0, 0, -1, -1, -1, 1 << 30
                )
                default.append(0, 0, KEY, value, [])
                [record] = _crecords.DefaultRecordBatch(bytes(default.build()))
            else:
                legacy = _crecords.LegacyRecordBatchBuilder(1, 0, 1 << 30)
                legacy.append(0, 0, KEY, value)
                [record] = _crecords.LegacyRecordBatch(bytes(legacy.build()), 1)
            assert record.key == KEY
            assert record.value in (short, long)
    finally:
        stop.set()
        resizer.join()
