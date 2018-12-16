import pytest

from aiokafka.record.control_record import \
    ControlRecord, ABORT_MARKER, COMMIT_MARKER


@pytest.mark.parametrize("data,marker", [
    (b"\x00\x00\x00\x00", ABORT_MARKER),
    (b"\x00\x00\x00\x01", COMMIT_MARKER),
])
def test_control_record_serde(data, marker):
    assert ControlRecord.parse(data) == marker


def test_control_record_parse():
    record = ControlRecord.parse(b"\x00\x01\x00\x01")
    assert record.version == 1
    assert record.type_ == 1

    record = ControlRecord.parse(b"\xFF\xFF\xFF\xFF")
    assert record.version == 65535
    assert record.type_ == 65535

    record = ControlRecord.parse(b"\x00\xFF\x00\x00")
    assert record.version == 255
    assert record.type_ == 0


def test_control_record_other():
    record = ControlRecord.parse(b"\x00\x00\x00\x01")
    assert record != 1
    assert record != object()

    assert repr(record) == "ControlRecord(version=0, type_=1)"
