"""Tests for _device_stem — the IoTDB values device naming.

Guards two fixes:
  • same filename + different content must NOT overwrite (a content tag makes the
    device unique per upload), while the exact same file re-uploaded stays the same
    stem (idempotent);
  • a degenerate filename (empty / all-punctuation / all-underscore) must fall back
    to a valid stem instead of producing a broken/junk device path.

Pure — no IoTDB.
"""

from __future__ import annotations

from pathlib import Path

from p0.api.database.timeseries import _device_stem


def test_plain_name_without_tag_is_normalized_only():
    assert _device_stem("data.csv", Path("x.csv")) == "data"
    assert (
        _device_stem("Hydrogen Consumers (Network).xlsx", Path("x"))
        == "Hydrogen_Consumers_Network"
    )


def test_content_tag_makes_same_name_distinct():
    a = _device_stem("data.csv", Path("x.csv"), content_tag="aaaa1111")
    b = _device_stem("data.csv", Path("x.csv"), content_tag="bbbb2222")
    assert a != b
    assert a.startswith("data__") and b.startswith("data__")


def test_same_file_same_stem_idempotent():
    a = _device_stem("data.csv", Path("x.csv"), content_tag="deadbeef")
    b = _device_stem("data.csv", Path("x.csv"), content_tag="deadbeef")
    assert a == b


def test_degenerate_stem_falls_back_to_file():
    assert _device_stem("___.csv", Path("___.csv")) == "file"
    s = _device_stem("已经.csv", Path("已经.csv"), content_tag="abcd1234")
    assert s.startswith("file")
    assert s.strip("_")
