"""Tests for jobs/web.py helpers — auto-accept form parsing.

Pins the multidict-vs-Form() contract that `_parse_auto_accept_form`
exists to enforce. Form(default=None) + str|None on FastAPI/Pydantic v2
collapses ``enabled=`` (empty value, switch off) and "field absent"
(no change) to the same None — this helper distinguishes them via
membership. A regression to ``Form(...)``-style parsing would be
caught here.
"""

from __future__ import annotations

import zipfile

import pytest

from social_data_pipeline.jobs.web import _build_result_zip, _parse_auto_accept_form


class TestParseAutoAcceptForm:
    def test_empty_enabled_means_switch_off(self):
        # The off-state of an HTMX/JS-built body is `enabled=` (key
        # present, value empty). Must round-trip to False, not None.
        enabled, limit = _parse_auto_accept_form({"enabled": ""})
        assert enabled is False
        assert limit is None

    def test_enabled_on_means_switch_on(self):
        enabled, limit = _parse_auto_accept_form({"enabled": "on"})
        assert enabled is True
        assert limit is None

    def test_absent_enabled_means_no_change(self):
        # A slider-only POST (no enabled field at all) must not flip
        # the switch; None signals "leave alone".
        enabled, limit = _parse_auto_accept_form({"limit": "3"})
        assert enabled is None
        assert limit == 3

    def test_limit_parses_int(self):
        enabled, limit = _parse_auto_accept_form({"limit": "7"})
        assert enabled is None
        assert limit == 7

    def test_limit_garbage_yields_none(self):
        # A stale browser tab or a hand-crafted curl with `limit=abc`
        # must not 500 — the route should treat it as "no change".
        enabled, limit = _parse_auto_accept_form({"limit": "abc"})
        assert limit is None

    def test_limit_empty_yields_none(self):
        enabled, limit = _parse_auto_accept_form({"limit": ""})
        assert limit is None

    def test_both_fields_present(self):
        enabled, limit = _parse_auto_accept_form({"enabled": "on", "limit": "5"})
        assert enabled is True
        assert limit == 5

    def test_empty_form(self):
        enabled, limit = _parse_auto_accept_form({})
        assert enabled is None
        assert limit is None


class TestBuildResultZip:
    def test_zips_all_files_flat(self, tmp_path):
        # Multi-part parquet shardset is the common case; the zip must
        # contain every part at the archive root (arcname = basename),
        # not nested under the folder name.
        (tmp_path / "part-0.parquet").write_bytes(b"alpha-bytes")
        (tmp_path / "part-1.parquet").write_bytes(b"beta-bytes")
        buf = _build_result_zip(tmp_path)
        try:
            with zipfile.ZipFile(buf) as zf:
                names = sorted(zf.namelist())
                assert names == ["part-0.parquet", "part-1.parquet"]
                assert zf.read("part-0.parquet") == b"alpha-bytes"
                assert zf.read("part-1.parquet") == b"beta-bytes"
        finally:
            buf.close()

    def test_skips_subdirectories(self, tmp_path):
        # Result folders are flat by contract; if something nests a
        # subdir in there (debug dump, stray index file), don't recurse
        # — the zip mirrors what Preview sees.
        (tmp_path / "part-0.parquet").write_bytes(b"x")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "nested.parquet").write_bytes(b"y")
        buf = _build_result_zip(tmp_path)
        try:
            with zipfile.ZipFile(buf) as zf:
                assert zf.namelist() == ["part-0.parquet"]
        finally:
            buf.close()

    def test_missing_folder_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _build_result_zip(tmp_path / "does-not-exist")

    def test_empty_folder_raises(self, tmp_path):
        # An empty result folder is a programming error (status=done
        # implies parts exist); the route surfaces it as 404 rather
        # than handing the user a 22-byte empty zip.
        with pytest.raises(RuntimeError):
            _build_result_zip(tmp_path)
