"""Tests for the profile-gating lookup tables in sdp.py.

These tables drive `cmd_run`'s pre-flight checks: every pipeline profile
must declare which DB it requires (or None) and which per-source override
file it expects. If a future profile is added to `VALID_PROFILES` without
being added to both tables, `sdp run <new_profile>` will silently bypass
the gates — exactly the failure mode this plan is trying to prevent.
These tests catch that omission.
"""

from __future__ import annotations

import sdp


class TestProfileGatingTables:
    def test_profile_db_mapping_complete(self):
        """Every VALID_PROFILES entry must be a key in _PROFILE_DB."""
        missing = set(sdp.VALID_PROFILES) - set(sdp._PROFILE_DB)
        assert not missing, (
            f"Profiles missing from _PROFILE_DB: {sorted(missing)}. "
            "Every profile must declare its DB dependency (or None)."
        )

    def test_profile_source_file_mapping_complete(self):
        """Every VALID_PROFILES entry must be a key in _PROFILE_SOURCE_FILE."""
        missing = set(sdp.VALID_PROFILES) - set(sdp._PROFILE_SOURCE_FILE)
        assert not missing, (
            f"Profiles missing from _PROFILE_SOURCE_FILE: {sorted(missing)}. "
            "Every profile must declare its expected source override filename."
        )

    def test_profile_db_values_known(self):
        """Non-None _PROFILE_DB values must reference a real DB."""
        valid_dbs = {"postgres", "mongo", "starrocks"}
        for profile, db in sdp._PROFILE_DB.items():
            if db is None:
                continue
            assert db in valid_dbs, (
                f"_PROFILE_DB[{profile!r}] = {db!r}, expected one of {valid_dbs}."
            )

    def test_db_to_source_files_covers_all_dbs(self):
        """Every DB referenced by _PROFILE_DB must have an entry in
        _DB_TO_SOURCE_FILES so `db unsetup` knows which override files to
        clean up for it."""
        referenced = {db for db in sdp._PROFILE_DB.values() if db is not None}
        missing = referenced - set(sdp._DB_TO_SOURCE_FILES)
        assert not missing, (
            f"DBs in _PROFILE_DB but missing from _DB_TO_SOURCE_FILES: "
            f"{sorted(missing)}."
        )


class TestDeleteSourceDbOverrides:
    def _make_source(self, root, name, files):
        src = root / "config" / "sources" / name
        src.mkdir(parents=True)
        for f in files:
            (src / f).write_text("pipeline: {}\n")
        return src

    def test_deletes_only_named_db_files(self, tmp_path, monkeypatch):
        # postgres unsetup must remove postgres.yaml + postgres_ml.yaml
        # but leave mongo.yaml, starrocks.yaml, parse.yaml, platform.yaml.
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
        src = self._make_source(
            tmp_path,
            "reddit",
            [
                "platform.yaml",
                "parse.yaml",
                "postgres.yaml",
                "postgres_ml.yaml",
                "mongo.yaml",
                "starrocks.yaml",
                "sr_ml.yaml",
            ],
        )

        n = sdp._delete_source_db_overrides(["postgres"])

        assert n == 2
        assert not (src / "postgres.yaml").exists()
        assert not (src / "postgres_ml.yaml").exists()
        # Other DBs untouched
        assert (src / "mongo.yaml").exists()
        assert (src / "starrocks.yaml").exists()
        assert (src / "sr_ml.yaml").exists()
        # Non-DB files untouched
        assert (src / "platform.yaml").exists()
        assert (src / "parse.yaml").exists()

    def test_deletes_across_multiple_sources(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
        a = self._make_source(tmp_path, "alpha", ["postgres.yaml", "parse.yaml"])
        b = self._make_source(tmp_path, "beta", ["postgres.yaml", "mongo.yaml"])

        n = sdp._delete_source_db_overrides(["postgres"])

        assert n == 2
        assert not (a / "postgres.yaml").exists()
        assert not (b / "postgres.yaml").exists()
        assert (a / "parse.yaml").exists()
        assert (b / "mongo.yaml").exists()

    def test_handles_missing_sources_dir(self, tmp_path, monkeypatch):
        # No config/sources/ at all → no error, returns 0.
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
        assert sdp._delete_source_db_overrides(["postgres"]) == 0

    def test_full_unsetup_deletes_all_db_files(self, tmp_path, monkeypatch):
        # Passing all three DBs must wipe every DB override file.
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path / "config")
        src = self._make_source(
            tmp_path,
            "reddit",
            [
                "platform.yaml",
                "parse.yaml",
                "postgres.yaml",
                "postgres_ml.yaml",
                "mongo.yaml",
                "starrocks.yaml",
                "sr_ml.yaml",
            ],
        )

        n = sdp._delete_source_db_overrides(["postgres", "mongo", "starrocks"])

        assert n == 5
        for fname in ("postgres.yaml", "postgres_ml.yaml", "mongo.yaml",
                      "starrocks.yaml", "sr_ml.yaml"):
            assert not (src / fname).exists()
        assert (src / "platform.yaml").exists()
        assert (src / "parse.yaml").exists()
