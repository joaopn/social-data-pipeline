"""Tests for the two add-path pure functions used by `sdp db setup --add`.

Both functions exist precisely so adding a database to an existing setup
doesn't clobber the others. Silent regressions here look like "I ran
`sdp db setup --add mongo` and now my postgres port env var is gone."

- `update_env_file`: merges KEY=VALUE updates into .env, replacing existing
  occurrences (commented or not), appending unknowns, leaving siblings alone.
- `_update_override_volumes`: writes a service's volume mounts to
  docker-compose.override.yml while preserving other services already
  present in the file.

The unsetup symmetric trim logic (`db unsetup <db>`) is inlined inside
`_unsetup_single_db` and is covered indirectly by the auth full-cycle
E2E test. Not refactored just for testability.
"""

from __future__ import annotations

import yaml

from social_data_pipeline.setup.utils import update_env_file
from social_data_pipeline.setup.db import _update_override_volumes, generate_db_starrocks_yaml


# ── update_env_file ─────────────────────────────────────────────────────────


class TestUpdateEnvFile:
    def test_creates_env_when_missing(self, tmp_path, monkeypatch):
        # No existing .env → new file created with just the updates.
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        update_env_file({"FOO": "1", "BAR": "2"})
        env = (tmp_path / ".env").read_text()
        assert "FOO=1" in env
        assert "BAR=2" in env

    def test_replaces_existing_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("FOO=old\nUNRELATED=keep\n")
        update_env_file({"FOO": "new"})
        env = (tmp_path / ".env").read_text()
        assert "FOO=new" in env
        assert "FOO=old" not in env
        # Critical: unrelated keys must survive.
        assert "UNRELATED=keep" in env

    def test_replaces_commented_key(self, tmp_path, monkeypatch):
        # An entry commented out (e.g. earlier `# FOO=`) gets replaced too.
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("# FOO=\nUNRELATED=keep\n")
        update_env_file({"FOO": "new"})
        env = (tmp_path / ".env").read_text()
        assert "FOO=new" in env
        assert "# FOO=" not in env
        assert "UNRELATED=keep" in env

    def test_appends_new_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("EXISTING=v\n")
        update_env_file({"NEW": "added"})
        env = (tmp_path / ".env").read_text()
        assert "EXISTING=v" in env
        assert "NEW=added" in env

    def test_empty_value_writes_commented_form(self, tmp_path, monkeypatch):
        # An empty value writes "# KEY=" — preserves the key as a placeholder
        # while signalling unset. Used for optional auth env vars.
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("")
        update_env_file({"OPTIONAL": ""})
        env = (tmp_path / ".env").read_text()
        assert "# OPTIONAL=" in env

    def test_preserves_blank_lines_and_comments(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        original = (
            "# Header comment\n"
            "\n"
            "FOO=1\n"
            "\n"
            "# Section\n"
            "BAR=2\n"
        )
        (tmp_path / ".env").write_text(original)
        update_env_file({"BAZ": "3"})
        env = (tmp_path / ".env").read_text()
        assert "# Header comment" in env
        assert "# Section" in env
        assert "FOO=1" in env
        assert "BAR=2" in env
        assert "BAZ=3" in env

    def test_idempotent_when_values_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("FOO=1\nBAR=2\n")
        update_env_file({"FOO": "1"})
        env = (tmp_path / ".env").read_text()
        # Single FOO=1 line, BAR untouched.
        assert env.count("FOO=1") == 1
        assert "BAR=2" in env

    def test_multiple_keys_at_once(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.utils.ROOT", tmp_path)
        (tmp_path / ".env").write_text("KEEP=k\nFOO=old\n")
        update_env_file({"FOO": "new", "BAR": "added"})
        env = (tmp_path / ".env").read_text()
        assert "KEEP=k" in env
        assert "FOO=new" in env
        assert "BAR=added" in env
        assert "FOO=old" not in env


# ── _update_override_volumes ────────────────────────────────────────────────


def _read_override(path):
    return yaml.safe_load(path.read_text()) or {}


class TestUpdateOverrideVolumes:
    def test_creates_file_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("social_data_pipeline.setup.db.ROOT", tmp_path)
        _update_override_volumes("postgres", ["./pg_data:/var/lib/postgresql/data"])
        data = _read_override(tmp_path / "docker-compose.override.yml")
        assert data["services"]["postgres"]["volumes"] == [
            "./pg_data:/var/lib/postgresql/data"
        ]

    def test_preserves_other_services(self, tmp_path, monkeypatch):
        # Critical regression check: adding a service must not wipe
        # services already listed in the override file.
        monkeypatch.setattr("social_data_pipeline.setup.db.ROOT", tmp_path)
        existing = {
            "services": {
                "postgres": {"volumes": ["./pg_data:/var/lib/postgresql/data"]},
            },
        }
        (tmp_path / "docker-compose.override.yml").write_text(yaml.dump(existing))

        _update_override_volumes("mongo", ["./mongo_data:/data/db"])

        data = _read_override(tmp_path / "docker-compose.override.yml")
        # Both services present.
        assert "postgres" in data["services"]
        assert "mongo" in data["services"]
        # Original postgres mount untouched.
        assert data["services"]["postgres"]["volumes"] == [
            "./pg_data:/var/lib/postgresql/data"
        ]
        assert data["services"]["mongo"]["volumes"] == ["./mongo_data:/data/db"]

    def test_replaces_same_service(self, tmp_path, monkeypatch):
        # Re-running setup --add for the same DB must replace its mounts,
        # not append. (Setup is the source of truth for that DB's mounts.)
        monkeypatch.setattr("social_data_pipeline.setup.db.ROOT", tmp_path)
        existing = {
            "services": {
                "postgres": {"volumes": ["./old:/var/lib/postgresql/data"]},
            },
        }
        (tmp_path / "docker-compose.override.yml").write_text(yaml.dump(existing))

        _update_override_volumes("postgres", ["./new:/var/lib/postgresql/data"])

        data = _read_override(tmp_path / "docker-compose.override.yml")
        assert data["services"]["postgres"]["volumes"] == [
            "./new:/var/lib/postgresql/data"
        ]

    def test_corrupt_yaml_does_not_explode(self, tmp_path, monkeypatch):
        # Defensive: malformed override yaml is silently treated as "start
        # fresh" rather than crashing setup.
        monkeypatch.setattr("social_data_pipeline.setup.db.ROOT", tmp_path)
        (tmp_path / "docker-compose.override.yml").write_text(
            "this: is\n  not: [valid"
        )

        _update_override_volumes("mongo", ["./mongo:/data/db"])

        data = _read_override(tmp_path / "docker-compose.override.yml")
        assert "mongo" in data["services"]

    def test_writes_auto_generated_header(self, tmp_path, monkeypatch):
        # The header signals the file is owned by sdp setup and helps
        # users know not to hand-edit it.
        monkeypatch.setattr("social_data_pipeline.setup.db.ROOT", tmp_path)
        _update_override_volumes("postgres", ["./pg:/var/lib/postgresql/data"])
        text = (tmp_path / "docker-compose.override.yml").read_text()
        assert text.startswith("# Auto-generated")


class TestGenerateDbStarrocksYaml:
    def test_writes_chosen_compression(self):
        data = yaml.safe_load(generate_db_starrocks_yaml({"sr_compression": "LZ4"}))
        assert data["compression"] == "LZ4"

    def test_defaults_to_zstd(self):
        data = yaml.safe_load(generate_db_starrocks_yaml({}))
        assert data["compression"] == "ZSTD"
