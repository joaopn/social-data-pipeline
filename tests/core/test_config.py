"""Tests for social_data_pipeline.core.config."""

import pytest
import yaml

from social_data_pipeline.core.config import (
    deep_merge,
    load_yaml_file,
    get_required,
    get_optional,
    validate_processing_config,
    validate_database_config,
    validate_mongo_config,
    validate_starrocks_config,
    validate_classifier_config,
    load_profile_config,
    load_platform_config,
    apply_env_overrides,
    get_platform_fields,
    ConfigurationError,
)


# ── deep_merge ──────────────────────────────────────────────────────────────

class TestDeepMerge:
    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 3}

    def test_nested_merge(self):
        base = {"x": {"a": 1, "b": 2}}
        override = {"x": {"b": 99, "c": 3}}
        result = deep_merge(base, override)
        assert result == {"x": {"a": 1, "b": 99, "c": 3}}

    def test_lists_replace_by_default(self):
        base = {"items": [1, 2, 3]}
        override = {"items": [4, 5]}
        result = deep_merge(base, override)
        assert result["items"] == [4, 5]

    def test_originals_unchanged(self):
        base = {"x": {"a": 1}}
        override = {"x": {"b": 2}}
        deep_merge(base, override)
        assert base == {"x": {"a": 1}}
        assert override == {"x": {"b": 2}}

    def test_new_keys_added(self):
        result = deep_merge({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_empty_base(self):
        result = deep_merge({}, {"a": 1})
        assert result == {"a": 1}

    def test_empty_override(self):
        result = deep_merge({"a": 1}, {})
        assert result == {"a": 1}

    def test_deeply_nested(self):
        base = {"a": {"b": {"c": 1, "d": 2}}}
        override = {"a": {"b": {"c": 99}}}
        result = deep_merge(base, override)
        assert result["a"]["b"]["c"] == 99
        assert result["a"]["b"]["d"] == 2


# ── load_yaml_file ─────────────────────────────────────────────────────────

class TestLoadYamlFile:
    def test_file_not_found(self, tmp_path):
        assert load_yaml_file(tmp_path / "nonexistent.yaml") is None

    def test_valid_yaml(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text("key: value\nnested:\n  a: 1\n")
        result = load_yaml_file(f)
        assert result == {"key": "value", "nested": {"a": 1}}

    def test_empty_yaml_returns_empty_dict(self, tmp_path):
        f = tmp_path / "empty.yaml"
        f.write_text("")
        assert load_yaml_file(f) == {}

    def test_invalid_yaml_raises(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("key: [invalid\n")
        with pytest.raises(ConfigurationError, match="Failed to parse"):
            load_yaml_file(f)


# ── get_required / get_optional ─────────────────────────────────────────────

class TestGetRequired:
    def test_simple_key(self):
        assert get_required({"a": 42}, "a") == 42

    def test_nested_keys(self):
        cfg = {"x": {"y": {"z": "found"}}}
        assert get_required(cfg, "x", "y", "z") == "found"

    def test_missing_key_raises(self):
        with pytest.raises(ConfigurationError, match="processing"):
            get_required({}, "processing", "data_types")

    def test_custom_error_msg(self):
        with pytest.raises(ConfigurationError, match="custom msg"):
            get_required({}, "k", error_msg="custom msg")

    def test_non_dict_intermediate_raises(self):
        with pytest.raises(ConfigurationError):
            get_required({"a": "not_a_dict"}, "a", "b")


class TestGetOptional:
    def test_present(self):
        assert get_optional({"a": 1}, "a") == 1

    def test_missing_returns_default(self):
        assert get_optional({}, "missing", default=99) == 99

    def test_nested_present(self):
        assert get_optional({"a": {"b": 2}}, "a", "b") == 2

    def test_nested_missing(self):
        assert get_optional({"a": {}}, "a", "b", default="x") == "x"


# ── validate_processing_config ──────────────────────────────────────────────

class TestValidateProcessingConfig:
    def test_valid(self):
        cfg = {"processing": {"data_types": ["comments"]}}
        validate_processing_config(cfg, "parse")  # should not raise

    def test_missing_processing_key(self):
        with pytest.raises(ConfigurationError, match="data_types"):
            validate_processing_config({}, "parse")

    def test_missing_data_types(self):
        with pytest.raises(ConfigurationError, match="data_types"):
            validate_processing_config({"processing": {}}, "parse")


# ── validate_database_config ────────────────────────────────────────────────

class TestValidateDatabaseConfig:
    VALID_DB = {"database": {"host": "h", "port": 5432, "name": "n", "schema": "s", "user": "u"}}

    def test_valid(self):
        validate_database_config(self.VALID_DB)

    @pytest.mark.parametrize("missing_key", ["host", "port", "name", "schema", "user"])
    def test_missing_key(self, missing_key):
        db = {k: v for k, v in self.VALID_DB["database"].items() if k != missing_key}
        with pytest.raises(ConfigurationError, match=missing_key):
            validate_database_config({"database": db})

    def test_no_database_section(self):
        with pytest.raises(ConfigurationError):
            validate_database_config({})


# ── validate_mongo_config ───────────────────────────────────────────────────

class TestValidateMongoConfig:
    def test_valid(self):
        validate_mongo_config({"database": {"host": "h", "port": 27017}})

    @pytest.mark.parametrize("missing_key", ["host", "port"])
    def test_missing_key(self, missing_key):
        db = {"host": "h", "port": 27017}
        del db[missing_key]
        with pytest.raises(ConfigurationError, match=missing_key):
            validate_mongo_config({"database": db})


# ── validate_starrocks_config ──────────────────────────────────────────────

class TestValidateStarrocksConfig:
    def test_valid(self):
        validate_starrocks_config({"database": {"host": "h", "port": 9030, "user": "root"}})

    @pytest.mark.parametrize("missing_key", ["host", "port", "user"])
    def test_missing_key(self, missing_key):
        db = {"host": "h", "port": 9030, "user": "root"}
        del db[missing_key]
        with pytest.raises(ConfigurationError, match=missing_key):
            validate_starrocks_config({"database": db})

    def test_no_database_section(self):
        with pytest.raises(ConfigurationError):
            validate_starrocks_config({})

    def test_absent_compression_is_valid(self):
        # Backward compatible: no compression key → no-op.
        validate_starrocks_config({"database": {"host": "h", "port": 9030, "user": "root"}})

    @pytest.mark.parametrize("codec", ["LZ4", "ZSTD", "ZLIB", "SNAPPY"])
    def test_valid_compression(self, codec):
        validate_starrocks_config(
            {"database": {"host": "h", "port": 9030, "user": "root", "compression": codec}}
        )

    def test_invalid_compression_raises(self):
        with pytest.raises(ConfigurationError, match="compression"):
            validate_starrocks_config(
                {"database": {"host": "h", "port": 9030, "user": "root", "compression": "GZIP"}}
            )


# ── validate_classifier_config ──────────────────────────────────────────────

class TestValidateClassifierConfig:
    def test_lingua_valid(self):
        cfg = {"suffix": "_lang", "languages": ["ENGLISH"]}
        validate_classifier_config(cfg, "lingua", "lingua")

    def test_lingua_missing_languages(self):
        with pytest.raises(ConfigurationError, match="languages"):
            validate_classifier_config({"suffix": "_lang"}, "lingua", "lingua")

    def test_gpu_valid(self):
        cfg = {"suffix": "_toxic", "model": "some/model"}
        validate_classifier_config(cfg, "toxic", "ml")

    def test_gpu_missing_model(self):
        with pytest.raises(ConfigurationError, match="model"):
            validate_classifier_config({"suffix": "_toxic"}, "toxic", "ml")

    def test_gpu_missing_suffix(self):
        with pytest.raises(ConfigurationError, match="suffix"):
            validate_classifier_config({"model": "m"}, "toxic", "ml")


# ── load_profile_config ────────────────────────────────────────────────────

class TestLoadProfileConfig:
    def _setup_parse_profile(self, tmp_path):
        """Create minimal parse profile config tree."""
        parse_dir = tmp_path / "parse"
        parse_dir.mkdir()
        pipeline = {"processing": {"data_types": ["comments"], "workers": 4}}
        (parse_dir / "pipeline.yaml").write_text(yaml.dump(pipeline))
        return tmp_path

    def test_loads_base_config(self, tmp_path):
        cfg_dir = self._setup_parse_profile(tmp_path)
        result = load_profile_config("parse", config_dir=str(cfg_dir), quiet=True)
        assert result["processing"]["data_types"] == ["comments"]

    def test_source_override(self, tmp_path):
        cfg_dir = self._setup_parse_profile(tmp_path)
        src = tmp_path / "sources" / "reddit"
        src.mkdir(parents=True)
        override = {"pipeline": {"processing": {"workers": 32}}}
        (src / "parse.yaml").write_text(yaml.dump(override))

        result = load_profile_config("parse", config_dir=str(cfg_dir), source="reddit", quiet=True)
        assert result["processing"]["workers"] == 32
        assert result["processing"]["data_types"] == ["comments"]

    def test_unknown_profile_raises(self, tmp_path):
        # "bogus" has no config directory, so it raises before checking profile name
        (tmp_path / "bogus").mkdir()
        with pytest.raises(ConfigurationError, match="Unknown profile"):
            load_profile_config("bogus", config_dir=str(tmp_path))

    def test_missing_config_dir_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="Config directory not found"):
            load_profile_config("parse", config_dir=str(tmp_path / "nope"))

    def test_missing_base_yaml_raises(self, tmp_path):
        (tmp_path / "parse").mkdir()
        with pytest.raises(ConfigurationError, match="Required config file not found"):
            load_profile_config("parse", config_dir=str(tmp_path), quiet=True)


# ── load_platform_config ───────────────────────────────────────────────────

class TestLoadPlatformConfig:
    def test_loads_from_source(self, tmp_path):
        src = tmp_path / "sources" / "mysrc"
        src.mkdir(parents=True)
        (src / "platform.yaml").write_text(yaml.dump({"platform": "custom"}))
        result = load_platform_config(config_dir=str(tmp_path), source="mysrc")
        assert result["platform"] == "custom"

    def test_missing_source_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="Platform config not found"):
            load_platform_config(config_dir=str(tmp_path), source="nope")

    def test_falls_back_to_env(self, tmp_path, monkeypatch):
        src = tmp_path / "sources" / "envsrc"
        src.mkdir(parents=True)
        (src / "platform.yaml").write_text(yaml.dump({"from": "env"}))
        monkeypatch.setenv("SOURCE", "envsrc")
        # Clear PLATFORM so SOURCE is used
        monkeypatch.delenv("PLATFORM", raising=False)
        result = load_platform_config(config_dir=str(tmp_path))
        assert result["from"] == "env"


# ── apply_env_overrides ────────────────────────────────────────────────────

class TestApplyEnvOverrides:
    def test_postgres_port_override(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        result = apply_env_overrides({"database": {"port": 5432}}, "postgres_ingest")
        assert result["database"]["port"] == 5433

    def test_postgres_password_override(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        result = apply_env_overrides({"database": {}}, "postgres_ingest")
        assert result["database"]["password"] == "secret"

    def test_postgres_creates_database_key(self, monkeypatch):
        monkeypatch.setenv("DB_NAME", "mydb")
        result = apply_env_overrides({}, "postgres_ingest")
        assert result["database"]["name"] == "mydb"

    def test_mongo_port_override(self, monkeypatch):
        monkeypatch.setenv("MONGO_PORT", "27018")
        result = apply_env_overrides({"database": {}}, "mongo_ingest")
        assert result["database"]["port"] == 27018

    def test_mongo_auth_override(self, monkeypatch):
        monkeypatch.setenv("MONGO_ADMIN_USER", "admin")
        monkeypatch.setenv("MONGO_ADMIN_PASSWORD", "pass")
        result = apply_env_overrides({"database": {}}, "mongo_ingest")
        assert result["database"]["user"] == "admin"
        assert result["database"]["password"] == "pass"

    def test_irrelevant_profile_unchanged(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PORT", "9999")
        result = apply_env_overrides({"database": {"port": 5432}}, "parse")
        assert result["database"]["port"] == 5432

    def test_original_not_mutated(self, monkeypatch):
        monkeypatch.setenv("DB_NAME", "newdb")
        original = {"database": {"name": "olddb"}}
        apply_env_overrides(original, "postgres_ingest")
        assert original["database"]["name"] == "olddb"

    def test_starrocks_port_override(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_PORT", "9031")
        result = apply_env_overrides({"database": {}}, "sr_ingest")
        assert result["database"]["port"] == 9031

    def test_starrocks_fe_http_port_override(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_FE_HTTP_PORT", "8031")
        result = apply_env_overrides({"database": {}}, "sr_ingest")
        assert result["database"]["fe_http_port"] == 8031

    def test_starrocks_password_override(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_ROOT_PASSWORD", "srpass")
        result = apply_env_overrides({"database": {}}, "sr_ingest")
        assert result["database"]["password"] == "srpass"

    def test_starrocks_creates_database_key(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_PORT", "9030")
        result = apply_env_overrides({}, "sr_ingest")
        assert result["database"]["port"] == 9030

    def test_starrocks_sr_ml_profile(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_PORT", "9031")
        monkeypatch.setenv("STARROCKS_ROOT_PASSWORD", "secret")
        result = apply_env_overrides({"database": {}}, "sr_ml")
        assert result["database"]["port"] == 9031
        assert result["database"]["password"] == "secret"

    def test_starrocks_env_ignored_for_other_profiles(self, monkeypatch):
        monkeypatch.setenv("STARROCKS_PORT", "9031")
        result = apply_env_overrides({"database": {"port": 5432}}, "postgres_ingest")
        assert result["database"]["port"] == 5432


# ── get_platform_fields ────────────────────────────────────────────────────

class TestGetPlatformFields:
    def test_returns_fields(self):
        cfg = {"fields": {"comments": ["id", "body", "author"]}}
        assert get_platform_fields(cfg, "comments") == ["id", "body", "author"]

    def test_missing_data_type_raises(self):
        with pytest.raises(ConfigurationError, match="submissions"):
            get_platform_fields({"fields": {}}, "submissions")

    def test_empty_fields_raises(self):
        with pytest.raises(ConfigurationError):
            get_platform_fields({"fields": {"comments": []}}, "comments")
