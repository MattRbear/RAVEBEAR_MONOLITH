"""Tests for configuration loading and validation."""

from pathlib import Path

import pytest

from ravebear_monolith.foundation.config import AppConfig, load_config
from ravebear_monolith.util.errors import ConfigError


class TestLoadConfigMissingFile:
    """Tests for missing configuration files."""

    def test_missing_file_raises_config_error(self, tmp_path: Path) -> None:
        """load_config raises ConfigError when file does not exist."""
        nonexistent = tmp_path / "does_not_exist.yaml"
        with pytest.raises(ConfigError, match="not found"):
            load_config(nonexistent)


class TestLoadConfigInvalidYaml:
    """Tests for invalid YAML content."""

    def test_invalid_yaml_syntax_raises_config_error(self, tmp_path: Path) -> None:
        """load_config raises ConfigError for malformed YAML."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("{ unclosed: [bracket", encoding="utf-8")
        with pytest.raises(ConfigError, match="Invalid YAML"):
            load_config(bad_yaml)

    def test_invalid_type_raises_config_error(self, tmp_path: Path) -> None:
        """load_config raises ConfigError when value types are wrong."""
        bad_types = tmp_path / "bad_types.yaml"
        bad_types.write_text("shutdown_timeout_s: not_an_int", encoding="utf-8")
        with pytest.raises(ConfigError, match="validation failed"):
            load_config(bad_types)

    def test_invalid_environment_raises_config_error(self, tmp_path: Path) -> None:
        """load_config raises ConfigError when environment is not dev/prod."""
        bad_env = tmp_path / "bad_env.yaml"
        bad_env.write_text("environment: staging", encoding="utf-8")
        with pytest.raises(ConfigError, match="validation failed"):
            load_config(bad_env)

    def test_non_dict_raises_config_error(self, tmp_path: Path) -> None:
        """load_config raises ConfigError when YAML is not a mapping."""
        list_yaml = tmp_path / "list.yaml"
        list_yaml.write_text("- item1\n- item2", encoding="utf-8")
        with pytest.raises(ConfigError, match="must be a mapping"):
            load_config(list_yaml)


class TestLoadConfigValid:
    """Tests for valid configuration loading."""

    def test_empty_yaml_uses_defaults(self, tmp_path: Path) -> None:
        """load_config uses defaults when YAML is empty."""
        empty = tmp_path / "empty.yaml"
        empty.write_text("", encoding="utf-8")
        config = load_config(empty)
        assert config.app_name == "RAVEBEAR_MONOLITH"
        assert config.environment == "dev"
        assert config.log_level == "INFO"
        assert config.shutdown_timeout_s == 5
        assert config.heartbeat_interval_s == 5

    def test_full_config_loads_correctly(self, tmp_path: Path) -> None:
        """load_config correctly parses all fields."""
        full = tmp_path / "full.yaml"
        full.write_text(
            """
app_name: TestApp
environment: prod
log_level: DEBUG
shutdown_timeout_s: 10
heartbeat_interval_s: 2
""",
            encoding="utf-8",
        )
        config = load_config(full)
        assert config.app_name == "TestApp"
        assert config.environment == "prod"
        assert config.log_level == "DEBUG"
        assert config.shutdown_timeout_s == 10
        assert config.heartbeat_interval_s == 2

    def test_partial_config_merges_with_defaults(self, tmp_path: Path) -> None:
        """load_config uses defaults for missing fields."""
        partial = tmp_path / "partial.yaml"
        partial.write_text("environment: prod", encoding="utf-8")
        config = load_config(partial)
        assert config.environment == "prod"
        assert config.app_name == "RAVEBEAR_MONOLITH"  # default


class TestAppConfigModel:
    """Tests for AppConfig model validation."""

    def test_strict_mode_rejects_extra_fields(self) -> None:
        """AppConfig in strict mode rejects unexpected fields."""
        with pytest.raises(Exception):  # ValidationError
            AppConfig(unknown_field="value")  # type: ignore

    def test_positive_int_rejects_zero(self) -> None:
        """PositiveInt fields reject zero."""
        with pytest.raises(Exception):
            AppConfig(shutdown_timeout_s=0)

    def test_positive_int_rejects_negative(self) -> None:
        """PositiveInt fields reject negative values."""
        with pytest.raises(Exception):
            AppConfig(heartbeat_interval_s=-1)


class TestPathCoercion:
    """Tests for Path coercion from YAML strings."""

    def test_data_dir_string_coerced_to_path(self, tmp_path: Path) -> None:
        """String data_dir from YAML is coerced to Path."""
        yaml_file = tmp_path / "paths.yaml"
        yaml_file.write_text(
            "data_dir: data\nkill_switch_path: config/kill_switch.txt\n", encoding="utf-8"
        )
        config = load_config(yaml_file)

        assert isinstance(config.data_dir, Path)
        assert isinstance(config.kill_switch_path, Path)
        assert str(config.data_dir).endswith("data")
        assert str(config.kill_switch_path).endswith("kill_switch.txt")

    def test_storage_db_path_string_coerced_to_path(self, tmp_path: Path) -> None:
        """String db_path in storage from YAML is coerced to Path."""
        yaml_file = tmp_path / "storage.yaml"
        yaml_file.write_text("storage:\n  db_path: data/my_events.db\n", encoding="utf-8")
        config = load_config(yaml_file)

        assert isinstance(config.storage.db_path, Path)
        assert str(config.storage.db_path).endswith("my_events.db")

    def test_path_objects_still_accepted(self) -> None:
        """Path objects are accepted directly."""
        config = AppConfig(
            data_dir=Path("custom/data"),
            kill_switch_path=Path("custom/kill.txt"),
        )
        assert config.data_dir == Path("custom/data")
        assert config.kill_switch_path == Path("custom/kill.txt")
