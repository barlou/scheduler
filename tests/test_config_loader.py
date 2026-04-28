"""Tests for framework/config_loader.py"""
import pytest
from pathlib import Path

from framework.config_loader import (
    _resolve_placeholders,
    _validate,
    load_job_config,
    scan_job_configs,
    AirflowJobConfig,
    ExecutionConfig,
    JobConfig,
    ServerConfig,
    RetryConfig,
    AlertConfig,
)

# ─────────────────────────────────────────────────────────────────────────────
# YAML fixtures
# ─────────────────────────────────────────────────────────────────────────────

VALID_LOCAL_YAML = """\
airflow_id: my_pipeline
dag_id: my_dag
schedule: "0 1 * * *"
description: Test pipeline
timezone: Europe/Paris
enabled: true
execution:
  mode: local
job:
  module: my_module
  entry_point: src/main.py
  config_path: config/config.json
retry:
  attempts: 3
  delay_minutes: 10
alerts:
  on_failure: true
  on_retry: false
  on_success: true
"""

VALID_CLOUD_YAML = """\
airflow_id: cloud_pipeline
dag_id: cloud_dag
schedule: "0 2 * * *"
execution:
  mode: cloud
  server:
    provider: aws
    instance_type: t3.large
    region: eu-west-1
    ami_id: ami-12345
    subnet_id: subnet-12345
    security_group_id: sg-12345
    iam_instance_profile: my-profile
    startup_timeout_minutes: 15
    force_terminate: true
job:
  module: cloud_module
  entry_point: src/main.py
  config_path: config/config.json
"""

MINIMAL_YAML = """\
airflow_id: minimal
dag_id: minimal_dag
schedule: "0 1 * * *"
execution:
  mode: local
job:
  module: m
  entry_point: e
  config_path: c
"""

_SRC = Path("/fake/airflow_job.yml")


# ─────────────────────────────────────────────────────────────────────────────
# _resolve_placeholders
# ─────────────────────────────────────────────────────────────────────────────

class TestResolvePlaceholders:
    def test_replaces_single_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert _resolve_placeholders("value: {{ MY_VAR }}") == "value: hello"

    def test_replaces_multiple_env_vars(self, monkeypatch):
        monkeypatch.setenv("VAR_A", "alpha")
        monkeypatch.setenv("VAR_B", "beta")
        result = _resolve_placeholders("a={{ VAR_A }} b={{ VAR_B }}")
        assert "alpha" in result
        assert "beta" in result

    def test_no_placeholders_returns_unchanged(self):
        raw = "no placeholders here: just plain text"
        assert _resolve_placeholders(raw) == raw

    def test_raises_on_missing_env_var(self):
        with pytest.raises(ValueError, match="MISSING_VAR"):
            _resolve_placeholders("value: {{ MISSING_VAR }}")

    def test_reports_all_missing_keys_in_one_error(self):
        with pytest.raises(ValueError) as exc:
            _resolve_placeholders("{{ MISSING_A }} and {{ MISSING_B }}")
        msg = str(exc.value)
        assert "MISSING_A" in msg
        assert "MISSING_B" in msg

    def test_empty_string_returns_empty(self):
        assert _resolve_placeholders("") == ""

    def test_env_var_value_is_injected_inline(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "prod.db.internal")
        result = _resolve_placeholders("host: {{ DB_HOST }}")
        assert "prod.db.internal" in result
        assert "{{" not in result


# ─────────────────────────────────────────────────────────────────────────────
# _validate
# ─────────────────────────────────────────────────────────────────────────────

class TestValidate:
    def _local(self, **overrides) -> dict:
        base = {
            "airflow_id": "test",
            "dag_id": "test_dag",
            "schedule": "0 1 * * *",
            "execution": {"mode": "local"},
            "job": {"module": "m", "entry_point": "e", "config_path": "c"},
        }
        base.update(overrides)
        return base

    def _cloud(self) -> dict:
        return {
            "airflow_id": "test",
            "dag_id": "test_dag",
            "schedule": "0 1 * * *",
            "execution": {
                "mode": "cloud",
                "server": {
                    "provider": "aws",
                    "instance_type": "t3.large",
                    "region": "eu-west-1",
                    "ami_id": "ami-x",
                    "subned_id": "subnet-x",  # uses _REQUIRED_SERVER typo
                    "security_group_id": "sg-x",
                    "iam_instance_profile": "profile",
                },
            },
            "job": {"module": "m", "entry_point": "e", "config_path": "c"},
        }

    def test_valid_local_config_passes(self):
        _validate(self._local(), _SRC)

    def test_valid_cloud_config_passes(self):
        _validate(self._cloud(), _SRC)

    def test_raises_on_empty_config(self):
        with pytest.raises(ValueError, match="missing required field"):
            _validate({}, _SRC)

    def test_raises_on_missing_airflow_id(self):
        cfg = self._local()
        del cfg["airflow_id"]
        with pytest.raises(ValueError, match="airflow_id"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_dag_id(self):
        cfg = self._local()
        del cfg["dag_id"]
        with pytest.raises(ValueError, match="dag_id"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_schedule(self):
        cfg = self._local()
        del cfg["schedule"]
        with pytest.raises(ValueError, match="schedule"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_execution(self):
        cfg = self._local()
        del cfg["execution"]
        with pytest.raises(ValueError, match="execution"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_job(self):
        cfg = self._local()
        del cfg["job"]
        with pytest.raises(ValueError, match="job"):
            _validate(cfg, _SRC)

    def test_raises_on_invalid_mode(self):
        cfg = self._local()
        cfg["execution"]["mode"] = "kubernetes"
        with pytest.raises(ValueError, match="execution.mode"):
            _validate(cfg, _SRC)

    def test_raises_on_cloud_without_server_block(self):
        cfg = self._local()
        cfg["execution"]["mode"] = "cloud"
        with pytest.raises(ValueError, match="no 'server' block"):
            _validate(cfg, _SRC)

    def test_raises_on_invalid_provider(self):
        cfg = self._cloud()
        cfg["execution"]["server"]["provider"] = "digital_ocean"
        with pytest.raises(ValueError, match="provider"):
            _validate(cfg, _SRC)

    def test_all_valid_providers_accepted(self):
        for provider in ("aws", "gcp", "azure", "ovh"):
            cfg = self._cloud()
            cfg["execution"]["server"]["provider"] = provider
            _validate(cfg, _SRC)

    def test_raises_on_cron_too_few_fields(self):
        cfg = self._local(schedule="0 1 *")
        with pytest.raises(ValueError, match="cron"):
            _validate(cfg, _SRC)

    def test_raises_on_cron_too_many_fields(self):
        cfg = self._local(schedule="0 0 1 * * *")
        with pytest.raises(ValueError, match="cron"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_job_module(self):
        cfg = self._local()
        del cfg["job"]["module"]
        with pytest.raises(ValueError, match="job.module"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_job_entry_point(self):
        cfg = self._local()
        del cfg["job"]["entry_point"]
        with pytest.raises(ValueError, match="job.entry_point"):
            _validate(cfg, _SRC)

    def test_raises_on_missing_job_config_path(self):
        cfg = self._local()
        del cfg["job"]["config_path"]
        with pytest.raises(ValueError, match="job.config_path"):
            _validate(cfg, _SRC)

    def test_multiple_errors_reported_together(self):
        """Validation collects all errors before raising."""
        cfg = self._local()
        del cfg["job"]["module"]
        del cfg["job"]["entry_point"]
        with pytest.raises(ValueError) as exc:
            _validate(cfg, _SRC)
        msg = str(exc.value)
        assert "job.module" in msg
        assert "job.entry_point" in msg

    def test_subnet_id_typo_means_missing_subnet_is_not_caught(self):
        """
        Known bug: _REQUIRED_SERVER contains 'subned_id' (typo for 'subnet_id').
        A cloud config without 'subnet_id' passes server validation because
        the validator checks for the typo'd key, never 'subnet_id'.
        """
        cfg = self._cloud()
        del cfg["execution"]["server"]["subned_id"]  # remove the typo'd field
        # This should ideally fail, but passes due to the typo bug
        _validate(cfg, _SRC)  # no error raised — documents the bug


# ─────────────────────────────────────────────────────────────────────────────
# load_job_config
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadJobConfig:
    def test_loads_valid_local_config(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text(VALID_LOCAL_YAML)
        cfg = load_job_config(f)

        assert cfg.airflow_id == "my_pipeline"
        assert cfg.dag_id == "my_dag"
        assert cfg.schedule == "0 1 * * *"
        assert cfg.description == "Test pipeline"
        assert cfg.timezone == "Europe/Paris"
        assert cfg.enabled is True
        assert cfg.execution.mode == "local"
        assert cfg.execution.server is None
        assert cfg.job.module == "my_module"
        assert cfg.job.entry_point == "src/main.py"
        assert cfg.job.config_path == "config/config.json"
        assert cfg.retry.attempts == 3
        assert cfg.retry.delay_minutes == 10
        assert cfg.alerts.on_failure is True
        assert cfg.alerts.on_retry is False
        assert cfg.alerts.on_success is True

    def test_loads_valid_cloud_config(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text(VALID_CLOUD_YAML)
        cfg = load_job_config(f)

        assert cfg.execution.mode == "cloud"
        assert cfg.execution.server is not None
        s = cfg.execution.server
        assert s.provider == "aws"
        assert s.instance_type == "t3.large"
        assert s.region == "eu-west-1"
        assert s.ami_id == "ami-12345"
        assert s.subnet_id == "subnet-12345"
        assert s.security_group_id == "sg-12345"
        assert s.iam_instance_profile == "my-profile"
        assert s.startup_timeout_minutes == 15
        assert s.force_terminate is True

    def test_applies_default_values_when_optional_fields_absent(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text(MINIMAL_YAML)
        cfg = load_job_config(f)

        assert cfg.timezone == "Europe/Paris"
        assert cfg.enabled is True
        assert cfg.description == ""
        assert cfg.retry.attempts == 1
        assert cfg.retry.delay_minutes == 5
        assert cfg.alerts.on_failure is True
        assert cfg.alerts.on_retry is False
        assert cfg.alerts.on_success is False

    def test_raises_for_empty_file(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text("")
        with pytest.raises(ValueError, match="empty or not valid YAML"):
            load_job_config(f)

    def test_raises_for_list_yaml(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text("- item_one\n- item_two\n")
        with pytest.raises(ValueError, match="empty or not valid YAML"):
            load_job_config(f)

    def test_raises_for_malformed_yaml(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text("key: [unclosed bracket")
        with pytest.raises(Exception):
            load_job_config(f)

    def test_resolves_env_placeholder_before_parse(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AMI_ID_ENV", "ami-from-env")
        yaml_content = VALID_CLOUD_YAML.replace("ami-12345", "{{ AMI_ID_ENV }}")
        f = tmp_path / "airflow_job.yml"
        f.write_text(yaml_content)
        cfg = load_job_config(f)
        assert cfg.execution.server.ami_id == "ami-from-env"

    def test_raises_for_undefined_placeholder(self, tmp_path):
        yaml_content = VALID_CLOUD_YAML.replace("ami-12345", "{{ UNDEFINED_VAR }}")
        f = tmp_path / "airflow_job.yml"
        f.write_text(yaml_content)
        with pytest.raises(ValueError, match="UNDEFINED_VAR"):
            load_job_config(f)

    def test_sets_source_path_from_argument(self, tmp_path):
        f = tmp_path / "airflow_job.yml"
        f.write_text(VALID_LOCAL_YAML)
        cfg = load_job_config(f)
        assert cfg.source_path == f

    def test_file_not_found_raises(self, tmp_path):
        """
        The `if not config_path.exists:` guard has a bug (missing parentheses),
        so it never fires. However read_text() on a missing file still raises
        FileNotFoundError, so the overall behaviour is correct by accident.
        """
        missing = tmp_path / "nonexistent.yml"
        with pytest.raises(FileNotFoundError):
            load_job_config(missing)

    def test_server_config_defaults_applied(self, tmp_path):
        """startup_timeout_minutes and force_terminate default correctly."""
        yaml_content = VALID_CLOUD_YAML.replace(
            "    startup_timeout_minutes: 15\n    force_terminate: true\n", ""
        )
        f = tmp_path / "airflow_job.yml"
        f.write_text(yaml_content)
        cfg = load_job_config(f)
        assert cfg.execution.server.startup_timeout_minutes == 10
        assert cfg.execution.server.force_terminate is False


# ─────────────────────────────────────────────────────────────────────────────
# scan_job_configs
# ─────────────────────────────────────────────────────────────────────────────

class TestScanJobConfigs:
    def _write(self, base: Path, module: str, yaml: str):
        d = base / module / "airflow"
        d.mkdir(parents=True, exist_ok=True)
        (d / "airflow_job.yml").write_text(yaml)

    def test_returns_enabled_configs(self, tmp_path):
        self._write(tmp_path, "mod_a", VALID_LOCAL_YAML)
        result = scan_job_configs(tmp_path)
        assert len(result) == 1
        assert result[0].job.module == "my_module"

    def test_skips_disabled_configs(self, tmp_path):
        disabled = VALID_LOCAL_YAML.replace("enabled: true", "enabled: false")
        self._write(tmp_path, "mod_a", disabled)
        result = scan_job_configs(tmp_path)
        assert result == []

    def test_returns_all_enabled_when_multiple_present(self, tmp_path):
        yaml_b = (
            VALID_LOCAL_YAML
            .replace("airflow_id: my_pipeline", "airflow_id: other")
            .replace("dag_id: my_dag", "dag_id: other_dag")
        )
        self._write(tmp_path, "mod_a", VALID_LOCAL_YAML)
        self._write(tmp_path, "mod_b", yaml_b)
        result = scan_job_configs(tmp_path)
        assert len(result) == 2

    def test_filters_by_airflow_id(self, tmp_path):
        yaml_a = VALID_LOCAL_YAML.replace("airflow_id: my_pipeline", "airflow_id: pipeline_a")
        yaml_b = (
            VALID_LOCAL_YAML
            .replace("airflow_id: my_pipeline", "airflow_id: pipeline_b")
            .replace("dag_id: my_dag", "dag_id: dag_b")
        )
        self._write(tmp_path, "mod_a", yaml_a)
        self._write(tmp_path, "mod_b", yaml_b)
        result = scan_job_configs(tmp_path, airflow_id="pipeline_a")
        assert len(result) == 1
        assert result[0].airflow_id == "pipeline_a"

    def test_airflow_id_filter_returns_empty_when_no_match(self, tmp_path):
        self._write(tmp_path, "mod_a", VALID_LOCAL_YAML)
        result = scan_job_configs(tmp_path, airflow_id="nonexistent_pipeline")
        assert result == []

    def test_returns_empty_for_empty_directory(self, tmp_path):
        assert scan_job_configs(tmp_path) == []

    def test_handles_invalid_yaml_gracefully(self, tmp_path, capsys):
        d = tmp_path / "bad" / "airflow"
        d.mkdir(parents=True)
        (d / "airflow_job.yml").write_text("{ bad yaml: ][")
        result = scan_job_configs(tmp_path)
        assert result == []
        assert "[ERROR]" in capsys.readouterr().out

    def test_handles_missing_required_fields_gracefully(self, tmp_path, capsys):
        d = tmp_path / "incomplete" / "airflow"
        d.mkdir(parents=True)
        (d / "airflow_job.yml").write_text("some_key: some_value\n")
        result = scan_job_configs(tmp_path)
        assert result == []
        assert "[ERROR]" in capsys.readouterr().out

    def test_ok_message_printed_for_loaded_config(self, tmp_path, capsys):
        self._write(tmp_path, "mod_a", VALID_LOCAL_YAML)
        scan_job_configs(tmp_path)
        assert "[OK]" in capsys.readouterr().out

    def test_skip_message_printed_for_disabled_config(self, tmp_path, capsys):
        disabled = VALID_LOCAL_YAML.replace("enabled: true", "enabled: false")
        self._write(tmp_path, "mod_a", disabled)
        scan_job_configs(tmp_path)
        assert "[SKIP]" in capsys.readouterr().out
