# tests/test_config_loader_v2.py
"""
Tests for config_loader.py — new PipelineConfig fields,
build_output_path, and extract_file_date.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from framework.config_loader import (
    AirflowJobConfig,
    PipelineConfig,
    build_output_path,
    extract_file_date,
    load_job_config,
)


# ─────────────────────────────────────────────────────────────────────────────
# build_output_path
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildOutputPath:
    def test_returns_dated_filename(self):
        result = build_output_path("transactions", ".parquet")
        assert re.match(r"^transactions_\d{4}-\d{2}-\d{2}\.parquet$", result)

    def test_date_is_utc_today(self):
        result   = build_output_path("snapshot", ".csv")
        today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert today in result

    def test_preserves_base_name(self):
        result = build_output_path("daily_report", ".json")
        assert result.startswith("daily_report_")

    def test_preserves_extension(self):
        for ext in [".parquet", ".csv", ".json", ".orc"]:
            assert build_output_path("data", ext).endswith(ext)

    def test_format_is_consistent(self):
        r1 = build_output_path("x", ".parquet")
        r2 = build_output_path("x", ".parquet")
        # Same day → identical (unless test crosses midnight)
        assert r1 == r2


# ─────────────────────────────────────────────────────────────────────────────
# extract_file_date
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractFileDate:
    def test_parses_valid_date(self):
        result = extract_file_date("transactions_2026-05-03.parquet")
        assert result == datetime(2026, 5, 3, tzinfo=timezone.utc)

    def test_returns_none_for_no_date(self):
        assert extract_file_date("no_date_here.parquet") is None

    def test_returns_none_for_invalid_date(self):
        assert extract_file_date("data_9999-99-99.parquet") is None

    def test_works_on_full_path(self):
        result = extract_file_date("s3://bucket/data/transactions_2026-01-15.parquet")
        assert result == datetime(2026, 1, 15, tzinfo=timezone.utc)

    def test_result_is_utc(self):
        result = extract_file_date("file_2026-05-03.csv")
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_picks_first_date_in_name(self):
        # When multiple dates appear, the first one wins
        result = extract_file_date("2026-01-01_backup_2026-05-03.parquet")
        assert result == datetime(2026, 1, 1, tzinfo=timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# PipelineConfig defaults
# ─────────────────────────────────────────────────────────────────────────────

class TestPipelineConfig:
    def test_defaults(self):
        pc = PipelineConfig(position=1)
        assert pc.upstream_jobs       == []
        assert pc.is_singleton        is False
        assert pc.chunk_threshold     == 10
        assert pc.max_parallel_chunks == 4

    def test_custom_values(self):
        pc = PipelineConfig(
            position=3,
            upstream_jobs=["upstream_a"],
            is_singleton=True,
            chunk_threshold=20,
            max_parallel_chunks=8,
        )
        assert pc.position            == 3
        assert pc.upstream_jobs       == ["upstream_a"]
        assert pc.is_singleton        is True
        assert pc.chunk_threshold     == 20
        assert pc.max_parallel_chunks == 8


# ─────────────────────────────────────────────────────────────────────────────
# load_job_config — pipeline block validation
# ─────────────────────────────────────────────────────────────────────────────

def _write_yaml(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "airflow_job.yml"
    p.write_text(yaml.dump(content), encoding="utf-8")
    return p


_BASE_CONFIG = {
    "airflow_id": "test_pipeline",
    "dag_id":     "test_pipeline",
    "schedule":   "0 1 * * *",
    "execution":  {"mode": "local"},
    "job":        {"module": "mod", "entry_point": "src/main.py", "config_path": "cfg.json"},
    "pipeline":   {"position": 1},
}


class TestLoadJobConfigPipeline:
    def test_valid_config_loads(self, tmp_path):
        p   = _write_yaml(tmp_path, _BASE_CONFIG)
        cfg = load_job_config(p)
        assert cfg.pipeline.position == 1
        assert cfg.pipeline.upstream_jobs == []

    def test_missing_position_raises(self, tmp_path):
        bad = {**_BASE_CONFIG, "pipeline": {}}
        p   = _write_yaml(tmp_path, bad)
        with pytest.raises(ValueError, match="pipeline.position is required"):
            load_job_config(p)

    def test_position_zero_raises(self, tmp_path):
        bad = {**_BASE_CONFIG, "pipeline": {"position": 0}}
        p   = _write_yaml(tmp_path, bad)
        with pytest.raises(ValueError, match="pipeline.position must be a positive integer"):
            load_job_config(p)

    def test_negative_position_raises(self, tmp_path):
        bad = {**_BASE_CONFIG, "pipeline": {"position": -1}}
        p   = _write_yaml(tmp_path, bad)
        with pytest.raises(ValueError, match="pipeline.position must be a positive integer"):
            load_job_config(p)

    def test_chunk_threshold_defaults_to_10(self, tmp_path):
        p   = _write_yaml(tmp_path, _BASE_CONFIG)
        cfg = load_job_config(p)
        assert cfg.pipeline.chunk_threshold == 10

    def test_chunk_threshold_custom(self, tmp_path):
        custom = {**_BASE_CONFIG, "pipeline": {"position": 1, "chunk_threshold": 25}}
        p      = _write_yaml(tmp_path, custom)
        cfg    = load_job_config(p)
        assert cfg.pipeline.chunk_threshold == 25

    def test_invalid_chunk_threshold_raises(self, tmp_path):
        bad = {**_BASE_CONFIG, "pipeline": {"position": 1, "chunk_threshold": 0}}
        p   = _write_yaml(tmp_path, bad)
        with pytest.raises(ValueError, match="pipeline.chunk_threshold"):
            load_job_config(p)

    def test_upstream_jobs_loaded(self, tmp_path):
        cfg_dict = {
            **_BASE_CONFIG,
            "pipeline": {"position": 2, "upstream_jobs": ["ingestor", "cleaner"]},
        }
        p   = _write_yaml(tmp_path, cfg_dict)
        cfg = load_job_config(p)
        assert cfg.pipeline.upstream_jobs == ["ingestor", "cleaner"]

    def test_is_singleton_default_false(self, tmp_path):
        p   = _write_yaml(tmp_path, _BASE_CONFIG)
        cfg = load_job_config(p)
        assert cfg.pipeline.is_singleton is False

    def test_is_singleton_true(self, tmp_path):
        cfg_dict = {**_BASE_CONFIG, "pipeline": {"position": 1, "is_singleton": True}}
        p        = _write_yaml(tmp_path, cfg_dict)
        cfg      = load_job_config(p)
        assert cfg.pipeline.is_singleton is True
