# tests/test_pipeline_state.py
"""Tests for pipeline_state.py — PipelineConsumptionState and detect_shared_upstreams."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from framework.config_loader import (
    AirflowJobConfig, ExecutionConfig, JobConfig, PipelineConfig,
    RetryConfig, AlertConfig
)
from framework.pipeline_state import (
    PipelineConsumptionState,
    PipelineStateManager,
    detect_shared_upstreams,
    _state_remote_key,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_config(airflow_id: str, position: int, upstream_jobs: list[str] | None = None):
    from pathlib import Path
    return AirflowJobConfig(
        airflow_id=  airflow_id,
        dag_id=      airflow_id,
        schedule=    "0 1 * * *",
        execution=   ExecutionConfig(mode="local"),
        job=         JobConfig(module="mod", entry_point="src/main.py", config_path="cfg.json"),
        pipeline=    PipelineConfig(position=position, upstream_jobs=upstream_jobs or []),
        source_path= Path(f"/fake/{airflow_id}/airflow/airflow_job.yml"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# PipelineConsumptionState
# ─────────────────────────────────────────────────────────────────────────────

class TestPipelineConsumptionState:
    def test_defaults(self):
        s = PipelineConsumptionState(pipeline_id="p", upstream_job_id="u")
        assert s.last_consumed_file == ""
        assert s.last_consumed_at   is None
        assert s.files_pending      == []
        assert s.has_run            is False

    def test_state_key(self):
        s = PipelineConsumptionState(pipeline_id="pipeline_a", upstream_job_id="ingestor")
        assert s.state_key == "pipeline_a__ingestor"

    def test_has_run_false_when_empty(self):
        s = PipelineConsumptionState(pipeline_id="p", upstream_job_id="u")
        assert s.has_run is False

    def test_has_run_true_after_consume(self):
        s = PipelineConsumptionState(pipeline_id="p", upstream_job_id="u")
        s.mark_consumed("file_2026-05-03.parquet")
        assert s.has_run is True

    def test_mark_consumed_advances_cursor(self):
        s = PipelineConsumptionState(pipeline_id="p", upstream_job_id="u")
        s.files_pending = ["file_2026-05-03.parquet"]
        s.mark_consumed("file_2026-05-03.parquet")
        assert s.last_consumed_file == "file_2026-05-03.parquet"
        assert s.files_pending      == []
        assert s.last_consumed_at   is not None

    def test_mark_consumed_sets_utc_timestamp(self):
        s = PipelineConsumptionState(pipeline_id="p", upstream_job_id="u")
        s.mark_consumed("file_2026-05-03.parquet")
        assert s.last_consumed_at.tzinfo is not None

    def test_roundtrip_serialisation(self):
        ts = datetime(2026, 5, 3, 10, 0, 0, tzinfo=timezone.utc)
        s  = PipelineConsumptionState(
            pipeline_id=        "pipeline_a",
            upstream_job_id=    "ingestor",
            last_consumed_file= "transactions_2026-05-03.parquet",
            last_consumed_at=   ts,
            files_pending=      ["new_file_2026-05-04.parquet"],
        )
        d       = s.to_dict()
        s2      = PipelineConsumptionState.from_dict(d)
        assert s2.pipeline_id         == s.pipeline_id
        assert s2.upstream_job_id     == s.upstream_job_id
        assert s2.last_consumed_file  == s.last_consumed_file
        assert s2.last_consumed_at    == s.last_consumed_at
        assert s2.files_pending       == s.files_pending

    def test_from_dict_handles_none_timestamp(self):
        d = {
            "pipeline_id":       "p",
            "upstream_job_id":   "u",
            "last_consumed_file": "",
            "last_consumed_at":   None,
            "files_pending":      [],
        }
        s = PipelineConsumptionState.from_dict(d)
        assert s.last_consumed_at is None


# ─────────────────────────────────────────────────────────────────────────────
# State remote key
# ─────────────────────────────────────────────────────────────────────────────

class TestStateRemoteKey:
    def test_pattern(self):
        key = _state_remote_key("pipeline_a", "ingestor")
        assert key == "framework/state/pipeline_a__ingestor.json"


# ─────────────────────────────────────────────────────────────────────────────
# detect_shared_upstreams
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectSharedUpstreams:
    def test_no_upstreams(self):
        configs = [_make_config("pipeline_a", 1)]
        result  = detect_shared_upstreams(configs)
        assert result == {}

    def test_single_consumer(self):
        configs = [_make_config("pipeline_a", 2, upstream_jobs=["ingestor"])]
        result  = detect_shared_upstreams(configs)
        assert result == {}  # only one consumer — not shared

    def test_shared_upstream_detected(self):
        configs = [
            _make_config("pipeline_a", 2, upstream_jobs=["ingestor"]),
            _make_config("pipeline_b", 2, upstream_jobs=["ingestor"]),
        ]
        result = detect_shared_upstreams(configs)
        assert "ingestor" in result
        assert sorted(result["ingestor"]) == ["pipeline_a", "pipeline_b"]

    def test_no_duplicate_pipeline_ids(self):
        # Same pipeline declaring the upstream twice should not duplicate
        configs = [
            _make_config("pipeline_a", 2, upstream_jobs=["ingestor"]),
            _make_config("pipeline_a", 3, upstream_jobs=["ingestor"]),
        ]
        result = detect_shared_upstreams(configs)
        assert result == {}  # still only one pipeline — not shared

    def test_multiple_shared_upstreams(self):
        configs = [
            _make_config("pipeline_a", 2, upstream_jobs=["up1", "up2"]),
            _make_config("pipeline_b", 2, upstream_jobs=["up1"]),
            _make_config("pipeline_c", 2, upstream_jobs=["up2"]),
        ]
        result = detect_shared_upstreams(configs)
        assert "up1" in result
        assert "up2" in result
        assert sorted(result["up1"]) == ["pipeline_a", "pipeline_b"]
        assert sorted(result["up2"]) == ["pipeline_a", "pipeline_c"]
