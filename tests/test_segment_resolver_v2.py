# tests/test_segment_resolver_v2.py
"""Tests for segment_resolver.py — position-based ordering and singleton validation."""
from __future__ import annotations

from pathlib import Path

import pytest

from framework.config_loader import (
    AirflowJobConfig, ExecutionConfig, JobConfig, PipelineConfig,
    RetryConfig, AlertConfig, ServerConfig
)
from framework.segment_resolver import (
    sort_by_position,
    validate_singleton,
    build_pipeline_segments,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_local(module: str, position: int, airflow_id: str = "test_pipeline") -> AirflowJobConfig:
    return AirflowJobConfig(
        airflow_id=  airflow_id,
        dag_id=      airflow_id,
        schedule=    "0 1 * * *",
        execution=   ExecutionConfig(mode="local"),
        job=         JobConfig(module=module, entry_point="src/main.py", config_path="cfg.json"),
        pipeline=    PipelineConfig(position=position),
        source_path= Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


def _make_cloud(
    module: str,
    position: int,
    instance_type: str = "t3.large",
    force_terminate: bool = False,
    airflow_id: str = "test_pipeline",
) -> AirflowJobConfig:
    server = ServerConfig(
        provider="aws", instance_type=instance_type, region="eu-west-1",
        ami_id="ami-test", subnet_id="subnet-test",
        security_group_id="sg-test", iam_instance_profile="test-profile",
        force_terminate=force_terminate,
    )
    return AirflowJobConfig(
        airflow_id=  airflow_id,
        dag_id=      airflow_id,
        schedule=    "0 1 * * *",
        execution=   ExecutionConfig(mode="cloud", server=server),
        job=         JobConfig(module=module, entry_point="src/main.py", config_path="cfg.json"),
        pipeline=    PipelineConfig(position=position),
        source_path= Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# sort_by_position
# ─────────────────────────────────────────────────────────────────────────────

class TestSortByPosition:
    def test_sorts_ascending(self):
        configs = [_make_local("c", 3), _make_local("a", 1), _make_local("b", 2)]
        sorted_ = sort_by_position(configs)
        assert [c.job.module for c in sorted_] == ["a", "b", "c"]

    def test_duplicate_positions_raise(self):
        configs = [_make_local("a", 1), _make_local("b", 1)]
        with pytest.raises(ValueError, match="Duplicate pipeline positions"):
            sort_by_position(configs)

    def test_single_item(self):
        configs = [_make_local("a", 5)]
        sorted_ = sort_by_position(configs)
        assert sorted_[0].pipeline.position == 5


# ─────────────────────────────────────────────────────────────────────────────
# validate_singleton
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateSingleton:
    def test_no_singleton_passes(self):
        configs = [_make_local("a", 1), _make_local("b", 2)]
        validate_singleton(configs)  # should not raise

    def test_singleton_alone_passes(self):
        cfg = _make_local("a", 1)
        cfg.pipeline.is_singleton = True
        validate_singleton([cfg])  # should not raise

    def test_singleton_with_other_raises(self):
        cfg1 = _make_local("a", 1)
        cfg1.pipeline.is_singleton = True
        cfg2 = _make_local("b", 2)
        with pytest.raises(ValueError, match="singleton"):
            validate_singleton([cfg1, cfg2])


# ─────────────────────────────────────────────────────────────────────────────
# build_pipeline_segments — position-based ordering
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildPipelineSegmentsPositionOrdering:
    def test_all_local_single_segment(self):
        configs  = [_make_local("a", 1), _make_local("b", 2), _make_local("c", 3)]
        segments = build_pipeline_segments(configs)
        assert len(segments) == 1
        assert [s.job.module for s in segments[0].steps] == ["a", "b", "c"]

    def test_out_of_order_input_sorted_correctly(self):
        # Input is deliberately out of order — framework must sort by position
        configs  = [_make_local("c", 3), _make_local("a", 1), _make_local("b", 2)]
        segments = build_pipeline_segments(configs)
        assert [s.job.module for s in segments[0].steps] == ["a", "b", "c"]

    def test_cloud_step_pulls_preceding_local(self):
        configs  = [_make_local("a", 1), _make_cloud("b", 2), _make_local("c", 3)]
        segments = build_pipeline_segments(configs)
        # All steps should be in one cloud segment
        assert len(segments) == 1
        assert segments[0].is_cloud
        assert [s.job.module for s in segments[0].steps] == ["a", "b", "c"]

    def test_empty_returns_empty(self):
        assert build_pipeline_segments([]) == []

    def test_last_segment_marked(self):
        configs  = [_make_local("a", 1), _make_local("b", 2)]
        segments = build_pipeline_segments(configs)
        assert segments[-1].is_last is True
        for seg in segments[:-1]:
            assert seg.is_last is False
