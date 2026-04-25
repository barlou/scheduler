"""Tests for framework/segment_resolver.py"""
import pytest
from pathlib import Path

from framework.config_loader import (
    AirflowJobConfig,
    ExecutionConfig,
    JobConfig,
    ServerConfig,
)
from framework.segment_resolver import (
    ExecutionSegment,
    _cron_sort_key,
    _has_force_terminate,
    _same_server,
    build_pipeline_segments,
    resolve_segments,
    sort_by_cron,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _server(force_terminate: bool = False, instance_type: str = "t3.large") -> ServerConfig:
    return ServerConfig(
        provider="aws",
        instance_type=instance_type,
        region="eu-west-1",
        ami_id="ami-test",
        subnet_id="subnet-test",
        security_group_id="sg-test",
        iam_instance_profile="test-profile",
        force_terminate=force_terminate,
    )


def _local(module: str, schedule: str = "0 1 * * *") -> AirflowJobConfig:
    return AirflowJobConfig(
        airflow_id="pipeline",
        dag_id="pipeline",
        schedule=schedule,
        execution=ExecutionConfig(mode="local"),
        job=JobConfig(module=module, entry_point="src/main.py", config_path="cfg.json"),
        source_path=Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


def _cloud(
    module: str,
    schedule: str = "0 2 * * *",
    force_terminate: bool = False,
    instance_type: str = "t3.large",
) -> AirflowJobConfig:
    return AirflowJobConfig(
        airflow_id="pipeline",
        dag_id="pipeline",
        schedule=schedule,
        execution=ExecutionConfig(mode="cloud", server=_server(force_terminate, instance_type)),
        job=JobConfig(module=module, entry_point="src/main.py", config_path="cfg.json"),
        source_path=Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# _same_server
# ─────────────────────────────────────────────────────────────────────────────

class TestSameServer:
    def test_both_none_returns_true(self):
        assert _same_server(None, None) is True

    def test_first_none_returns_false(self):
        assert _same_server(None, _server()) is False

    def test_second_none_returns_false(self):
        assert _same_server(_server(), None) is False

    def test_same_object_returns_true(self):
        s = _server()
        assert _same_server(s, s) is True

    def test_equal_but_different_objects_returns_false(self):
        s1 = _server()
        s2 = _server()
        assert s1 == s2          # same field values
        assert _same_server(s1, s2) is False  # but not the same object


# ─────────────────────────────────────────────────────────────────────────────
# _has_force_terminate
# ─────────────────────────────────────────────────────────────────────────────

class TestHasForceTerminate:
    def test_local_config_returns_false(self):
        assert _has_force_terminate(_local("m1")) is False

    def test_cloud_without_force_terminate_returns_false(self):
        assert _has_force_terminate(_cloud("m1", force_terminate=False)) is False

    def test_cloud_with_force_terminate_returns_true(self):
        assert _has_force_terminate(_cloud("m1", force_terminate=True)) is True


# ─────────────────────────────────────────────────────────────────────────────
# ExecutionSegment properties
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionSegmentProperties:
    def test_is_local_true_when_no_server(self):
        seg = ExecutionSegment(server=None, steps=[])
        assert seg.is_local is True
        assert seg.is_cloud is False
        assert seg.provider is None

    def test_is_cloud_true_when_server_set(self):
        s = _server()
        seg = ExecutionSegment(server=s, steps=[])
        assert seg.is_cloud is True
        assert seg.is_local is False
        assert seg.provider == "aws"

    def test_repr_shows_server_and_steps(self):
        cfg = _local("my_module")
        seg = ExecutionSegment(server=None, steps=[cfg])
        r = repr(seg)
        assert "local" in r
        assert "my_module" in r


# ─────────────────────────────────────────────────────────────────────────────
# _cron_sort_key and sort_by_cron
# ─────────────────────────────────────────────────────────────────────────────

class TestCronSorting:
    def test_sort_key_returns_float(self):
        result = _cron_sort_key(_local("m", "0 1 * * *"))
        assert isinstance(result, float)

    def test_earlier_schedule_has_lower_key(self):
        early = _local("early", "0 1 * * *")
        late = _local("late", "0 23 * * *")
        assert _cron_sort_key(early) < _cron_sort_key(late)

    def test_sort_by_cron_orders_ascending(self):
        configs = [
            _local("c", "0 3 * * *"),
            _local("a", "0 1 * * *"),
            _local("b", "0 2 * * *"),
        ]
        result = sort_by_cron(configs)
        assert [c.job.module for c in result] == ["a", "b", "c"]

    def test_sort_by_cron_stable_for_equal_schedules(self):
        configs = [_local("x", "0 1 * * *"), _local("y", "0 1 * * *")]
        result = sort_by_cron(configs)
        assert len(result) == 2

    def test_sort_by_cron_single_item(self):
        configs = [_local("only", "0 6 * * *")]
        assert sort_by_cron(configs) == configs


# ─────────────────────────────────────────────────────────────────────────────
# resolve_segments
# ─────────────────────────────────────────────────────────────────────────────

class TestResolveSegments:
    def test_empty_input_returns_empty_list(self):
        assert resolve_segments([]) == []

    def test_single_local_module(self):
        cfg = _local("m1")
        segs = resolve_segments([cfg])
        assert len(segs) == 1
        assert segs[0].is_local
        assert segs[0].steps == [cfg]
        assert segs[0].is_last is True
        assert segs[0].force_terminate is False

    def test_single_cloud_module(self):
        cfg = _cloud("m1")
        segs = resolve_segments([cfg])
        assert len(segs) == 1
        assert segs[0].is_cloud
        assert segs[0].steps == [cfg]
        assert segs[0].is_last is True

    def test_single_cloud_module_with_force_terminate(self):
        cfg = _cloud("m1", force_terminate=True)
        segs = resolve_segments([cfg])
        assert len(segs) == 1
        assert segs[0].force_terminate is True
        assert segs[0].is_last is True

    def test_all_local_forms_one_segment(self):
        configs = [
            _local("m1", "0 1 * * *"),
            _local("m2", "0 2 * * *"),
            _local("m3", "0 3 * * *"),
        ]
        segs = resolve_segments(configs)
        assert len(segs) == 1
        assert segs[0].is_local
        assert len(segs[0].steps) == 3

    def test_cloud_pulls_preceding_locals_onto_same_server(self):
        """Rule 1: backwards pull — local steps before a cloud step join it."""
        configs = [
            _local("step1", "0 1 * * *"),
            _cloud("step2", "0 2 * * *"),
            _local("step3", "0 3 * * *"),
        ]
        segs = resolve_segments(configs)
        assert len(segs) == 1
        assert segs[0].is_cloud
        modules = [s.job.module for s in segs[0].steps]
        assert modules == ["step1", "step2", "step3"]

    def test_last_segment_is_last_true(self):
        configs = [_local("m1", "0 1 * * *"), _local("m2", "0 2 * * *")]
        segs = resolve_segments(configs)
        assert segs[-1].is_last is True

    def test_two_cloud_modules_sharing_same_server_form_one_segment(self):
        """If two modules share the exact same ServerConfig object they form one segment."""
        shared_server = _server()
        cfg1 = AirflowJobConfig(
            airflow_id="p", dag_id="p", schedule="0 1 * * *",
            execution=ExecutionConfig(mode="cloud", server=shared_server),
            job=JobConfig(module="m1", entry_point="e", config_path="c"),
            source_path=Path("/fake"),
        )
        cfg2 = AirflowJobConfig(
            airflow_id="p", dag_id="p", schedule="0 2 * * *",
            execution=ExecutionConfig(mode="cloud", server=shared_server),
            job=JobConfig(module="m2", entry_point="e", config_path="c"),
            source_path=Path("/fake"),
        )
        segs = resolve_segments([cfg1, cfg2])
        # Both get the same server via Phase 1, so one segment
        assert len(segs) == 1

    def test_local_before_cloud_is_pulled_onto_cloud_server(self):
        """The backwards pull places pre-cloud locals on the cloud server."""
        local_pre = _local("pre", "0 1 * * *")
        cloud_step = _cloud("gpu", "0 2 * * *")
        segs = resolve_segments([local_pre, cloud_step])
        assert len(segs) == 1
        server = segs[0].server
        assert server is cloud_step.execution.server

    def test_non_last_segment_is_last_set_to_none_bug(self):
        """
        Known bug: resolve_segments sets is_last = None (not False) for
        non-final segments. This test documents the actual (buggy) behaviour.

        To trigger the bug we need two output segments. We achieve this via
        a multi-step first segment (avoids the steps[1] IndexError) followed
        by a second cloud declaration with a different server object.
        """
        local_a = _local("a", "0 1 * * *")
        local_b = _local("b", "0 2 * * *")
        cloud_c = _cloud("c", "0 3 * * *", force_terminate=True)   # closes first segment
        local_d = _local("d", "0 4 * * *")
        cloud_e = _cloud("e", "0 5 * * *", instance_type="g4dn.xlarge")  # new server object

        segs = resolve_segments([local_a, local_b, cloud_c, local_d, cloud_e])
        assert len(segs) == 2
        # Bug: is_last should be False here, but it's None
        assert segs[0].is_last is None  # actual behaviour — documents the bug
        assert segs[-1].is_last is True

    def test_force_terminate_on_wrong_step_bug(self):
        """
        Known bug: when closing a segment with 3+ steps, the code checks
        steps[1] instead of steps[-1] to set force_terminate on the segment.
        The first segment in the pipeline below has steps [a, b, c, d] where
        c is the cloud step with force_terminate=True, but steps[1]=b is local
        (force_terminate=False), so the segment's force_terminate is set to False.
        """
        local_a = _local("a", "0 1 * * *")
        local_b = _local("b", "0 2 * * *")
        cloud_c = _cloud("c", "0 3 * * *", force_terminate=True)
        local_d = _local("d", "0 4 * * *")
        cloud_e = _cloud("e", "0 5 * * *", instance_type="g4dn.xlarge")

        segs = resolve_segments([local_a, local_b, cloud_c, local_d, cloud_e])
        assert len(segs) == 2
        # Correct behaviour would be force_terminate=True on segment 0 (step c has it)
        # Bug: steps[1] (b, a local step) has no force_terminate, so segment gets False
        assert segs[0].force_terminate is False  # documents the bug

    @pytest.mark.xfail(
        raises=IndexError,
        strict=True,
        reason=(
            "Bug: resolve_segments uses steps[1] instead of steps[-1] when closing "
            "a segment. For a 1-step non-final segment this raises IndexError."
        ),
    )
    def test_one_step_non_final_segment_causes_index_error(self):
        """
        Pipeline: [cloud_a(force_terminate), cloud_b(different server)]
        Phase 1: cloud_a gets server_a, force_terminate → last_boundary=1.
                 cloud_b gets server_b (no backwards pull from boundary).
        Phase 2: segment A = [cloud_a] (1 step), then cloud_b has different server
                 → segment A is closed → steps[1] → IndexError.
        """
        server_a = _server(force_terminate=True, instance_type="t3.large")
        server_b = _server(force_terminate=False, instance_type="g4dn.xlarge")
        cfg_a = AirflowJobConfig(
            airflow_id="p", dag_id="p", schedule="0 1 * * *",
            execution=ExecutionConfig(mode="cloud", server=server_a),
            job=JobConfig(module="m1", entry_point="e", config_path="c"),
            source_path=Path("/fake"),
        )
        cfg_b = AirflowJobConfig(
            airflow_id="p", dag_id="p", schedule="0 2 * * *",
            execution=ExecutionConfig(mode="cloud", server=server_b),
            job=JobConfig(module="m2", entry_point="e", config_path="c"),
            source_path=Path("/fake"),
        )
        resolve_segments([cfg_a, cfg_b])


# ─────────────────────────────────────────────────────────────────────────────
# build_pipeline_segments (integration)
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildPipelineSegments:
    def test_empty_input_returns_empty(self):
        assert build_pipeline_segments([]) == []

    def test_all_local_single_segment(self):
        configs = [
            _local("m1", "0 3 * * *"),
            _local("m2", "0 1 * * *"),
            _local("m3", "0 2 * * *"),
        ]
        segs = build_pipeline_segments(configs)
        assert len(segs) == 1
        assert segs[0].is_local

    def test_sorts_by_cron_before_resolving(self):
        """build_pipeline_segments must sort before segment resolution."""
        configs = [
            _local("last", "0 3 * * *"),
            _local("first", "0 1 * * *"),
        ]
        segs = build_pipeline_segments(configs)
        assert segs[0].steps[0].job.module == "first"

    def test_cloud_step_pulls_earlier_locals(self):
        configs = [
            _local("ingest", "0 1 * * *"),
            _local("transform", "0 2 * * *"),
            _cloud("train", "0 3 * * *"),
        ]
        segs = build_pipeline_segments(configs)
        assert len(segs) == 1
        assert segs[0].is_cloud
        modules = [s.job.module for s in segs[0].steps]
        assert "ingest" in modules
        assert "transform" in modules
        assert "train" in modules

    def test_single_cloud_step_pipeline(self):
        segs = build_pipeline_segments([_cloud("train", "0 1 * * *")])
        assert len(segs) == 1
        assert segs[0].is_cloud
        assert segs[0].is_last is True
