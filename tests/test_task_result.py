"""Tests for TaskResult dataclass in framework/executors/base_executor.py"""
import pytest

from framework.executors.base_executor import TaskResult


class TestTaskResultInstantiation:
    def test_required_fields_only(self):
        r = TaskResult(module="my_module", success=True)
        assert r.module == "my_module"
        assert r.success is True

    def test_default_optional_fields(self):
        r = TaskResult(module="m", success=False)
        assert r.output == ""
        assert r.error == ""
        assert r.duration_s == 0.0
        assert r.instance_id == ""

    def test_all_fields_set(self):
        r = TaskResult(
            module="m",
            success=True,
            output="some output",
            error="",
            duration_s=12.5,
            instance_id="i-0abc",
        )
        assert r.module == "m"
        assert r.success is True
        assert r.output == "some output"
        assert r.duration_s == 12.5
        assert r.instance_id == "i-0abc"

    def test_failure_task_result(self):
        r = TaskResult(module="failing_mod", success=False, error="connection refused")
        assert r.success is False
        assert r.error == "connection refused"


class TestTaskResultToDict:
    def test_returns_dict(self):
        r = TaskResult(module="m", success=True)
        d = r.to_dict()
        assert isinstance(d, dict)

    def test_dict_contains_all_keys(self):
        r = TaskResult(module="m", success=True)
        d = r.to_dict()
        assert set(d.keys()) == {"module", "success", "output", "error", "duration_s", "instance_id"}

    def test_dict_values_match_fields(self):
        r = TaskResult(
            module="mod",
            success=False,
            output="stdout",
            error="stderr",
            duration_s=3.14,
            instance_id="i-xyz",
        )
        d = r.to_dict()
        assert d["module"] == "mod"
        assert d["success"] is False
        assert d["output"] == "stdout"
        assert d["error"] == "stderr"
        assert d["duration_s"] == 3.14
        assert d["instance_id"] == "i-xyz"

    def test_dict_is_json_serialisable(self):
        import json
        r = TaskResult(module="m", success=True, duration_s=1.5)
        json.dumps(r.to_dict())  # should not raise


class TestTaskResultFromDict:
    def test_roundtrip_via_to_dict(self):
        original = TaskResult(
            module="mod",
            success=True,
            output="out",
            error="",
            duration_s=7.2,
            instance_id="i-abc123",
        )
        restored = TaskResult.from_dict(original.to_dict())
        assert restored.module == original.module
        assert restored.success == original.success
        assert restored.output == original.output
        assert restored.error == original.error
        assert restored.duration_s == original.duration_s
        assert restored.instance_id == original.instance_id

    def test_from_dict_with_all_fields(self):
        data = {
            "module": "x",
            "success": False,
            "output": "",
            "error": "timeout",
            "duration_s": 60.0,
            "instance_id": "i-dead",
        }
        r = TaskResult.from_dict(data)
        assert r.module == "x"
        assert r.success is False
        assert r.error == "timeout"
        assert r.duration_s == 60.0

    def test_from_dict_with_defaults(self):
        r = TaskResult.from_dict({"module": "m", "success": True, "output": "", "error": "", "duration_s": 0.0, "instance_id": ""})
        assert r.module == "m"
        assert r.success is True
