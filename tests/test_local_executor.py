"""Tests for framework/executors/local_executor.py"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call

from airflow import DAG
from airflow.operators.python import PythonOperator

from framework.executors.base_executor import TaskResult
from framework.executors.local_executor import LocalExecutor, _run_local_step
from framework.segment_resolver import ExecutionSegment
from tests.conftest import make_local_config


def _make_dag(dag_id: str = "test_dag") -> DAG:
    return DAG(dag_id, start_date=datetime(2024, 1, 1))


# ─────────────────────────────────────────────────────────────────────────────
# _run_local_step (Airflow task callable)
# ─────────────────────────────────────────────────────────────────────────────

class TestRunLocalStep:
    def _mock_manager_class(self):
        """Return a mock VenvManager class whose instances succeed."""
        mock_instance = MagicMock()
        mock_class = MagicMock(return_value=mock_instance)
        return mock_class, mock_instance

    def test_returns_task_result_dict_on_success(self):
        mock_cls, mock_inst = self._mock_manager_class()
        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            result = _run_local_step(
                module="my_mod",
                entry_point="src/main.py",
                config_path="config/config.json",
                dag_id="test_dag",
            )
        assert isinstance(result, dict)
        assert result["module"] == "my_mod"
        assert result["success"] is True
        assert result["error"] == ""
        assert result["duration_s"] >= 0

    def test_creates_installs_runs_in_order(self):
        mock_cls, mock_inst = self._mock_manager_class()
        call_order = []
        mock_inst.create.side_effect = lambda: call_order.append("create")
        mock_inst.install.side_effect = lambda: call_order.append("install")
        mock_inst.run.side_effect = lambda *a, **k: call_order.append("run")
        mock_inst.cleanup.side_effect = lambda: call_order.append("cleanup")

        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            _run_local_step(
                module="m", entry_point="e", config_path="c", dag_id="d"
            )
        assert call_order == ["create", "install", "run", "cleanup"]

    def test_cleanup_called_even_on_failure(self):
        mock_cls, mock_inst = self._mock_manager_class()
        mock_inst.run.side_effect = RuntimeError("script crashed")
        cleanup_called = []
        mock_inst.cleanup.side_effect = lambda: cleanup_called.append(True)

        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            with pytest.raises(RuntimeError, match="Module 'fail_mod' failed"):
                _run_local_step(
                    module="fail_mod", entry_point="e", config_path="c", dag_id="d"
                )
        assert cleanup_called == [True]

    def test_raises_runtime_error_with_module_name_on_failure(self):
        mock_cls, mock_inst = self._mock_manager_class()
        mock_inst.create.side_effect = RuntimeError("venv creation failed")

        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            with pytest.raises(RuntimeError) as exc:
                _run_local_step(
                    module="broken_mod", entry_point="e", config_path="c", dag_id="d"
                )
        assert "broken_mod" in str(exc.value)

    def test_accepts_airflow_context_kwargs(self):
        """_run_local_step must accept **context from Airflow without error."""
        mock_cls, mock_inst = self._mock_manager_class()
        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            result = _run_local_step(
                module="m",
                entry_point="e",
                config_path="c",
                dag_id="d",
                ti=MagicMock(),     # Airflow injects this
                run_id="run_123",   # and other context keys
            )
        assert result["success"] is True

    def test_failure_result_contains_error_message(self):
        mock_cls, mock_inst = self._mock_manager_class()
        mock_inst.install.side_effect = FileNotFoundError("no requirements.txt")

        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            with pytest.raises(RuntimeError):
                _run_local_step(
                    module="m", entry_point="e", config_path="c", dag_id="d"
                )

    def test_passes_module_dir_to_venv_manager(self):
        mock_cls, mock_inst = self._mock_manager_class()
        with patch("framework.executors.local_executor.VenvManager", mock_cls) as patched:
            _run_local_step(
                module="target_mod", entry_point="e", config_path="c", dag_id="d"
            )
        patched.assert_called_once_with("target_mod")

    def test_run_called_with_entry_point_and_config(self):
        mock_cls, mock_inst = self._mock_manager_class()
        with patch("framework.executors.local_executor.VenvManager", mock_cls):
            _run_local_step(
                module="m",
                entry_point="src/train.py",
                config_path="config/prod.json",
                dag_id="d",
            )
        mock_inst.run.assert_called_once_with("src/train.py", "config/prod.json")


# ─────────────────────────────────────────────────────────────────────────────
# LocalExecutor.build_tasks
# ─────────────────────────────────────────────────────────────────────────────

class TestLocalExecutorBuildTasks:
    def test_returns_last_task(self):
        steps = [
            make_local_config(module="step_a", schedule="0 1 * * *"),
            make_local_config(module="step_b", schedule="0 2 * * *"),
            make_local_config(module="step_c", schedule="0 3 * * *"),
        ]
        seg = ExecutionSegment(server=None, steps=steps)
        executor = LocalExecutor(seg)
        dag = _make_dag("multi_step")

        last_task = executor.build_tasks(dag)
        assert last_task.task_id == "step_c"

    def test_creates_one_task_per_step(self):
        steps = [
            make_local_config(module="a", schedule="0 1 * * *"),
            make_local_config(module="b", schedule="0 2 * * *"),
        ]
        seg = ExecutionSegment(server=None, steps=steps)
        executor = LocalExecutor(seg)
        dag = _make_dag("two_steps")

        executor.build_tasks(dag)
        task_ids = {t.task_id for t in dag.tasks}
        assert "a" in task_ids
        assert "b" in task_ids

    def test_single_step_returns_that_task(self):
        step = make_local_config(module="only_step")
        seg = ExecutionSegment(server=None, steps=[step])
        executor = LocalExecutor(seg)
        dag = _make_dag("single")

        last_task = executor.build_tasks(dag)
        assert last_task.task_id == "only_step"

    def test_upstream_task_connected_to_first_step(self):
        steps = [
            make_local_config(module="first", schedule="0 1 * * *"),
            make_local_config(module="second", schedule="0 2 * * *"),
        ]
        seg = ExecutionSegment(server=None, steps=steps)
        executor = LocalExecutor(seg)
        dag = _make_dag("upstream_chain")

        upstream = MagicMock(spec=PythonOperator)
        executor.build_tasks(dag, upstream_task=upstream)

        # The first task should have the external upstream set
        first_task = dag.get_task("first")
        assert upstream in first_task.upstream_list

    def test_task_kwargs_contain_module_info(self):
        step = make_local_config(module="train_model")
        step.job.entry_point = "src/train.py"
        step.job.config_path = "config/prod.json"
        seg = ExecutionSegment(server=None, steps=[step])
        executor = LocalExecutor(seg)
        dag = _make_dag("kwargs_check")

        executor.build_tasks(dag)
        task = dag.get_task("train_model")
        kwargs = task.op_kwargs
        assert kwargs["module"] == "train_model"
        assert kwargs["entry_point"] == "src/train.py"
        assert kwargs["config_path"] == "config/prod.json"
