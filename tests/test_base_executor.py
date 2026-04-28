"""Tests for framework/executors/base_executor.py (helpers and factory)."""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.operators.python import PythonOperator

from framework.config_loader import AirflowJobConfig, ExecutionConfig, JobConfig
from framework.executors.base_executor import BaseExecutor, TaskResult
from framework.segment_resolver import ExecutionSegment
from tests.conftest import make_local_config, make_cloud_config, make_server_config


# ─────────────────────────────────────────────────────────────────────────────
# Concrete subclass used to test abstract base
# ─────────────────────────────────────────────────────────────────────────────

class _ConcreteExecutor(BaseExecutor):
    def build_tasks(self, dag, upstream_task=None):
        return MagicMock(spec=PythonOperator)


def _make_dag(dag_id: str = "test_dag") -> DAG:
    return DAG(dag_id, start_date=datetime(2024, 1, 1))


# ─────────────────────────────────────────────────────────────────────────────
# _task_id
# ─────────────────────────────────────────────────────────────────────────────

class TestTaskId:
    def _executor(self):
        seg = ExecutionSegment(server=None, steps=[make_local_config()])
        return _ConcreteExecutor(seg)

    def test_basic_module_name(self):
        exe = self._executor()
        step = make_local_config(module="my_module")
        assert exe._task_id(step) == "my_module"

    def test_hyphens_replaced_with_underscores(self):
        exe = self._executor()
        step = make_local_config(module="data-ingestion")
        assert exe._task_id(step) == "data_ingestion"

    def test_with_suffix(self):
        exe = self._executor()
        step = make_local_config(module="train")
        assert exe._task_id(step, suffix="setup") == "train__setup"

    def test_suffix_with_hyphenated_module(self):
        exe = self._executor()
        step = make_local_config(module="data-load")
        assert exe._task_id(step, suffix="teardown") == "data_load__teardown"

    def test_uppercase_lowercased(self):
        exe = self._executor()
        step = make_local_config(module="MyModule")
        assert exe._task_id(step) == "mymodule"


# ─────────────────────────────────────────────────────────────────────────────
# _chain_tasks
# ─────────────────────────────────────────────────────────────────────────────

class TestChainTasks:
    def _executor(self):
        seg = ExecutionSegment(server=None, steps=[make_local_config()])
        return _ConcreteExecutor(seg)

    def test_empty_tasks_does_nothing(self):
        exe = self._executor()
        upstream = MagicMock(spec=PythonOperator)
        exe._chain_tasks([], upstream)  # should not raise

    def test_single_task_connected_to_upstream(self):
        exe = self._executor()
        upstream = MagicMock(spec=PythonOperator)
        task = MagicMock(spec=PythonOperator)
        exe._chain_tasks([task], upstream)
        task.set_upstream.assert_called_once_with(upstream)

    def test_no_upstream_means_no_set_upstream_call_on_first(self):
        exe = self._executor()
        task = MagicMock(spec=PythonOperator)
        exe._chain_tasks([task], upstream_task=None)
        task.set_upstream.assert_not_called()

    def test_multiple_tasks_chained_sequentially(self):
        exe = self._executor()
        t1 = MagicMock(spec=PythonOperator)
        t2 = MagicMock(spec=PythonOperator)
        t3 = MagicMock(spec=PythonOperator)
        exe._chain_tasks([t1, t2, t3], upstream_task=None)
        # t1 has no upstream (None was passed)
        t1.set_upstream.assert_not_called()
        # t2 must follow t1
        t2.set_upstream.assert_called_once_with(t1)
        # t3 must follow t2
        t3.set_upstream.assert_called_once_with(t2)

    def test_upstream_connected_to_first_task_only(self):
        exe = self._executor()
        upstream = MagicMock(spec=PythonOperator)
        t1 = MagicMock(spec=PythonOperator)
        t2 = MagicMock(spec=PythonOperator)
        exe._chain_tasks([t1, t2], upstream_task=upstream)
        t1.set_upstream.assert_called_once_with(upstream)
        # t2 should be connected to t1, not to upstream
        t2.set_upstream.assert_called_once_with(t1)


# ─────────────────────────────────────────────────────────────────────────────
# _make_python_task
# ─────────────────────────────────────────────────────────────────────────────

class TestMakePythonTask:
    def test_creates_python_operator_with_correct_task_id(self):
        step = make_local_config(module="m1")
        seg = ExecutionSegment(server=None, steps=[step])
        exe = _ConcreteExecutor(seg)
        dag = _make_dag()

        task = exe._make_python_task(
            dag=dag,
            task_id="test_task",
            callable=lambda **ctx: None,
            op_kwargs={"key": "val"},
            step=step,
        )
        assert task.task_id == "test_task"

    def test_applies_retry_config_from_step(self):
        from framework.config_loader import RetryConfig
        step = make_local_config()
        step.retry = RetryConfig(attempts=5, delay_minutes=3)
        seg = ExecutionSegment(server=None, steps=[step])
        exe = _ConcreteExecutor(seg)
        dag = _make_dag()

        task = exe._make_python_task(
            dag=dag,
            task_id="retry_task",
            callable=lambda **ctx: None,
            op_kwargs={},
            step=step,
        )
        assert task.retries == 5

    def test_applies_alert_config_from_step(self):
        from framework.config_loader import AlertConfig
        step = make_local_config()
        step.alerts = AlertConfig(on_failure=False, on_retry=True, on_success=True)
        seg = ExecutionSegment(server=None, steps=[step])
        exe = _ConcreteExecutor(seg)
        dag = _make_dag()

        task = exe._make_python_task(
            dag=dag,
            task_id="alert_task",
            callable=lambda **ctx: None,
            op_kwargs={},
            step=step,
        )
        assert task.email_on_failure is False
        assert task.email_on_retry is True


# ─────────────────────────────────────────────────────────────────────────────
# from_segment factory
# ─────────────────────────────────────────────────────────────────────────────

class TestFromSegment:
    def test_local_segment_returns_local_executor(self):
        from framework.executors.local_executor import LocalExecutor
        seg = ExecutionSegment(server=None, steps=[make_local_config()])
        executor = BaseExecutor.from_segment(seg)
        assert isinstance(executor, LocalExecutor)

    def test_cloud_segment_returns_cloud_executor(self):
        from framework.executors.cloud_executor import CloudExecutor
        seg = ExecutionSegment(server=make_server_config(), steps=[make_cloud_config()])
        executor = BaseExecutor.from_segment(seg)
        assert isinstance(executor, CloudExecutor)

    def test_executor_holds_the_segment(self):
        seg = ExecutionSegment(server=None, steps=[make_local_config()])
        executor = BaseExecutor.from_segment(seg)
        assert executor.segment is seg


# ─────────────────────────────────────────────────────────────────────────────
# _log_segment_summary
# ─────────────────────────────────────────────────────────────────────────────

class TestLogSegmentSummary:
    def test_does_not_raise_for_local_segment(self):
        seg = ExecutionSegment(server=None, steps=[make_local_config()])
        exe = _ConcreteExecutor(seg)
        exe._log_segment_summary()  # should not raise

    def test_does_not_raise_for_cloud_segment(self):
        seg = ExecutionSegment(server=make_server_config(), steps=[make_cloud_config()])
        exe = _ConcreteExecutor(seg)
        exe._log_segment_summary()  # should not raise
