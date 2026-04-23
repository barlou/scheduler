# framework/executors/base_executors.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

from airflow import DAG
from airflow.models import TaskInstance 
from airflow.operators.python import PythonOperator

from framework.config_loader import AirflowJobConfig
from framework.segment_resolver import ExecutionSegment

# ─────────────────────────────────────────────────────────────────────────────
# Task result — passed between tasks via Airflow XCom
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TaskResult:
    """
    Outcome of a single module step execution.
    Pushed to XCom s downstream tasks and the DAG summary can read it
    
    Attributes:
        module:      module name that ran
        success:     True if the step completed without error
        output:      captured stdout from the step
        error:       error message if success=False
        duration_s:  wall-clock seconds the step took
        instance_id: cloud Instance ID if ran on cloud, empty if local
    """
    module:      str
    success:     bool
    output:      str = ""
    error:       str = ""
    duration_s:  float = 0.0
    instance_id: str = ""
    
    def to_dict(self) -> dict:
        return {
            "module":      self.module,
            "success":     self.success,
            "output":      self.output,
            "error":       self.error,
            "duration_s":  self.duration_s,
            "instance_id": self.instance_id,
        }
    
    @classmethod
    def from_dict(cls, data:dict) -> "TaskResult":
        return cls(**data)
    
# ─────────────────────────────────────────────────────────────────────────────
# Base executor
# ─────────────────────────────────────────────────────────────────────────────

class BaseExecutor(ABC):
    """
    Abstract base class for pipeline semgent executors.
    
    Each executor receives one ExecutionSegment and builds the Airflow tasks 
    for it inside the provided DAG.
    
    Two concrete implementation:
        - LocalExecutor - runs steps on the Airflow server
        - CloudExecutor - runs steps on a cloud instance via provider
        
    Executors are stateless - they only build tasks.
    All runtime state(instance IDs, outputs) lives in XCom.
    """
    
    def __init__(self, segment: ExecutionSegment):
        self.segment = segment
    
    # Abstract - must be implemented 
    @abstractmethod
    def build_tasks(
        self,
        dag: DAG,
        upstream_task: "PythonOperator | None" = None,
    ) -> "PythonOperator":
        """
        Build Airflow tasks for this segment and add them to the DAG.

        Args:
            dag (DAG): the DAG this segment belongs to 
            upstream_task (PythonOperator | None, optional): the last task of the previous segment, used to chain
                dependencies between segments. None if this is the first segment. Defaults to None.

        Returns:
            PythonOperator: The last task created in this segment.
            dag_factory uses this to chain to the next segment.
        """
        ...
    
    # Concret helpers - shared by both executors
    def _task_id(self, step: AirflowJobConfig, suffix: str = "") -> str:
        """
        Generate a unique, readable Airflow task ID for a step.
        Format: {module}__{suffix} if suffix given
                {module}           otherwise
        
        Airflow requires task IDs to be unique within a DAG. Using module name guarantee
        uniqueness since each module appears exactly once per pipeline.
        """
        base = step.job.module.lower().replace("-", "_")
        return f"{base}__{suffix}" if suffix else base
    
    def _make_python_task(
        self,
        dag:       DAG,
        task_id:   str,
        callable:  Callable,
        op_kwargs: dict,
        step:      AirflowJobConfig,
    ) -> PythonOperator:
        """
        Build a single Python Operator task with retry and alert config derived 
        from the step's AirflowJobConfig.

        Args:
            dag (DAG):               Dag to attach the task to
            task_id (str):           unique task ID within the DAG
            callable (Callable):     Python function to call
            op_kwargs (dict):        keyword args passed to callable 
            step (AirflowJobConfig): AirflowJobConfig for retry/alert config

        Returns:
            PythonOperator
        """
        from datetime import timedelta
        
        return PythonOperator(
            task_id =          task_id,
            python_callable =  callable,
            op_kwargs =        op_kwargs,
            retries =          step.retry.attempts,
            retry_delay =      timedelta(minutes=step.retry.delay_minutes),
            email_on_failure = step.alerts.on_failure,
            email_on_retry =   step.alerts.on_retry,
            email_on_success = step.alerts.on_success,
            dag =              dag,
        )
    
    def _chain_tasks(
        self,
        tasks: list[PythonOperator],
        upstream_task: "PythonOperator | None",
    ) -> None:
        """
        Chain a list of tasks sequentially and connect to upsteam

        Args:
            tasks (list[PythonOperator]): order list of tasks to chain
            upstream_task (PythonOperator | None): task from previous segment to connect to first task
                None if this segment starts the pipeline
        """
        if not tasks:
            return 
        
        # Connect to previous segment:
        if upstream_task is not None:
            tasks[0].set_upstream(upstream_task)
        
        # Chain tasks withing segment sequentially
        for i in range(1, len(tasks)):
            tasks[i].set_upstream(tasks[i - 1])
    
    def _log_segment_summary(self) -> None:
        """Print a readable summary of this summary for Airflow task logs"""
        server_label = (
            f"{self.segment.server.provider}:"
            f"{self.segment.server.instance_type}"
            if self.segment.server else "local"
        )
        step_names = [s.job.module for s in self.segment.steps]
        print(
            f"\n[executor] Segment summary\n"
            f"  Server: {server_label}\n"
            f"  Steps: {step_names}\n"
            f"  Force terminate : {self.segment.force_terminate}\n"
            f"  Is last : {self.segment.is_last}"
        )
        
    # Factory method - instantiate correct executor from segment 
    @staticmethod
    def from_segment(segment: ExecutionSegment) -> "BaseExecutor":
        """
        Instantiate the correct executor base on segment.is_local

        Args:
            segment (ExecutionSegment): resolve ExecutionSegment

        Returns:
            LocalExecutor: if segment.is_local
            CloudExecutor: if segment.is_cloud
        """
        if segment.is_local:
            from framework.executors.local_executor import LocalExecutor
            return LocalExecutor(segment)
        else:
            from framework.executors.cloud_executor import CloudExecutor
            return CloudExecutor(segment)