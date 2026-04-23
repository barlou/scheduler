# framework/executors/local_executor.py
from __future__ import annotations

from multiprocessing import context
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

from framework.config_loader import AirflowJobConfig
from framework.executors.base_executor import BaseExecutor, TaskResult
from framework.segment_resolver import ExecutionSegment
from framework.venv_manager import VenvManager

# ─────────────────────────────────────────────────────────────────────────────
# Step callable — runs inside Airflow task
# ─────────────────────────────────────────────────────────────────────────────

def _run_local_step(
    module:      str,
    entry_point: str,
    config_path: str,
    dag_id:      str,
    **context,
) -> dict:

    """
    Airflow task callable for a single local step
    
    Lifecycle:
        1. Create venv at /tmp/venv_{module}
        2. Install module requirements.txt
        3. Run entry_point with config_path
        4. Cleanup venv (always - even on failure)

    Args:
        module (str): module name matching ~/deployments/{module}/
        entry_point (str): relative path to script inside module dir
        config_path (_type_): relative path to config inside module dir 
        dag_id (str): pipeline DAG ID for logging
        **context: Airflow task context (injected automatically)

    Returns:
        dict: _description_
    """
    ti: object = context.get("ti")
    print(
        f"\n[local] Starting step\n"
        f"  DAG: {dag_id}\n"
        f"  Module: {module}\n"
        f"  Script: {entry_point}\n"
        f"  Config: {config_path}"
    )
    
    start = time.monotonic()
    manager = VenvManager(module)
    error = ""
    output = ""
    
    try:
        manager.create()
        manager.install()
        manager.run(entry_point, config_path)
        
        duration = time.monotonic() - start
        result = TaskResult(
            module =     module,
            success =    True,
            duration_s = round(duration, 2),
        )
        
        print(
            f"\n[local] Step completed\n"
            f"  Module: {module}\n"
            f"  Duration: {duration:.1f}s"
        )
    
    except Exception as e:
        duration = time.monotonic() - start
        error = str(e)
        result = TaskResult(
            module = module,
            success = False,
            error = error,
            duration_s = round(duration, 2),
        )
        print(
            f"\n[local] Step FAILED\n"
            f"  Module: {module}\n"
            f"  Error: {error}"
        )
        # Re-raise so Airflow marks the task as failed 
        # and supplies retry logic from AirflowJobConfig
        raise RuntimeError(
            f"[Local] Module '{module}' failed: {error}"
        ) from e 
    
    finally:
        manager.cleanup()
    
    return result.to_dict()

# ─────────────────────────────────────────────────────────────────────────────
# LocalExecutor
# ─────────────────────────────────────────────────────────────────────────────

class LocalExecutor(BaseExecutor):
    """
    Runs all steps in a segment directly on the Airflow server.
    
    One PythonOperator per step.
    Steps are chained sequentially within the segment
    The last task is returned for chaining to the next segment
    
    Used when:
        - segment.is_local is True
        - All steps before the first EC2 declaration in a pipeline
    """
    
    def build_tasks(
        self,
        dag: DAG,
        upstream_task: PythonOperator | None = None,
    ) -> PythonOperator:
        """
        Build one PythonOperator per step in this segment

        Args:
            dag (DAG): DAG to attach tasks to
            upstream_task (PythonOperator | None, optional): last task of previous segment, or None. Defaults to None.

        Returns:
            PythonOperator: Last task created - used to chain to next segment
        """
        self._log_segment_summary()
        
        tasks: list[PythonOperator] = []
        
        for step in self.segment.steps:
            task = self._build_step_task(dag, step)
            tasks.append(task)
        
        self._chain_tasks(tasks, upstream_task)
        
        return tasks[-1]
    
    # Private 
    def _build_step_task(
        self, 
        dag: DAG,
        step: AirflowJobConfig,
    ) -> PythonOperator:
        """
        Build a single PythonOperator for one module step

        Args:
            dag (DAG): DAG to attach to 
            step (AirflowJobConfig): for this module

        Returns:
            PythonOperator: 
        """
        task_id = self._task_id(step)
        
        return self._make_python_task(
            dag=    dag,
            task_id= task_id,
            callable= _run_local_step,
            op_kwargs={
                "module":      step.job.module,
                "entry_point": step.job.entry_point,
                "config_path": step.job.config_path,
                "dag_id":      step.dag_id,
            },
            step = step,
        )