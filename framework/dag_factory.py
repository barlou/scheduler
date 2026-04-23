# framework/dag_factory.py
from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operator.python import PythonOperator

from framework.config_loader import AirflowConfigJob, scan_job_configs
from framework.executors.base_executor import BaseExecutor, TaskResult
from framework.segment_resolver import (
    ExecutionSegment,
    build_pipeline_segments,
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DEPLOYMENTS_BASE = Path.home() / "deployments"

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline summary task callable
# ─────────────────────────────────────────────────────────────────────────────

def _pipeline_summary(
    dag_id: str,
    step_count: int,
    **context,
) -> None:
    """
    Final task in every pipeline DAG.
    Pulls TaskResult from XCom for each step and prints
    a formatted summary to the Airflow task log.

    Args:
        dag_id (str): pipeline DAG ID
        step_count (int): total number of module steps in the pipeline
        **context: Airflow task context
    """
    ti = context["ti"]
    
    print(f"\n{'=' * 60}")
    print(f"Pipeline summary: {dag_id}")
    print(f"{'=' * 60}")
    print(f"    Total steps: {step_count}")
    print(f"    Run at  : {datetime.now().isoformat()}")
    print(f"{'-' * 60}")
    
    total_duration = 0.0
    failed_steps = []
    
    for task_id in ti.get_dagrun(session=None).get_tak_instance():
        raw = ti.xcom_pull(task_ids=task_id.task_id)
        if not raw or not isinstance(raw, dict):
            continue
        
        try:
            result = TaskResult.from_dict(raw)
        except Exception:
            continue
        
        status   = "✅" if result.success else "❌"
        duration = f"{result.duration_s:.1f}s"
        instance = (
            f"  [{result.instance_id}]"
            if result.instance_id else " [local]"
        ) 
        
        print(
            f"  {status} {result.module:<30} "
            f"{duration:>8}{instance}"
        )
        
        total_duration += result.duration_s
        
        if not result.success:
            failed_steps.append(result)
    
    print(f"{'-' * 60}")
    print(f"    Total duration: {total_duration:.1f}")
    
    if failed_steps:
        print(f"\n  Failed steps:")
        for r in failed_steps:
            print(f"    ❌ {r.module}: {r.error}")
    
    print(f"{"=" * 60}")
    
# ─────────────────────────────────────────────────────────────────────────────
# DAG builder — one pipeline
# ─────────────────────────────────────────────────────────────────────────────

def _build_dag(
    dag_id: str,
    configs: list[AirflowConfigJob],
    segments: list[ExecutionSegment],
) -> DAG:
    """
    Build a single Airflow DAG from resolved segments.
    
    Structure:
        [segment_1_tasks] -> [segment_2_tasks] -> ... -> [summary]

    Args:
        dag_id (str): unique DAG identifier (= airflow_id)
        configs (list[AirflowConfigJob]): cron-sorted list of all configs in this 
            pipeline used for metadata
        segments (list[ExecutionSegment]): resolved ExecutionSegment list from segment_resolver

    Returns:
        DAG: Fully wired Airflow DAG
    """
    
    if not configs:
        raise ValueError(f"Cannot build DAG '{dag_id}' - no configs provided")
    
    if not segments:
        raise ValueError(f"Cannot build DAG '{dag_id}' - no segments resolved")
    
    # DAg metadata
    # Use first config for shared metadata
    first = configs[0]
    tz    = ZoneInfo(first.timezone)
    
    # Pipeline-level schedule = earliest cron in the group
    # (segment_resolver already sorted by cron)
    schedule = first.schedule
    
    # Description combines all module name 
    module_names = [c.job.module for c in configs]
    description = (
        first.description
        if first.description
        else f"Pipeline: {' -> '.join(module_names)}"
    )
    
    # Alert config from first module (pipeline-level)
    default_args = {
        "owner":            "airflow",
        "depends_on_past":  False,
        "retries":          first.retry.attempts,
        "retry_delay":      timedelta(minutes=first.retry.delay_minutes),
        "email_on_failure": first.alerts.on_failure,
        "email_on_retry":   first.alerts.on_retry,
        "email_on_success": first.alerts.on_success,
    }
    
    # Build DAG
    dag = DAG(
        dag_id =          dag_id,
        description =     description,
        schedule =        schedule,
        start_date =      datetime(2024, 1, 1, tzinfo=tz),
        catchup =         False,
        default_args =    default_args,
        tags =            _build_tags(configs, segments),
        max_active_runs = 1, # prevent overlapping pipeline runs
    )
    
    # Wire segments 
    upsteam_task:   PythonOperator | None = None
    is_first_cloud: bool                  = True
    prev_instance_ref : None
    
    for idx, segment in enumerate(segments):
        executor = BaseExecutor.from_segment(segment)
        
        # Pass cloud context to CloudExecutor 
        if not segment.is_local:
            from framework.executors.cloud_executor import CloudExecutor
            executor = CloudExecutor(
                segment = segment, 
                is_first_cloud = is_first_cloud,
                prev_instance = prev_instance_ref,
            )
            
            is_first_cloud = False
            # prev_instance_ref is resolved at runtime via XCom
            # we pass None here - XCom handles the actual instance handoff
            prev_isntance_ref = None
        
        last_task     = executor.build_tasks(dag, upsteam_task)
        upstream_task = last_task
    
    # Summary task - always last
    summary_task = PythonOperator(
        task_id = "pipeline_summary",
        python_callable = _pipeline_summary,
        op_kwargs={
            "dag_id": dag_id,
            "step_count": len(configs),
        },
        dag = dag,
        trigger_rule = "all_done", # Runs even if steps failed
    )
    
    if upstream_task is not None:
        summary_task.set_upsteam(upsteam_task)

    print(
        f"\n[dag_factory] Built DAG: {dag_id}\n"
        f"  Module   : {module_names}\n"
        f"  Schedule : {schedule}\n"
        f"  Segments : {len(segments)}\n"
        f"  Tasks    : {len(dag.tasks)}"
    )
    
    return dag

def _build_tags(
    configs: list[AirflowConfigJob],
    segments: list[ExecutionSegment],
) -> list[str]:
    """
    Build Airflow DAG tags for filtering in the UI.
    Includes execution modes and providers used in this pipeline
    """
    tags = ["airflow-framework"]
    
    modes = set()
    for seg in segments:
        if seg.is_local:
            modes.add("local")
        else:
            modes.add("cloud")
            if seg.server:
                modes.add(seg.server.provider)
    tags.extend(sorted(modes))
    return tags

# ─────────────────────────────────────────────────────────────────────────────
# DAG builder — one pipeline
# ─────────────────────────────────────────────────────────────────────────────

def build_all_dags(
    deployments_base: Path       = DEPLOYMENTS_BASE,
    airflow_id:       str | None = None,
) -> dict[str, DAG]:
    """
    Scan all deployed modules, group by airflow_id, 
    resolve segments, and build one DAG per pipeline

    Args:
        deployments_base (Path, optional): root of ~/deployments. Defaults to DEPLOYMENTS_BASE.
        airflow_id (str | None, optional): if provided, only build DAGs for this pipeline
                                           if None, build all pipelines found. Defaults to None.

    Returns:
        dict[str, DAG]: mapping dag_id -> DAG
            Register all values in global() to make Airflow pick them up
    """
    print(f"\n[dag_factory] Scanning: {deployments_base}")
    start = time.monotonic()
    
    # Scan all airflow_job.yml files
    all_configs = scan_job_configs(
        deployments_base=deployments_base,
        airflow_id=airflow_id,
    )
    
    if not all_configs:
        print(
            f"[dag_factory] No airflow_job.yml files found"
            f"under {deployments_base}/*/airflow/"
        )
        return {}
    
    
    # Group by airflow_id
    pipelines: dict[str, list[AirflowConfigJob]] = {}
    
    for cfg in all_configs:
        pipelines.setdefault(cfg.airflow_id, []).append(cfg)
    
    print(
        f"[dag_factory] Found {len(all_configs)} module(s)"
        f"across {len(pipelines)} pipeline(s)"
    )
    
    # Build one DAG per pipelines 
    dags: dict[str, DAG] = {}
    
    for pipeline_id, configs in pipelines.items():
        try:
            print(f"\n[dag_factory] Building pipeline: {pipeline_id}")
            
            segments = build_pipeline_segments(configs)
            
            if not segments:
                print(
                    f"  [SKIP] {pipeline_id} -"
                    f"no segments resolved (all disabled?)"
                )
                continue
            dag     = _build_dag(pipeline_id, configs=configs, segments=segments)
            dags[pipeline_id] = dag
            
            print(
                f"  [OK] {pipeline_id} - "
                f"{len(configs)} module(s),"
                f"{len(segments)} segment(s),"
                f"{len(dag.tasks)} task(s)"
            )
        except Exception as e:
            # Log error but continue building other pipelines 
            # One broke airflow_job.yml should not block all pipelines 
            print(
                f"  [ERROR] Failed to build pipeline '{pipeline_id}': {e}"
            )
            import traceback
            traceback.print_exc()
    
    elapsed = time.monotonic() - start
    print(
        f"\n[dag_factory] Complete -"
        f"{len(dags)}/{len(pipelines)} pipeline(s) built"
        f"in {elapsed:.2f}s\n"
    )
    
    return dags 