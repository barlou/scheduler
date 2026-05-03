# framework/freshness.py
"""
freshness.py
============
Preflight freshness check tasks for Airflow DAGs.

Each pipeline with upstream dependencies gets a ``freshness_check``
task injected as the first node of its DAG. This task:

1. Loads ``PipelineConsumptionState`` for each declared upstream.
2. Lists available files from cloud storage for each upstream.
3. Filters files to those not yet consumed by this pipeline.
4. Calls ``resolve_upstream_frequency`` to determine the processing
   strategy (block / sequential / parallel).
5. Pushes a structured freshness report to XCom for downstream tasks.
6. Writes a human-readable summary to Airflow task logs.

The downstream processing tasks pull the freshness report from XCom
and adjust their behaviour accordingly.

References
----------
- Airflow XCom: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
- Airflow params: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html
"""
from __future__ import annotations 

import sys
from datetime import datetime, timezone 
from typing import Any 

from framework.config_loader import AirflowJobConfig, extract_file_date
from framework.frequency_resolver import (
    FrequencyRelation,
    ProcessingStrategy,
    resolve_upstream_frequency,
)
from framework.pipeline_state import PipelineConsumptionState, PipelineStateManager

# ─────────────────────────────────────────────────────────────────────────────
# Freshness report dataclass (XCom-serialisable)
# ─────────────────────────────────────────────────────────────────────────────

def _build_freshness_report(
    pipeline_id: str,
    upstream_job_id: str,
    resolution:  Any,
    state:       PipelineConsumptionState,
) -> dict[str, Any]:
    """Build a JSON-serialisable freshness report for one upstream."""
    return {
        "pipeline_id":            pipeline_id,
        "upstream_job_id":        upstream_job_id,
        "relation":               resolution.relation.value,
        "strategy":               resolution.strategy.value,
        "is_blocked":             resolution.is_blocked,
        "pending_files":          resolution.pending_files,
        "pending_count":          len(resolution.pending_files),
        "last_consumed_file":     state.last_consumed_file,
        "last_consumed_at":       (
            state.last_consumed_at.isoformat()
            if state.last_consumed_at else None
        ),
        "upstream_interval_h":    round(resolution.upstream_interval_s / 3600, 2),
        "downstream_interval_h":  round(resolution.downstream_interval_s / 3600, 2),
        "checked_at":             datetime.now(timezone.utc).isoformat(),
    }

def _print_freshness_table(reports: list[dict[str, Any]]) -> None:
    """Print a human-readable freshness summary to Airflow task logs."""
    print(f"\n{'=' * 65}")
    print(f"{'FRESHNESS REPORT':^65}")
    print(f"{'=' * 65}")
    print(f"    {'Upstream':<25} {'Relation':<14} {'Strategy':<12} {'Pending':>7}")
    print(f"    {'-'*25} {'-'*14} {'-'*12} {'-'*7}")
    
    any_blocked = False
    for r in reports:
        icon = "🚫" if r["is_blocked"] else ("✅" if not r["pending_count"] else "📦")
        print(
            f"  {icon} {r['upstream_job_id']:<23} "
            f"{r['relation']:<14} "
            f"{r['strategy']:<12} "
            f"{r['pending_count']:>7}"
        )
        if r["is_blocked"]:
            any_blocked = True
    
    print(f"{'=' * 65}")
    if any_blocked:
            print(" ⚠️  One or more mandatory upstreams have no new file")
            print("     This pipeline run will be blocked")
    print(f"{'=' * 65}\n")
    
# ─────────────────────────────────────────────────────────────────────────────
# Airflow task callable
# ─────────────────────────────────────────────────────────────────────────────

def freshness_check_task(
    current_config:   AirflowJobConfig,
    upstream_configs: dict[str, AirflowJobConfig],
    state_manager:    PipelineStateManager,
    available_files:  dict[str, list[str]],
    **context,
) -> dict[str, Any]:
    """
    Preflight freshness check — runs as the first Airflow task in a pipeline.

    Determines which upstream files are new and selects a processing
    strategy for each upstream. Pushes a structured report to XCom.

    Parameters
    ----------
    current_config : AirflowJobConfig
        Config of the current (downstream) job.
    upstream_configs : dict[str, AirflowJobConfig]
        Mapping of upstream airflow_id → AirflowJobConfig.
    state_manager : PipelineStateManager
        State manager bound to cloud storage.
    available_files : dict[str, list[str]]
        Mapping of upstream airflow_id → list of filenames currently
        available in cloud storage for that upstream.
        Provided by the caller (cloud client listing).
    **context : dict
        Airflow task context (provides ``ti`` for XCom push).

    Returns
    -------
    dict[str, Any]
        XCom-serialisable dict mapping upstream_id → freshness report.
        Also pushed to XCom under key ``"freshness_reports"``.

    Raises
    ------
    SystemExit
        If any mandatory upstream is blocked (no new file).
    """
    ti          = context["ti"]
    pipeline_id = current_config.airflow_id
    reports     = {}
    any_blocked = False
    
    
    for upstream_id, upstream_cfg in upstream_configs.items():
        # Load persisted state for this (pipeline,  upstream) pair
        state = state_manager.load(pipeline_id, upstream_id)
        
        # Filter to files not yet consumed by THIS pipeline
        all_files       = available_files.get(upstream_id, [])
        last_consumed   = state.last_consumed_file
        
        if last_consumed:
            last_date = extract_file_date(last_consumed)
            if last_date is not None:
                pending = [
                    f for f in all_files
                    if (d := extract_file_date(f)) is not None and d > last_date
                ]
            else:
                # Cannot parse last_consumed date — conservation: treat all as pending
                pending = all_files
        else:
            # First run — all files are pending 
            pending = all_files
        
        pending = sorted(
            pending,
            key=lambda f: extract_file_date(f) or datetime.min.replace(tzinfo=timezone.utc),
        )
        
        # Update state with pending files 
        state.files_pending = pending
        
        # Resolve frequency relationship and processing strategy 
        resolution = resolve_upstream_frequency(
            upstream_job_id= upstream_id,
            upstream_cron=   upstream_cfg.schedule,
            downstream_cron= current_config.schedule,
            pending_files=   pending,
            chunk_threshold= current_config.pipeline.chunk_threshold,
        )
        
        report = _build_freshness_report(pipeline_id, upstream_id, resolution, state)
        reports[upstream_id] = report
        
        if resolution.is_blocked:
            any_blocked = True
        
    _print_freshness_table(list(reports.values()))
    
    # Push full report to XCom for downstream tasks
    ti.xcom_push(key="freshness_reports", value=reports)
    
    if any_blocked:
        print(
            "[freshness] BLOCKED — mandatory upstream(s) have no new file. "
            "Failing this task to prevent downstream processing on stale data.",
            file=sys.stderr,
        )
        # Raise so Airflow marks the task as failed and stops the DAG run
        raise RuntimeError("Freshness check failed: mandatory upstream(s) produced no new file.")
    
    return reports