# framework/__init__.py
"""
Airflow Orchestration Framework — public API surface.

Main entry points
-----------------
build_all_dags      Build and register all Airflow DAGs from deployed configs.
AirflowJobConfig    Fully parsed config dataclass for one deployed module.
build_output_path   Build a framework-convention dated output filename.
extract_file_date   Parse the production date from a convention filename.
"""
from framework.config_loader import (
    AirflowJobConfig,
    PipelineConfig,
    ExecutionConfig,
    JobConfig,
    ServerConfig,
    RetryConfig,
    AlertConfig,
    build_output_path,
    extract_file_date,
    load_job_config,
    scan_job_configs,
)
from framework.segment_resolver import (
    ExecutionSegment,
    build_pipeline_segments,
    sort_by_position,
    validate_singleton,
)
from framework.pipeline_state import (
    PipelineConsumptionState,
    PipelineStateManager,
    detect_shared_upstreams,
)
from framework.frequency_resolver import (
    FrequencyRelation,
    ProcessingStrategy,
    FrequencyResolution,
    resolve_upstream_frequency,
    cron_interval_seconds,
    split_into_chunks,
)
from framework.dag_factory import build_all_dags

__all__ = [
    # config
    "AirflowJobConfig", "PipelineConfig", "ExecutionConfig", "JobConfig",
    "ServerConfig", "RetryConfig", "AlertConfig",
    "build_output_path", "extract_file_date",
    "load_job_config", "scan_job_configs",
    # segments
    "ExecutionSegment", "build_pipeline_segments",
    "sort_by_position", "validate_singleton",
    # state
    "PipelineConsumptionState", "PipelineStateManager", "detect_shared_upstreams",
    # frequency
    "FrequencyRelation", "ProcessingStrategy", "FrequencyResolution",
    "resolve_upstream_frequency", "cron_interval_seconds", "split_into_chunks",
    # dag
    "build_all_dags",
]
