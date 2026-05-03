# Framework/config_loader.py
from __future__ import annotations

import os, re, yaml

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from datetime import datetime, timezone as _tz

# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses — one per section of airflow_job.yml
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ServerConfig: 
        provider:                   Literal["aws", "gcp", "azure", "ovh"]
        instance_type:              str
        region:                     str
        ami_id:                     str
        subnet_id:                  str
        security_group_id:          str
        iam_instance_profile:       str
        startup_timeout_minutes:    int = 10
        force_terminate:            bool = False
        
@dataclass
class ExecutionConfig:
        mode:   Literal["local", "cloud"]
        server: ServerConfig | None = None

@dataclass
class JobConfig:
        module:           str
        entry_point:      str
        config_path:      str
        base_output_name: str | None = None 

@dataclass
class RetryConfig:
    attempts:       int = 1
    delay_minutes:  int = 5
    
@dataclass
class AlertConfig:
    on_failure: bool = True
    on_retry:   bool = False
    on_success: bool = False

@dataclass
class PipelineConfig:
    """
    Pipeline-level metadata controlling execution order, dependencies,
    and data accumulation behaviour.

    Attributes
    ----------
    position : int
        Explicit execution order within the pipeline (1-based).
        Lower = runs first. Must be unique within a pipeline.
        Replaces the previous cron-based ordering.
    upstream_jobs : list[str]
        airflow_id values this job depends on.
        Used by the framework to detect shared upstreams and
        infer frequency relationships (mandatory vs accumulation).
    is_singleton : bool
        When True, this pipeline accepts no additional jobs.
        Raises a validation error at DAG build time if another job
        declares the same airflow_id.
    chunk_threshold : int
        Number of accumulated upstream files above which parallel
        chunk processing is triggered instead of sequential processing.
        Default: 10. Override per job when file sizes differ significantly.
    max_parallel_chunks : int
        Maximum number of Airflow worker tasks spawned in parallel
        during chunk processing. Must be <= Airflow parallelism setting.
        Default: 4.
    """
    position:            int
    upstream_jobs:       list[str] = field(default_factory=list)
    is_singleton:        bool      = False
    chunk_threshold:     int       = 10
    max_parallel_chunks: int       = 4

@dataclass
class AirflowJobConfig:
    airflow_id:     str
    dag_id:         str
    schedule:       str
    execution:      ExecutionConfig
    job:            JobConfig
    pipeline:       PipelineConfig
    description:    str         = ""
    timezone:       str         = "Europe/Paris"
    enabled:        bool        = True
    retry:          RetryConfig = field(default_factory=RetryConfig)
    alerts:         AlertConfig = field(default_factory=AlertConfig)
    source_path:    Path        = field(default_factory=Path)
    
# ─────────────────────────────────────────────────────────────────────────────
# File naming convention — used by all framework components
# ─────────────────────────────────────────────────────────────────────────────

def build_output_path(base_name: str, extension: str) -> str:
    """
    Build a dated output filename following the framework convention.

    The date is injected automatically by the framework — the caller
    provides only the semantic base name and file extension.
    This convention lets the framework detect whether an upstream has
    produced a new file since the last run of a downstream job.

    Convention: ``{base_name}_{YYYY-MM-DD}{extension}``

    Parameters
    ----------
    base_name : str
        Semantic name of the output, e.g. ``"transactions"`` or
        ``"daily_snapshot"``. Must not contain slashes.
    extension : str
        File extension including the leading dot, e.g. ``".parquet"``,
        ``".csv"``, ``".json"``.

    Returns
    -------
    str
        Filename with UTC date injected, e.g.
        ``"transactions_2026-05-03.parquet"``.

    Examples
    --------
    >>> build_output_path("transactions", ".parquet")
    'transactions_2026-05-03.parquet'
    """
    date_str = datetime.now(_tz.utc).strftime("%Y-%m-%d")
    return f"{base_name}_{date_str}{extension}"

def extract_file_date(filename: str) -> datetime | None:
    """
    Extract the production date from a framework-convention filename.

    Expects the pattern ``{any_prefix}_{YYYY-MM-DD}{any_suffix}``.
    Returns None if the pattern is not found.

    Parameters
    ----------
    filename : str
        Filename or full path string, e.g.
        ``"transactions_2026-05-03.parquet"``.

    Returns
    -------
    datetime | None
        Timezone-aware UTC datetime on success, None otherwise.

    Examples
    --------
    >>> extract_file_date("transactions_2026-05-03.parquet")
    datetime.datetime(2026, 5, 3, 0, 0, tzinfo=datetime.timezone.utc)
    >>> extract_file_date("no_date_here.parquet")
    None
    """
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=_tz.utc)
    except ValueError:
        return None
    
# ─────────────────────────────────────────────────────────────────────────────
# Placeholder resolution — same {{ PLACEHOLDER }} convention as config.template.json
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_placeholders(raw: str) -> str:
    """
    Replace {{ PLACEHOLDERS }} with values from environment.
    Raises ValueError if a placeholder has no corresponding env var.
    """
    placeholders = re.findall(r'\{\{\s*(\w+)\s*\}\}', raw)
    missing = []
    
    for key in placeholders:
        value = os.environ.get(key)
        if value is None:
            missing.append(key)
            continue
        raw = raw.replace("{{" + key + "}}", value)
        raw = raw.replace("{{ " + key + "}}", value)
        raw = raw.replace("{{ " + key + " }}", value)
        
    if missing: 
        raise ValueError(
            f"airflow_job.yml references undefined placeholders: {missing}\n"
            f"Set them as environment variables or Github Secrets."
        )
    return raw 

# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────

_REQUIRED_TOP_LEVEL = ["airflow_id", "dag_id", "schedule", "execution", "job"]
_REQUIRED_JOB       = ["module", "entry_point", "config_path"]
_REQUIRED_PIPELINE  = ["position"]
_REQUIRED_SERVER    = [
    "provider", "instance_type", "region",
    "ami_id", "subnet_id", "security_group_id", "iam_instance_profile",
]  
_VALID_PROVIDERS    = {"aws", "gcp", "azure", "ovh"}
_VALID_MODES        = {"local", "cloud"}

def _validate(cfg: dict, source: Path) -> None:
    """Raise ValueError with a clear message if the config is invalid"""
    errors = []
    
    # Top level required field
    for key in _REQUIRED_TOP_LEVEL:
        if key not in cfg:
            errors.append(f"missing required field: '{key}'")
    
    if errors:
        raise ValueError(f"{source}:\n" + "\n".join(f"   - {e}" for e in errors))
        
    # Execution mode
    execution = cfg.get("execution", {})
    mode = execution.get("mode")
    if mode not in _VALID_MODES:
        errors.append(f"execution.mode must be one of {_VALID_MODES}, got: '{mode}'")
        
    if mode == "cloud":
        server = execution.get("server")
        if not server:
            errors.append("execution.mode=cloud but no 'server' block declared")
        else:
            for key in _REQUIRED_SERVER:
                if key not in server:
                    errors.append(f"execution.server.{key} is required when mode=cloud")
            provider = server.get("provider")
            if provider not in _VALID_PROVIDERS:
                errors.append(
                    "execution.server.provider must be one of "
                    f"{_VALID_PROVIDERS}, got: '{provider}'"
                )
    
    # Job rquired fields
    job = cfg.get("job", {})
    for key in _REQUIRED_JOB:
        if key not in job:
            errors.append(f"job.{key} is required")
    
    # Pipeline required fields
    pipeline = cfg.get("pipeline", {})
    for key in _REQUIRED_PIPELINE:
        if key not in pipeline:
            errors.append(f"pipeline.{key} is required")
    
    position = pipeline.get("position")
    if position is not None and (not isinstance(position, int) or position < 1):
        errors.append(f"pipeline.position must be a positive integer (>=1), got: '{position}'")
        
    chunk_threshold = pipeline.get("chunk_threshold", 10)
    if not isinstance(chunk_threshold, int) or chunk_threshold < 1:
        errors.append(f"pipeline.chunk_threshold must be a positive integer, got '{chunk_threshold}'")
    
    max_parallel = pipeline.get("max_parallel_chunk", 4)
    if not isinstance(max_parallel, int) or max_parallel < 1:
        errors.append(f"pipeline.max_parallele_chunks must be a positive integer, got: '{max_parallel}'")
    
    # Schedule - basic cron validation (5 fields)
    schedule = cfg.get("schedule", "")
    if schedule and len(schedule.split()) != 5:
        errors.append(
            f"schedule must be a valid 5-field cron expression, got: '{schedule}'"
        )
    
    if errors:
        raise ValueError(
            f"{source}:\n" + "\n".join(f"   - {e}" for e in errors)
        )
        
# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load_job_config(config_path: Path) -> AirflowJobConfig:
    """
    Load, resolve placeholders, validate, and parse one airflow_job.yml

    Args:
        config_path (Path): absolute path to the airflow_job.yml file

    Returns:
        AirflowJobConfig: dataclass
    
    Raises:
        FileNotFoundError: if the file does not exist
        ValueError: if required fields are missing or invalid 
    """
    if not config_path.exists():
        raise FileNotFoundError(f"airflow_job.yml not found: {config_path}")
    
    raw = config_path.read_text(encoding="utf-8")
    
    # Resolve {{ PLACEHOLDERS }} before parsing YAML
    raw = _resolve_placeholders(raw)
    
    cfg = yaml.safe_load(raw)
    
    if not isinstance(cfg, dict):
        raise ValueError(f"{config_path}: file is empty or not valid YAML")
    
    _validate(cfg, config_path)
    
    # Build dataclasses
    execution_raw = cfg["execution"]
    server_raw    = execution_raw.get("server")
    
    server = None
    if server_raw:
        server = ServerConfig(
            provider=               server_raw["provider"],
            instance_type=          server_raw["instance_type"],
            region=                 server_raw["region"],
            ami_id=                 server_raw["ami_id"],
            subnet_id=              server_raw["subnet_id"],
            security_group_id=      server_raw["security_group_id"],
            iam_instance_profile=   server_raw["iam_instance_profile"],
            startup_timeout_minutes=server_raw.get("startup_timeout_minutes", 10),
            force_terminate=        server_raw.get("force_terminate", False),
        )
    
    execution = ExecutionConfig(mode=execution_raw["mode"], server=server)
    
    job_raw = cfg["job"]
    job = JobConfig(
        module=           job_raw["module"],
        entry_point=      job_raw["entry_point"],
        config_path=      job_raw["config_path"],
        base_output_name= job_raw.get("base_output_name")
    ) 
    
    pipeline_raw = cfg["pipeline"]
    pipeline = PipelineConfig(
        position=            pipeline_raw["position"],
        upstream_jobs=       pipeline_raw.get("upstream_jobs", []),
        is_singleton=        pipeline_raw.get("is_singleton", False),
        chunk_threshold=     pipeline_raw.get("chunk_threshold", 10),
        max_parallel_chunks= pipeline_raw.get("max_parallel_chunks", 4)
    )
    
    retry_raw = cfg.get("retry", {})
    retry = RetryConfig(
        attempts=      retry_raw.get("attempts",      1),
        delay_minutes= retry_raw.get("delay_minutes", 5),
    )
    
    alerts_raw = cfg.get("alerts", {})
    alerts = AlertConfig(
        on_failure= alerts_raw.get("on_failure", True),
        on_retry=   alerts_raw.get("on_retry",   False),
        on_success= alerts_raw.get("on_success", False),
    )
    
    return AirflowJobConfig(
        airflow_id=     cfg["airflow_id"],
        dag_id=         cfg["dag_id"],
        description=    cfg.get("description", ""),
        schedule=       cfg["schedule"],
        timezone=       cfg.get("timezone", "Europe/Paris"),
        enabled=        cfg.get("enabled", True),
        execution=      execution,
        job=            job,
        pipeline=       pipeline,
        retry=          retry,
        alerts=         alerts,
        source_path=    config_path,
    )
    
def scan_job_configs(
    deployments_base: Path,
    airflow_id:       str | None = None,
) -> list[AirflowJobConfig]:
    """
    Scan all deployed modules for airflow_job.yml file

    Args:
        deployments_base (Path): root of deployments directory (~/deployments)
        airflow_id (str | None, optional): if provided, only return configs matching this airflow_id
                                           if None, return all enabled configs

    Returns:
        list[AirflowJobConfig]: only enabled ones, unsorted
        Sorting by cron is done in dag_factory.py
    """
    
    pattern = "*/airflow/airflow_job.yml"
    configs = []
    
    for config_path in sorted(deployments_base.glob(pattern)):
        try:
            cfg = load_job_config(config_path)
            
            if not cfg.enabled:
                print(f"[SKIP] {config_path} - disabled")
                continue 
            
            if airflow_id is not None and cfg.airflow_id != airflow_id:
                continue
            
            configs.append(cfg)
            print(f"[OK] loaded: {cfg.dag_id} ({cfg.job.module}) position={cfg.pipeline.position} - {cfg.execution.mode}")
        
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
            
        except ValueError as e:
            print(f"[ERROR] invalid config at {config_path}:\n{e}")
            
        except Exception as e:
            print(f"[ERROR] unexpected error loading {config_path}; {e}")
            
    return configs