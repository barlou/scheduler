# framework/pipeline_state.py
"""
pipeline_state.py
=================
Persistent per-pipeline consumption state for upstream file tracking.

Design
------
Each (pipeline_id, upstream_job_id) pair maintains its own cursor:
the last file it consumed from the upstream. This allows multiple
pipelines to share the same upstream job without stepping on each
other — no file copy, no coupling between pipelines.

The state is serialised as JSON and persisted to cloud storage via
the framework's CloudClient abstraction, so it survives Airflow
restarts and worker recycling.

State lifecycle
---------------
1. Preflight task calls ``PipelineStateManager.load()``
2. Framework compares ``last_consumed_file`` against files currently
   available from the upstream to determine:
   - ``mandatory`` upstreams  → block if no new file
   - ``accumulation`` upstreams → collect files since last consumption
3. After successful processing, ``PipelineStateManager.save()`` is
   called to advance the cursor.

References
----------
- Airflow XCom docs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
"""
from __future__ import annotations 

import json 
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any 

# ─────────────────────────────────────────────────────────────────────────────
# State dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PipelineConsumptionState:
    """
    Cursor tracking which upstream files a pipeline has already consumed.

    Attributes
    ----------
    pipeline_id : str
        The ``airflow_id`` of the consuming pipeline (downstream).
    upstream_job_id : str
        The ``airflow_id`` of the upstream pipeline being tracked.
    last_consumed_file : str
        Filename (not full path) of the last file successfully consumed.
        Empty string means the pipeline has never run.
    last_consumed_at : datetime | None
        UTC timestamp of the last successful consumption.
        None if the pipeline has never run.
    files_pending : list[str]
        Files produced by the upstream that this pipeline has not yet
        consumed. Populated by the preflight task and cleared on success.
    """
    pipeline_id:        str
    upstream_job_id:    str
    last_consumed_file: str                 = ""
    last_consumed_at:   datetime | None     = None
    files_pending:      list[str]           = field(default_factory=list)
    
    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-safe dict"""
        return {
            "pipeline_id":          self.pipeline_id,
            "upstream_job_id":      self.upstream_job_id,
            "last_consumed_file":   self.last_consumed_file,
            "last_consumed_at":     {
                self.last_consumed_at.isoformat()
                if self.last_consumed_at else None
            },
            "files_pending": self.files_pending,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineConsumptionState":
        """Deserialize from a dict (loaded from JSON)"""
        last_consumed_at = None
        raw_ts = data.get("last_consumed_at")
        if raw_ts:
            last_consumed_at = datetime.fromisoformat(raw_ts)
            if last_consumed_at.tzinfo is None:
                last_consumed_at = last_consumed_at.replace(tzinfo=timezone.utc)
        return cls(
            pipeline_id=        data["pipeline_id"],
            upstream_job_id=    data["upstream_job_id"],
            last_consumed_file= data.get("last_consumed_file", ""),
            last_consumed_at=   last_consumed_at,
            files_pending=      data.get("files_pending", []),
        )
        
    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    @property
    def has_run(self) -> bool:
        """True if this pipeline has consumed at least one file"""
        return bool(self.last_consumed_file)

    @property
    def state_key(self) -> str:
        """Unique storage key for this (pipeline, upstream) pair."""
        return f"{self.pipeline_id}_{self.upstream_job_id}"

    def mark_consumed(self, filename: str) -> None:
        """
        Advanced the cursor after successful processing.
        
        Parameters
        ----------
        filename: str
            The filename that was just successfully consumed.
        """
        self.last_consumed_file = filename
        self.last_consumed_at   = datetime.now(timezone.utc)
        self.files_pending      = []
        
    def __repr__(self) -> str:
        return(
            f"PipelineConsumptionState("
            f"pipeline={self.pipeline_id!r}, "
            f"upstream={self.upstream_job_id!r}, "
            f"last={self.last_consumed_file!r}, "
            f"pending={len(self.files_pending)} file(s))"
        )

# ─────────────────────────────────────────────────────────────────────────────
# State key builder
# ─────────────────────────────────────────────────────────────────────────────

def _state_remote_key(pipeline_id:str, upstream_job_id: str) -> str:
    """
    Build the cloud storage key for a (pipeline, upstream) state file.
    Pattern: ``framework/state/{pipeline_id}_{upstream_job_id}.json``
    """

# ─────────────────────────────────────────────────────────────────────────────
# State manager
# ─────────────────────────────────────────────────────────────────────────────

class PipelineStateManager:
    """
    Load and persist PipelineConsumptionState to/from cloud storage.

    Uses the framework's CloudClientBase abstraction so the storage
    backend (S3, GCS, OVH Object Storage, …) is interchangeable.

    Parameters
    ----------
    cloud_client : CloudClientBase
        An initialised cloud client from the ``tools`` library.
    local_cache_dir : Path
        Directory for temporary local JSON files before upload.
        Defaults to ``/tmp/framework_state/``.

    Examples
    --------
    >>> from cloud_client import CloudClientFactory
    >>> client  = CloudClientFactory.s3("my-bucket")
    >>> manager = PipelineStateManager(client)
    >>> state   = manager.load("my_pipeline", "upstream_ingestor")
    >>> state.files_pending = ["transactions_2026-05-03.parquet"]
    >>> manager.save(state)
    """
    
    def __init__(self, cloud_client: Any, local_cache_dir: Path | None = None) -> None:
        self._client    = cloud_client
        self._cache_dir = local_cache_dir or Path("/tmp/framework_state")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def load(self, pipeline_id: str, upstream_job_id: str) -> PipelineConsumptionState:
        """
        Load state for a (pipeline, upstream) pair from cloud storage.

        Returns a fresh empty state if no persisted state is found
        (first run or state was never saved).

        Parameters
        ----------
        pipeline_id : str
            Consuming pipeline's airflow_id.
        upstream_job_id : str
            Upstream pipeline's airflow_id.

        Returns
        -------
        PipelineConsumptionState
        """
        remote_key = _state_remote_key(pipeline_id, upstream_job_id)
        local_path = self._cache_dir / f"{pipeline_id}_{upstream_job_id}.json"
        
        try:
            self._client.download(remote_key, local_path)
            data = json.loads(local_path.read_text(encoding="utf-8"))
            state = PipelineConsumptionState.from_dict(data)
            print(f"[state] loaded: {state}")
            return state
        except Exception as e:
            print(f"[state] no existing state for ({pipeline_id}, {upstream_job_id}): {e}")
            return PipelineConsumptionState(
                pipeline_id, upstream_job_id
            )
            
    def save(self, state: PipelineConsumptionState) -> bool:
        """
        Persist state to cloud storage.
        
        Parameters
        ----------
        state : PipelineConsumptionState
            Updated state to persist 
        
        Returns
        -------
        bool
            True on success, False on failure (logged, never raises)
        """
        remote_key = _state_remote_key(state.pipeline_id, state.upstream_job_id)
        local_path = self._cache_dir / f"{state.state_key}.json"
        
        try:
            local_path.write_text(
                json.dumps(state.to_dict(), indent=2, ensure_ascli=False),
                encoding="utf-8"
            )
            result = self._client.upload(local_path, remote_key)
            print(f"[state] saved: {state}")
            return result 
        except Exception as e:
            print(f"[state] save failed for {state.state_key}: {e}")
            return False

# ─────────────────────────────────────────────────────────────────────────────
# Shared upstream detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_shared_upstreams(all_configs: list[Any]) -> dict[str, list[str]]:
    """
    Detect upstream jobs that are shared across multiple pipelines.

    Scans all loaded configs and returns a mapping of
    ``upstream_job_id → [pipeline_ids that depend on it]``.
    Only entries with 2+ consumers are returned.

    This is called automatically at DAG build time. No manual
    declaration is needed — sharing is inferred from
    ``pipeline.upstream_jobs`` declarations.

    Parameters
    ----------
    all_configs : list[AirflowJobConfig]
        All loaded configs from ``scan_job_configs()``.

    Returns
    -------
    dict[str, list[str]]
        Keys are upstream airflow_ids. Values are lists of pipeline
        airflow_ids that declared a dependency on that upstream.
        Only upstreams with 2+ consumers are included.

    Examples
    --------
    >>> shared = detect_shared_upstreams(configs)
    >>> for upstream_id, consumers in shared.items():
    ...     print(f"{upstream_id} is shared by: {consumers}")
    ingestor is shared by: ['pipeline_a', 'pipeline_b']
    """
    usage: dict[str, list[str]] = {}
    
    for cfg in all_configs:
        for upstream_id in cfg.pipeline.upstream_jobs:
            usage.setdefault(upstream_id, [])
            if cfg.airflow_id not in usage[upstream_id]:
                usage[upstream_id].append(cfg.airflow_id)
    
    shared = {k: v for k, v in usage.items() if len(v) > 1}
    
    if shared:
        print("[pipeline_state] Shared upstreams detected:")
        for upstream_id, consumers in shared.items():
            print(f"    {upstream_id!r} -> consumed by {consumers}")
    else:
        print("[pipeline_state] No shared upstreams detected.")
    
    return shared 