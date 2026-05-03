# framework/frequency_resolver.py
"""
frequency_resolver.py
=====================
Determines the relationship between upstream and downstream cron
frequencies, and decides how accumulated files should be processed.

Core concepts
-------------
- **mandatory upstream**: the upstream runs LESS frequently than the
  downstream. If the upstream has not produced a new file since its
  last scheduled run, the downstream is blocked (cannot use stale
  data as a substitute for missing data).
- **accumulation upstream**: the upstream runs MORE frequently than
  the downstream. Multiple files may have accumulated since the last
  downstream run. Two processing strategies apply:

  - **sequential**: if the number of pending files <= ``chunk_threshold``,
    files are processed one by one in chronological order.
  - **parallel chunks**: if pending files > ``chunk_threshold``, files
    are split into chunks and processed in parallel via Airflow native
    workers.

Design note
-----------
Frequency comparison is done by comparing cron interval durations in
seconds using ``croniter``. This is a heuristic — cron expressions
like ``0 0 * * 1-5`` (weekdays only) will be approximated. For the
use cases this framework targets (daily, weekly, hourly schedules),
this approximation is accurate enough.

References
----------
- croniter: https://github.com/kiorky/croniter
- Airflow dynamic task mapping: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
"""
from __future__ import annotations 

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from croniter import croniter 

if TYPE_CHECKING:
    from framework.config_loader import AirflowJobConfig
    
# ─────────────────────────────────────────────────────────────────────────────
# Enums and result dataclasses
# ─────────────────────────────────────────────────────────────────────────────

class FrequencyRelation(str, Enum):
    """Relationship between upstream and downstream cron frequencies"""
    MANDATORY    = "mandatory"
    ACCUMULATION = "accumulation"
    EQUAL        = "equal"

class ProcessingStrategy(str, Enum):
    """How accumulated files should be processed"""
    SEQUENTIAL = "sequential"
    PARALLEL   = "parallel"
    BLOCK      = "block"

@dataclass
class FrequencyResolution:
    """
    Result of comparing an upstream job's frequency to the current job.

    Attributes
    ----------
    upstream_job_id : str
        The airflow_id of the upstream being evaluated.
    relation : FrequencyRelation
        Whether the upstream is mandatory, accumulating, or equal.
    strategy : ProcessingStrategy
        Processing strategy to apply for this upstream's files.
    pending_files : list[str]
        Files available for processing, sorted chronologically.
        Empty when relation is MANDATORY and no new file is available.
    is_blocked : bool
        True when a mandatory upstream has no new file.
        The downstream task should fail-fast in this case.
    upstream_interval_s : float
        Upstream cron interval in seconds (approximate).
    downstream_interval_s : float
        Downstream cron interval in seconds (approximate).
    """
    upstream_job_id:        str
    relation:               FrequencyRelation
    strategy:               ProcessingStrategy
    pending_files:          list[str]
    is_blocked:             bool
    upstream_interval_s:    float 
    downstream_interval_s:  float
    
    def __repr__(self) -> str:
        return(
            f"FrequencyResolution("
            f"upstream={self.upstream_job_id!r}, "
            f"relation={self.relation.value}, "
            f"strategy={self.strategy.value}, "
            f"pending={len(self.pending_files)} file(s), "
            f"blocked={self.is_blocked})"
        )
        

# ─────────────────────────────────────────────────────────────────────────────
# Cron interval helpers
# ─────────────────────────────────────────────────────────────────────────────

def cron_interval_seconds(cron_expr: str) -> float:
    """
    Estimate the average interval in seconds between two cron fires.

    Uses croniter to compute two consecutive trigger times and returns
    their difference. This is an approximation — expressions with
    weekday or month constraints will not be perfectly accurate.

    Parameters
    ----------
    cron_expr : str
        A valid 5-field cron expression, e.g. ``"0 2 * * *"``.

    Returns
    -------
    float
        Interval in seconds between two consecutive triggers.

    Raises
    ------
    ValueError
        If the cron expression is invalid.

    Examples
    --------
    >>> cron_interval_seconds("0 * * * *")   # hourly
    3600.0
    >>> cron_interval_seconds("0 0 * * *")   # daily
    86400.0
    >>> cron_interval_seconds("0 0 * * 0")   # weekly
    604800.0
    """
    try:
        it = croniter(cron_expr)
        t1 = it.get_next(float)
        t2 = it.get_next(float)
        return t2 - t1
    except Exception as e:
        raise ValueError(f"Invalid cron expression'{cron_expr}': {e}") from e
    
def compare_frequencies(upstream_cron: str, downstream_cron: str) -> tuple[FrequencyRelation, float, float]:
    """
    Compare two cron frequencies and return their relationship.

    Parameters
    ----------
    upstream_cron : str
        Cron expression of the upstream job.
    downstream_cron : str
        Cron expression of the current (downstream) job.

    Returns
    -------
    tuple[FrequencyRelation, float, float]
        (relation, upstream_interval_s, downstream_interval_s)
    """
    up_s = cron_interval_seconds(upstream_cron)
    down_s = cron_interval_seconds(downstream_cron)
    
    if up_s > down_s:
        relation = FrequencyRelation.MANDATORY
    elif up_s < down_s:
        relation = FrequencyRelation.ACCUMULATION
    else:
        relation = FrequencyRelation.EQUAL
    return relation, up_s, down_s

# ─────────────────────────────────────────────────────────────────────────────
# File splitting for parallel processing
# ─────────────────────────────────────────────────────────────────────────────
def split_into_chunks(files: list[str], chunk_size: int) -> list[list[str]]:
    """
    Split a list of files into equal-sized chunks for parallel processing.

    The last chunk may be smaller than ``chunk_size`` if the total
    number of files is not evenly divisible.

    Parameters
    ----------
    files : list[str]
        Filenames to split, expected in chronological order.
    chunk_size : int
        Maximum number of files per chunk. Must be >= 1.

    Returns
    -------
    list[list[str]]
        List of chunks, each being a list of filenames.

    Examples
    --------
    >>> split_into_chunks(["a", "b", "c", "d", "e"], chunk_size=2)
    [['a', 'b'], ['c', 'd'], ['e']]
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    return [files[i: 1 + chunk_size] for i in range (0, len(files), chunk_size)]

# ─────────────────────────────────────────────────────────────────────────────
# Main resolution function
# ─────────────────────────────────────────────────────────────────────────────

def resolve_upstream_frequency(upstream_job_id: str, upstream_cron: str, 
                               downstream_cron: str, pending_files: list[str],
                               chunk_threshold: int) -> FrequencyResolution:
    """
    Determine how a downstream job should handle its upstream's files.

    Parameters
    ----------
    upstream_job_id : str
        Identifying name of the upstream (for logging and XCom).
    upstream_cron : str
        Cron expression of the upstream job.
    downstream_cron : str
        Cron expression of the current (downstream) job.
    pending_files : list[str]
        Files produced by the upstream since the downstream's last run,
        sorted chronologically (oldest first).
    chunk_threshold : int
        Files above this count trigger parallel chunk processing.

    Returns
    -------
    FrequencyResolution
        Full resolution result including processing strategy.

    Examples
    --------
    >>> # Upstream weekly, downstream daily → mandatory
    >>> resolve_upstream_frequency(
    ...     "ingestor", "0 0 * * 0", "0 1 * * *", [], chunk_threshold=10
    ... )
    FrequencyResolution(upstream='ingestor', relation=mandatory, strategy=block, ...)

    >>> # Upstream hourly, downstream daily → accumulation
    >>> resolve_upstream_frequency(
    ...     "ingestor", "0 * * * *", "0 0 * * *",
    ...     ["f_2026-05-03.parquet", "f_2026-05-04.parquet"], chunk_threshold=10
    ... )
    FrequencyResolution(upstream='ingestor', relation=accumulation, strategy=sequential, ...)
    """
    relation, up_s, down_s = compare_frequencies(upstream_cron, downstream_cron)
    
    is_blocked = False
    strategy   = ProcessingStrategy.SEQUENTIAL
    
    if relation == FrequencyRelation.MANDATORY:
        if not pending_files:
            # Upstream has not produced since its last scheduled run.
            # The downstream must block — stale data is not acceptable
            # for a mandatory upstream.
            is_blocked = True
            strategy   = ProcessingStrategy.BLOCK
        else:
            # Upstream produced exactly one new file (expected for
            # a less-frequent upstream). Process it sequentially.
            strategy = ProcessingStrategy.SEQUENTIAL
    
    elif relation == FrequencyRelation.ACCUMULATION:
        if len(pending_files) > chunk_threshold:
            strategy = ProcessingStrategy.PARALLEL
        else:
            strategy = ProcessingStrategy.SEQUENTIAL
    
    else:
        strategy = ProcessingStrategy.SEQUENTIAL
    
    return FrequencyResolution(
        upstream_job_id=        upstream_job_id,
        relation=               relation,
        strategy=               strategy,
        pending_files=          pending_files,
        is_blocked=             is_blocked,
        upstream_interval_s=    up_s,
        downstream_interval_s=  down_s,
    )