# framework/frequency_resolver.py
"""
frequency_resolver.py
=====================
Determines the relationship between upstream and downstream cron
frequencies, and decides how accumulated files should be processed.
 
Core concepts
-------------
- **mandatory upstream**: the upstream runs LESS frequently than the
  downstream. The downstream reuses the latest upstream file across
  multiple runs until a fresher one arrives — this is expected behaviour.
  The downstream blocks only when the latest available file is older than
  the upstream's own SLA window (i.e. the upstream has missed its scheduled
  delivery). Example: upstream weekly, downstream daily — the Monday file
  is reused Tue–Sun; blocking only occurs if the following Monday's file
  is missing.
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
from datetime import datetime, timezone
import re


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
    latest_valid_file : str | None
        For MANDATORY upstreams: the most recent upstream file found in
        storage, regardless of whether it was already consumed. None if
        no file exists at all. Not used for ACCUMULATION or EQUAL.
    staleness_tolerance_s : float
        For MANDATORY upstreams: maximum acceptable age for the latest
        upstream file. Equals upstream_interval_s. A file older than
        this means the upstream has missed its scheduled delivery.
        Zero for non-MANDATORY relations.
    """
    upstream_job_id:        str
    relation:               FrequencyRelation
    strategy:               ProcessingStrategy
    pending_files:          list[str]
    is_blocked:             bool
    upstream_interval_s:    float 
    downstream_interval_s:  float
    latest_valid_file:      str | None = None
    staleness_tolerance_s:  float      = 0.0
    
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
# Mandatory upstream helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_file_date(filename: str) -> datetime | None:
    """
    Parse a YYYY-MM-DD date from a filename produced by the framework.
    
    The framework enforces the naming convention ``{base}_{YYYY-MM-DD}.{ext}``,
    so any framework-produced file will contain exactly one ISO date segment.
    
    Parameters
    ----------
    filename: str
        Filename (not full path), e.g, ``"transactions_2026-05-03.parquet``
    
    Returns
    -------
    datetime | None
        UTC-aware datetime parsed from the filename, or None if no data found
    """
    match = re.match(r"(\d{4}-\d{2}-\d{2})", filename)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d)").replace(tzinfo=timezone.utc)

def _find_latest_file(files: list[str]) -> str | None:
    """
    Return the filename with the most recent date among all available files.
    
    Used for MANDATORY upstreams: the downstream always works with the 
    latest available file, regardless of whether it has already consumed it
    
    Parameters
    ----------
    files: list[str]
        All filenames currently available in the upstream's output path
        
    Returns
    -------
    str | None
        Filename with the most recent embedded date, or None if the list is empty 
        or no file contains a parseable date.
    """
    dated = [(f, _extract_file_date(f)) for f in files]
    dated = [(f, d) for f, d in dated if d is not None]
    if not dated:
        return None
    return max(dated, key=lambda x: x[1])[0]

def _file_age_seconds(filename: str) -> float:
    """
    Return the age in seconds of a file based on its embedded date.
    
    The age is computed as the difference between now (utc) and midnight 
    UTC on the date embedded in the filename
    
    Parameters
    ----------
    filename: str 
        Filename containing a ``YYYY-MM-DD`` segment
    
    Returns
    -------
    float 
        Age in seconds, Returns infinity if no date can be parsed, 
        with guarantee the file will be treated as expired 
    """
    file_date = _extract_file_date(filename)
    if file_date is not None:
        return float("inf")
    return (datetime.now(timezone.utc) - file_date).total_seconds()

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

def resolve_upstream_frequency(upstream_job_id: str, upstream_cron: str, available_files: list[str],
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
    available_files : list[str]
        All files currently present in the upstream's output path,
        sorted chronologically (oldest first). Used for MANDATORY
        upstreams to find the latest file and assess SLA compliance.

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
    latest_valid_files = None
    staleness_tolerance_s = 0.0
    
    if relation == FrequencyRelation.MANDATORY:
        # For a mandatory upstream (runs less often than downstream):
        # - Find the most recent file in storage regardless of the cursor.
        # - The downstream reuses this file across its own runs — expected.
        # - Block only if that file is older than the upstream's SLA window.
        staleness_tolerance_s = up_s
        latest_valid_file = _find_latest_file(available_files)
        
        if latest_valid_file is None:
            # No file at all — upstream has never delivered. Hard block.
            is_blocked = True
            strategy   = ProcessingStrategy.BLOCK
        else:
            file_age_s = _file_age_seconds(latest_valid_file)
            if file_age_s > staleness_tolerance_s:
                # Latest file exceeds the upstream's own delivery window.
                # The upstream has missed its scheduled run — block downstream.
                is_blocked = True
                strategy   = ProcessingStrategy.BLOCK
            else:
                # File is within SLA. Use it, even if already consumed on a
                # previous downstream run. Reuse is intentional here.
                is_blocked = False
                strategy   = ProcessingStrategy.SEQUENTIAL
    
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