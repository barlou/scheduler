# Technical Design — Airflow Orchestration Framework

> Internal reference. Covers architecture decisions, data flow, module contracts, and implementation choices.  
> For user-facing documentation, see `Readme.md`.

---

## Table of contents

1. [Design philosophy](#1-design-philosophy)
2. [Module map](#2-module-map)
3. [Breaking changes from v1](#3-breaking-changes-from-v1)
4. [config_loader.py](#4-config_loaderpy)
5. [segment_resolver.py](#5-segment_resolverpy)
6. [pipeline_state.py](#6-pipeline_statepy)
7. [frequency_resolver.py](#7-frequency_resolverpy)
8. [freshness.py](#8-freshnesspy)
9. [dag_factory.py](#9-dag_factorypy)
10. [Data flow — end to end](#10-data-flow--end-to-end)
11. [File naming convention](#11-file-naming-convention)
12. [Shared upstream handling](#12-shared-upstream-handling)
13. [Frequency strategies in detail](#13-frequency-strategies-in-detail)
14. [Testing strategy](#14-testing-strategy)
15. [Known limitations and future work](#15-known-limitations-and-future-work)

---

## 1. Design philosophy

**Declarative over imperative.** Every pipeline behaviour is expressed in `airflow_job.yml`. The framework code is generic — no pipeline-specific logic lives in the framework itself.

**Explicit over inferred.** Execution order is now declared via `pipeline.position` (integer). The previous v1 behaviour inferred order from cron schedule times, which was fragile and unintuitive.

**Fail fast on bad data.** Mandatory upstream freshness failures raise immediately. The framework never processes a downstream job on data it cannot confirm is fresh.

**State per pipeline, not global.** Each `(pipeline_id, upstream_job_id)` pair maintains its own consumption cursor. Two pipelines consuming the same upstream never interfere with each other.

**Cloud-agnostic throughout.** Every cloud interaction goes through `CloudClientBase`. The framework itself has no direct dependency on boto3, google-cloud, or any provider SDK.

---

## 2. Module map

```
framework/
├── config_loader.py       AirflowJobConfig, PipelineConfig, build_output_path, extract_file_date
├── segment_resolver.py    ExecutionSegment, sort_by_position, validate_singleton, build_pipeline_segments
├── pipeline_state.py      PipelineConsumptionState, PipelineStateManager, detect_shared_upstreams
├── frequency_resolver.py  FrequencyRelation, ProcessingStrategy, resolve_upstream_frequency, cron_interval_seconds
├── freshness.py           freshness_check_task (Airflow callable)
├── dag_factory.py         build_all_dags, _build_dag
├── venv_manager.py        (unchanged from v1)
└── executors/
    ├── base_executor.py   TaskResult, BaseExecutor
    ├── local_executor.py  LocalExecutor (unchanged)
    └── cloud_executor.py  CloudExecutor (unchanged)
```

---

## 3. Breaking changes from v1

| Area | v1 behaviour | v2 behaviour |
|---|---|---|
| Execution ordering | Sorted by cron schedule time (earlier cron = runs first) | **Sorted by `pipeline.position`** (explicit integer, 1-based) |
| `airflow_job.yml` | No `pipeline:` block | **`pipeline.position` is now required** |
| `sort_by_cron()` | Public function in segment_resolver | **Removed** — replaced by `sort_by_position()` |
| `_cron_sort_key()` | Internal helper | **Removed** |
| Output file naming | No convention enforced | **`build_output_path()` convention** — `{base_name}_{YYYY-MM-DD}.{ext}` |

Migration path for existing `airflow_job.yml` files:

```yaml
# Add this block to every existing airflow_job.yml
pipeline:
  position: 1   # assign positions matching the old cron order
```

---

## 4. config_loader.py

### New: `PipelineConfig`

```python
@dataclass
class PipelineConfig:
    position:            int           # required, >= 1
    upstream_jobs:       list[str]     # default []
    is_singleton:        bool          # default False
    chunk_threshold:     int           # default 10
    max_parallel_chunks: int           # default 4
```

`position` replaces cron-based ordering. It must be unique within a pipeline — `_validate()` raises `ValueError` on duplicates.

### New: `JobConfig.base_output_name`

Optional field. When set, the framework uses `build_output_path(base_output_name, ext)` to name output files. If not set, the job handles its own output naming (backwards compatible with v1).

### New: `build_output_path(base_name, extension) -> str`

Injects UTC date into the filename:

```
build_output_path("transactions", ".parquet") → "transactions_2026-05-03.parquet"
```

The date is always UTC. Changing the timezone here would break freshness detection across environments in different timezones.

### New: `extract_file_date(filename) -> datetime | None`

Parses the `YYYY-MM-DD` pattern from a filename. Returns a UTC-aware datetime on success, `None` on failure.

Uses regex `(\d{4}-\d{2}-\d{2})` — matches the first date-like pattern found. If a filename contains multiple dates (e.g. `backup_2026-01-01_copy_2026-05-03.parquet`), the first one wins. This is intentional — the production date is always the first date in the name by convention.

---

## 5. segment_resolver.py

### `sort_by_position(configs) -> list[AirflowJobConfig]`

Sorts by `cfg.pipeline.position` ascending. Raises `ValueError` if any two configs within the input share the same position.

Position uniqueness is validated at sort time, not at load time, because `scan_job_configs()` returns configs across pipelines — duplicates within the same pipeline are only detectable when grouped.

### `validate_singleton(configs)`

Called before `sort_by_position`. A pipeline is singleton if any of its configs has `pipeline.is_singleton = True`. If singleton and `len(configs) > 1`, raises `ValueError`.

The singleton constraint is enforced at DAG build time (inside `build_pipeline_segments`), not at config load time. This is intentional — a config file cannot know whether other configs will share its `airflow_id` until all are loaded.

### Segment resolution rules (unchanged from v1)

See inline docstring in `resolve_segments()`. The algorithm itself is unchanged — only the input ordering changed (position-based instead of cron-based).

---

## 6. pipeline_state.py

### `PipelineConsumptionState`

```python
@dataclass
class PipelineConsumptionState:
    pipeline_id:         str
    upstream_job_id:     str
    last_consumed_file:  str            # "" if never run
    last_consumed_at:    datetime|None  # UTC
    files_pending:       list[str]      # populated by freshness_check_task
```

Keyed by `(pipeline_id, upstream_job_id)`. Two pipelines consuming the same upstream each have their own independent state.

State is serialised as JSON and stored at `framework/state/{pipeline_id}__{upstream_job_id}.json` in cloud storage. Local cache at `/tmp/framework_state/` for the duration of the Airflow task.

### `PipelineStateManager`

```python
manager = PipelineStateManager(cloud_client)
state   = manager.load("pipeline_a", "upstream_ingestor")  # never raises
manager.save(state)                                         # returns bool, never raises
```

`load()` returns an empty default state on first run or if the remote file doesn't exist. Failures are logged and swallowed — the framework treats a missing state as "never run" rather than failing the DAG.

`save()` returns `False` on failure and logs the error. It does not raise. This is intentional: a state save failure should not fail the DAG run that already successfully processed data.

### `detect_shared_upstreams(all_configs) -> dict[str, list[str]]`

Scans all configs and returns a mapping of `upstream_id → [pipeline_ids]`. Only entries with 2+ consumers are returned.

Called automatically at DAG build time in `dag_factory.py` for informational logging. No action is taken based on the result — each pipeline's `PipelineConsumptionState` handles isolation.

---

## 7. frequency_resolver.py

### `cron_interval_seconds(cron_expr) -> float`

Computes two consecutive cron trigger times using `croniter` and returns their difference.

**Approximation note:** expressions with weekday or month constraints (`0 0 * * 1-5`, `0 0 1 * *`) will return the interval between two consecutive triggers, which may not represent the average interval accurately. For the scheduling patterns targeted by this framework (hourly, daily, weekly), the approximation is accurate within seconds.

### `FrequencyRelation` enum

```python
class FrequencyRelation(str, Enum):
    MANDATORY    = "mandatory"     # upstream slower → blocks if no new file
    ACCUMULATION = "accumulation"  # upstream faster → files accumulate
    EQUAL        = "equal"         # same frequency
```

### `ProcessingStrategy` enum

```python
class ProcessingStrategy(str, Enum):
    SEQUENTIAL = "sequential"   # file by file
    PARALLEL   = "parallel"     # chunked, via Airflow workers
    BLOCK      = "block"        # mandatory upstream, no new file
```

### `resolve_upstream_frequency(...) -> FrequencyResolution`

Decision table:

| Relation | Pending files | Strategy |
|---|---|---|
| MANDATORY | 0 | BLOCK (is_blocked=True) |
| MANDATORY | >= 1 | SEQUENTIAL |
| ACCUMULATION | <= chunk_threshold | SEQUENTIAL |
| ACCUMULATION | > chunk_threshold | PARALLEL |
| EQUAL | any | SEQUENTIAL |

---

## 8. freshness.py

### `freshness_check_task(...)`

Airflow task callable. Injected as the first task in every pipeline DAG that declares `upstream_jobs`.

**XCom output:** pushes `freshness_reports` key with a dict mapping `upstream_id → report dict`. Downstream processing tasks pull this via `ti.xcom_pull(key="freshness_reports")`.

**Blocking behaviour:** if `any_blocked is True` after evaluating all upstreams, raises `RuntimeError`. This marks the Airflow task as failed, which stops the DAG run via normal Airflow task dependency rules.

**File filtering logic:**

```python
# Files available from upstream
all_files = ["data_2026-05-01.parquet", "data_2026-05-02.parquet", "data_2026-05-03.parquet"]

# This pipeline last consumed
last_consumed = "data_2026-05-01.parquet"  # → date: 2026-05-01

# Pending = files with date > last_consumed date
pending = ["data_2026-05-02.parquet", "data_2026-05-03.parquet"]
```

If `last_consumed` has no parseable date (non-convention filename), all available files are treated as pending. This is a conservative fallback.

---

## 9. dag_factory.py

### Changes from v1

- `build_all_dags()` now calls `detect_shared_upstreams()` at startup and logs shared upstreams
- `_build_dag()` injects a `freshness_check` task as the first node when `pipeline.upstream_jobs` is non-empty
- Pipeline sorting is now done via `sort_by_position()` instead of `sort_by_cron()`

### DAG structure with freshness check

```
[freshness_check] → [segment_1_tasks] → ... → [pipeline_summary]
```

### DAG structure without upstream dependencies (unchanged)

```
[segment_1_tasks] → [segment_2_tasks] → ... → [pipeline_summary]
```

---

## 10. Data flow — end to end

```
1. Airflow scheduler heartbeat (every 30s)
   └─ dag_factory_loader.py calls build_all_dags()

2. build_all_dags()
   ├─ scan_job_configs()        → list[AirflowJobConfig]
   ├─ detect_shared_upstreams() → logs shared upstreams
   ├─ group by airflow_id
   └─ for each pipeline:
      ├─ validate_singleton()
      ├─ sort_by_position()
      ├─ resolve_segments()
      └─ _build_dag()           → DAG registered in Airflow

3. DAG triggered (scheduled or manual)
   ├─ freshness_check_task (if upstream_jobs declared)
   │   ├─ PipelineStateManager.load() for each upstream
   │   ├─ cloud_client.list() to get available files
   │   ├─ filter to unconsumed files
   │   ├─ resolve_upstream_frequency() for each upstream
   │   ├─ push freshness_reports to XCom
   │   └─ raise RuntimeError if any MANDATORY upstream is BLOCKED
   │
   ├─ segment tasks (local or cloud, per ExecutionSegment)
   │   ├─ pull freshness_reports from XCom
   │   ├─ for SEQUENTIAL: process files one by one
   │   ├─ for PARALLEL: split_into_chunks() → spawn workers
   │   └─ PipelineStateManager.save() on success
   │
   └─ pipeline_summary task (always runs via trigger_rule=all_done)
```

---

## 11. File naming convention

**Pattern:** `{base_output_name}_{YYYY-MM-DD}.{extension}`

**Responsibility:** `build_output_path()` in `config_loader.py`. Always UTC date.

**Why not cloud timestamps:** Cloud storage modification timestamps are unreliable:
- Re-uploading an identical file updates the timestamp even though content is unchanged
- Cloud-to-cloud copies produce a timestamp of the copy operation, not the original production date
- Different cloud providers handle timestamp precision differently

The date-in-filename convention is deterministic, provider-independent, and human-readable.

**Convention is mandatory within the framework** for any file that participates in freshness tracking. Jobs that produce files not tracked by freshness (intermediate outputs, debug files) may use any naming scheme.

---

## 12. Shared upstream handling

**Detection:** `detect_shared_upstreams()` runs at DAG build time. It is informational only — it logs which upstreams are shared but takes no action.

**Isolation:** Each `(pipeline_id, upstream_job_id)` pair has its own `PipelineConsumptionState`. Two pipelines consuming the same upstream maintain independent cursors.

**No file copy:** Files are not duplicated. Each pipeline reads from the same location in cloud storage but tracks its own last-consumed position. This is equivalent to a consumer group pattern in message queues.

**No inter-pipeline coupling:** `pipeline_a` does not need to know that `pipeline_b` also consumes `upstream_ingestor`. Adding a new consumer pipeline requires zero changes to existing pipelines.

---

## 13. Frequency strategies in detail

### MANDATORY (upstream less frequent)

The upstream runs less often than the downstream. Example: weekly source, daily transformation.

**Expected state when transformation runs:**
- If the upstream ran this week → one new file exists → process it (SEQUENTIAL)
- If the upstream has not run yet this week → no new file → BLOCK

The framework never uses the previous week's file as a substitute. A mandatory upstream that hasn't produced new data is a signal that the downstream should wait, not that it should reuse stale data.

### ACCUMULATION (upstream more frequent)

The upstream runs more often than the downstream. Example: hourly source, daily transformation.

**Expected state when transformation runs:**
- Up to 24 new files from the past 24 hours

If `pending_files <= chunk_threshold` → SEQUENTIAL (files processed one by one, oldest first).

If `pending_files > chunk_threshold` → PARALLEL:
- Files split into chunks via `split_into_chunks()`
- One Airflow task spawned per chunk (native Airflow worker, not new cloud instance)
- `max_parallel_chunks` limits concurrency

**Why a threshold instead of always parallelising:**
- Each file's overhead (cloud download, venv activation) dominates for small files
- Sequential is simpler and avoids Airflow task scheduling overhead for small batches
- The threshold is configurable per job — heavy files need a lower threshold than light files

### EQUAL (same frequency)

The upstream and downstream run at the same frequency. The framework expects exactly one new file per run and processes it sequentially. No special handling needed.

---

## 14. Testing strategy

Tests are co-located in `tests/` and follow a file-per-module naming convention:

| Test file | Covers |
|---|---|
| `test_config_loader.py` | Original v1 fields — unchanged |
| `test_config_loader_v2.py` | PipelineConfig, build_output_path, extract_file_date |
| `test_segment_resolver.py` | Original v1 segment logic — unchanged |
| `test_segment_resolver_v2.py` | Position ordering, singleton validation |
| `test_pipeline_state.py` | PipelineConsumptionState, detect_shared_upstreams |
| `test_frequency_resolver.py` | cron_interval_seconds, resolve_upstream_frequency |
| `test_base_executor.py` | TaskResult — unchanged |
| `test_venv_manager.py` | VenvManager — unchanged |
| `test_aws_provider.py` | AWS provider — unchanged |

Run all tests:

```bash
pytest tests/ -v
```

Run only new v2 tests:

```bash
pytest tests/test_config_loader_v2.py tests/test_segment_resolver_v2.py \
       tests/test_pipeline_state.py tests/test_frequency_resolver.py -v
```

---

## 15. Known limitations and future work

### Current limitations

**Cron interval approximation.** `cron_interval_seconds()` uses two consecutive trigger times. Expressions with weekday or month constraints produce approximations. For the current use cases (hourly, daily, weekly), this is accurate enough.

**State save is best-effort.** If `PipelineStateManager.save()` fails silently, the pipeline will reprocess already-consumed files on the next run. This is safe (idempotent processing is assumed) but inefficient. Future improvement: add a dead-letter mechanism for state save failures.

**Freshness check lists all files each run.** `cloud_client.list()` is called at runtime for each upstream. For upstreams that produce thousands of files, this listing can be slow. Future improvement: maintain a manifest file per upstream that is updated on each production.

**Parallel chunks use Airflow native workers, not new cloud instances.** For very large files, spawning new cloud instances per chunk would be more scalable. This is noted as a future improvement — the current implementation prioritises simplicity.

### Planned

- Streaming mode: replace static batch freshness check with a trigger-based approach (Airflow sensors watching for new files)
- Manifests: upstream jobs write a `manifest.json` alongside their output, making freshness detection O(1) instead of O(listing)
- GCP, Azure, OVH provider implementations
- Web UI for freshness status across all pipelines (Grafana dashboard backed by state files)
