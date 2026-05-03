# Airflow Orchestration Framework

![CI](https://github.com/barlou/scheduler/actions/workflows/ci.yml/badge.svg?branch=main)
![Release](https://img.shields.io/github/v/release/barlou/scheduler)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Security](https://img.shields.io/badge/security-gitleaks-red)

> **Schedule, coordinate, and scale your data workflows — without cloud functions, without infrastructure complexity, without endless refactoring.**

---

## What is this?

This is a custom orchestration framework built on top of [Apache Airflow](https://airflow.apache.org/). It turns Airflow into a full **workflow execution platform** that can:

- Run jobs locally or spin up cloud instances on demand
- Coordinate pipelines with different data refresh frequencies
- Detect when upstream data is fresh enough to process — and block when it isn't
- Handle file accumulation when an upstream produces faster than downstream consumes

One configuration file per project. Zero changes to the framework to add a new pipeline.

---

## The problem it solves

### For business stakeholders

| Problem | Without this framework | With this framework |
|---|---|---|
| Running a data pipeline | Manually trigger scripts or manage cron | Fully automated, scheduled, monitored |
| Scaling a heavy job | Pay for Lambda limits or manage EC2 manually | Automatically spins up the right server, runs the job, shuts it down |
| Visibility | No central view of what ran | One Airflow dashboard showing all pipelines |
| Cost control | Always-on compute or unpredictable billing | Pay only for exact compute time used |
| Coordinating pipelines with different frequencies | Manual coordination, missed runs | Framework detects frequency mismatches and handles them automatically |

### For technical teams

| Problem | Without this framework | With this framework |
|---|---|---|
| Dependency management | Global packages, version conflicts | Each module gets its own isolated virtual environment |
| Multi-step pipelines | Scripts calling scripts, fragile bash chains | Declarative step ordering via explicit position |
| Upstream data freshness | Manual checks, race conditions | Automated freshness check task at the start of every pipeline run |
| Shared upstreams | Risk of double-consumption between pipelines | Per-pipeline consumption state — no coupling, no file copy |
| Cloud provider lock-in | Hardcoded boto3 throughout the codebase | Provider abstraction layer — swap cloud provider in one line |

---

## How it works — for business stakeholders

Think of this framework as a **smart coordinator** that knows:

1. **What needs to run** — each project declares its jobs in a simple config file
2. **In what order** — an explicit position number determines priority within a pipeline
3. **Where to run it** — locally for lightweight jobs, on a fresh cloud instance for heavy compute
4. **Whether upstream data is ready** — checks freshness before processing; blocks if mandatory data is missing
5. **When to clean up** — cloud instances are terminated automatically when the job finishes

```
One Airflow instance manages everything:

  ┌─────────────────────────────────────────┐
  │           Airflow Dashboard             │
  ├─────────────────────────────────────────┤
  │  rl_pipeline        ✅ ran 06:00        │
  │  genai_pipeline     🔄 running...       │
  │  frontend_pipeline  ⏳ scheduled 08:00  │
  └─────────────────────────────────────────┘
```

---

## How it works — for technical teams

### Architecture overview

```
barlou/scheduler/
├── framework/
│   ├── config_loader.py       # reads and validates airflow_job.yml
│   ├── segment_resolver.py    # determines where each step runs
│   ├── dag_factory.py         # builds Airflow DAGs dynamically
│   ├── pipeline_state.py      # per-pipeline upstream consumption cursor
│   ├── frequency_resolver.py  # compares cron frequencies, picks processing strategy
│   ├── freshness.py           # preflight freshness check Airflow task
│   ├── venv_manager.py        # manages per-module virtual environments
│   ├── executors/
│   │   ├── local_executor.py  # runs steps on the Airflow server
│   │   └── cloud_executor.py  # runs steps on cloud instances
│   └── providers/
│       ├── aws/provider.py    # AWS EC2 + SSM implementation
│       ├── gcp/provider.py    # GCP (stub — planned)
│       ├── azure/provider.py  # Azure (stub — planned)
│       └── ovh/provider.py    # OVH (stub — planned)
└── dags/
    └── dag_factory_loader.py  # Airflow entry point — scans + registers DAGs
```

### The config file — `airflow_job.yml`

Each project declares one file at `airflow/airflow_job.yml`. This is the **only thing a project needs** to participate in the framework.

```yaml
airflow_id: pipeline      # which pipeline this module belongs to
dag_id:     pipeline
schedule:   "0 1 * * *"          # cron — used for freshness comparison with upstreams
description: "Ingest data"
enabled: true

pipeline:
  position: 1                    # explicit order in the pipeline (1 = runs first)
  upstream_jobs: []              # airflow_ids this job depends on (optional)
  is_singleton: false            # true = no other job can join this pipeline
  chunk_threshold: 10            # files above this count trigger parallel processing
  max_parallel_chunks: 4         # max Airflow workers for parallel chunk processing

execution:
  mode: local                    # local | cloud

job:
  module: data_ingestion
  entry_point: src/main.py
  config_path: config/config.json
  base_output_name: data  # framework appends _YYYY-MM-DD automatically

retry:
  attempts: 1
  delay_minutes: 5

alerts:
  on_failure: true
```

### Pipeline assembly — automatic

The framework scans all deployed projects, groups them by `airflow_id`, sorts by `pipeline.position`, and builds the pipeline automatically.

```
Projects deployed with airflow_id: pipeline

  data_ingestion      position: 1   → step 1
  data_transformation position: 2   → step 2
  data_exposition     position: 3   → step 3

Result: one DAG called pipeline
  [freshness_check] → data_ingestion → data_transformation → data_exposition → [summary]
  Triggers at 01:00 every day
```

---

## Freshness-aware pipeline coordination

This is the core feature that differentiates this framework from a plain Airflow setup.

### The problem

Real data pipelines don't all run at the same frequency:

- Source A ingests data once a week
- Source B ingests data every hour
- Transformation T depends on both

Without explicit coordination, T runs without knowing whether A has fresh data, or how many files from B have accumulated since its last run.

### How the framework handles it

Every pipeline with declared `upstream_jobs` gets a **freshness check task** injected automatically as its first node.

```
DAG: transformation_pipeline
  [freshness_check] → [process] → [summary]
       ↑
  Checks all upstream_jobs before processing starts
```

The freshness check:

1. Loads the per-pipeline consumption state for each upstream
2. Lists files currently available from each upstream in cloud storage
3. Filters to files not yet consumed by this specific pipeline
4. Compares the upstream's cron frequency to the downstream's cron frequency
5. Applies the appropriate strategy

### Frequency strategies

| Relationship | Example | Behaviour |
|---|---|---|
| Upstream less frequent (mandatory) | Weekly source, daily transformation | **Block** if no new file since upstream's last scheduled run. Fail fast — never process with stale mandatory data. |
| Upstream more frequent (accumulation) | Hourly source, daily transformation | Collect all pending files. Process sequentially if few files, in parallel chunks if many. |
| Equal frequency | Daily source, daily transformation | Process the single new file sequentially. |

### The freshness report in Airflow UI

Every pipeline run produces a freshness table visible in the task logs:

```
═════════════════════════════════════════════════════════════════
                      FRESHNESS REPORT
═════════════════════════════════════════════════════════════════
  Upstream                  Relation       Strategy     Pending
  ------------------------- -------------- ------------ -------
  ✅ weekly_ingestor         mandatory      sequential         1
  📦 hourly_feed             accumulation   parallel          24
═════════════════════════════════════════════════════════════════
```

### Shared upstreams — no coupling between pipelines

When two pipelines depend on the same upstream, the framework detects this automatically. Each pipeline maintains its own consumption cursor — no file copy, no coordination required between pipelines.

```
upstream_ingestor produces: file_2026-05-03.parquet

pipeline_a: last consumed 2026-05-02 → file is new → process it
pipeline_b: last consumed 2026-05-01 → file is new → process it

Both pipelines process the same file independently.
```

---

## Cloud execution — the key differentiator

### Why not Lambda or Function Apps?

| Constraint | AWS Lambda | Azure Function App | This framework |
|---|---|---|---|
| Max execution time | 15 minutes | 10 minutes | Unlimited |
| Memory limit | 10 GB | 14 GB | Instance size of your choice |
| GPU support | No | No | Yes — any EC2 instance type |
| Long ML training jobs | Not possible | Not possible | Native support |

### Execution segments — intelligent routing

When a step declares `mode: cloud`, the framework automatically pulls preceding local steps onto the same instance. This minimises instance start/stop overhead.

```
Pipeline: 5 modules

Module 1   local  ─┐
Module 2   local   │
Module 3   cloud-A─┘← CLOUD-A launched, modules 1+2+3 run on it
Module 4   local   │  inherited by CLOUD-A
Module 5   local  ─┘← CLOUD-A terminated at end
```

---

## File naming convention

The framework uses a date-based naming convention for output files to track freshness:

```
{base_output_name}_{YYYY-MM-DD}.{extension}

Examples:
  crypto_data_2026-05-03.parquet
  daily_snapshot_2026-05-03.csv
```

The date is injected automatically by the framework via `build_output_path()`. Jobs only need to declare their `base_output_name` in `airflow_job.yml` — the framework handles the rest.

This convention is what allows the freshness check to determine whether a file is new without relying on cloud storage timestamps (which can be unreliable on re-uploads or copies).

---

## Adding a new project

**1. Create `airflow/airflow_job.yml` at the project root**

```yaml
airflow_id: my_new_pipeline
dag_id:     my_new_pipeline
schedule:   "0 4 * * *"
enabled: true

pipeline:
  position: 1

execution:
  mode: local

job:
  module: my_module
  entry_point: src/main.py
  config_path: config/config.json
  base_output_name: my_output

retry:
  attempts: 1
  delay_minutes: 5

alerts:
  on_failure: true
```

**2. Set `need_airflow_job: true` in `cicd.config.yml`**

**3. Deploy**

The CICD pipeline packages the `airflow_job.yml` with the deployment. On the next Airflow scheduler heartbeat (every 30 seconds), the new DAG appears in the UI automatically.

**Total time: under 5 minutes. Zero changes to the framework.**

---

## Monitoring

Every pipeline run produces a **summary task**:

```
════════════════════════════════════════════════════════════
Pipeline summary: sqpipeline
════════════════════════════════════════════════════════════
  Total steps : 3
  Run at      : 2026-05-03T01:00:00
────────────────────────────────────────────────────────────
  ✅ data_ingestion        12.3s  [local]
  ✅ data_transformation   45.1s  [local]
  ✅ data_exposition        8.7s  [local]
────────────────────────────────────────────────────────────
  Total duration: 66.1s
════════════════════════════════════════════════════════════
```

---

## Security

- No credentials in code — all secrets fetched from AWS SSM at runtime
- No credentials in artifacts — deployment packages use `{{ PLACEHOLDER }}` values
- Isolated environments — each module runs in its own venv, cleaned up after
- SSM over SSH — remote command execution uses AWS SSM, no open SSH ports required

---

## `airflow_job.yml` — complete field reference

| Field | Required | Default | Description |
|---|---|---|---|
| `airflow_id` | Yes | — | Pipeline group — modules sharing this ID form one DAG |
| `dag_id` | Yes | — | Unique DAG name in Airflow UI |
| `schedule` | Yes | — | 5-field cron expression |
| `enabled` | No | `true` | Set `false` to disable without removing the file |
| `pipeline.position` | Yes | — | Execution order within the pipeline (1-based, unique) |
| `pipeline.upstream_jobs` | No | `[]` | airflow_ids this job depends on |
| `pipeline.is_singleton` | No | `false` | If true, no other job can join this pipeline |
| `pipeline.chunk_threshold` | No | `10` | Files above this count trigger parallel processing |
| `pipeline.max_parallel_chunks` | No | `4` | Max Airflow workers for parallel chunk processing |
| `execution.mode` | Yes | — | `local` or `cloud` |
| `execution.server.provider` | If cloud | — | `aws`, `gcp`, `azure`, `ovh` |
| `execution.server.instance_type` | If cloud | — | e.g. `t3.large`, `g4dn.xlarge` |
| `execution.server.force_terminate` | No | `false` | Terminate instance after this step |
| `job.module` | Yes | — | Module name matching `~/deployments/{module}/` |
| `job.entry_point` | Yes | — | Relative path to script inside module directory |
| `job.config_path` | Yes | — | Relative path to config file inside module directory |
| `job.base_output_name` | No | — | Base name for output files (date appended automatically) |
| `retry.attempts` | No | `1` | Number of retry attempts |
| `retry.delay_minutes` | No | `5` | Delay between retries |
| `alerts.on_failure` | No | `true` | Send alert on failure |
| `alerts.on_retry` | No | `false` | Send alert on retry |
| `alerts.on_success` | No | `false` | Send alert on success |

---

## Supported providers

| Provider | Status | Notes |
|---|---|---|
| AWS EC2 + SSM | ✅ Production ready | Full implementation |
| GCP Compute Engine | 🔜 Planned | Interface defined |
| Azure VM | 🔜 Planned | Interface defined |
| OVH Cloud | 🔜 Planned | Interface defined |
| Local server | ✅ Production ready | Default mode |

---

## Requirements

**Airflow server:**
- Ubuntu 22.04+
- Python 3.11+
- AWS CLI configured with appropriate IAM role

**Projects using the framework:**
- `airflow/airflow_job.yml` at project root
- `requirements.txt` at module root
- Entry point script accepting `--config` argument

---

## Related repositories

| Repository | Purpose |
|---|---|
| `barlou/CICD` | Reusable CI/CD workflow templates |
| `barlou/scheduler` | This framework |
| `barlou/tools` | Shared Python utilities (CloudClient, Logger, ArchiveManager) |

---

*Built with Apache Airflow 2.9.0 — [Airflow documentation](https://airflow.apache.org/docs/)*
