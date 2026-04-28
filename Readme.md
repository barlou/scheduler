# Airflow Orchestration Framework

![CI](https://github.com/barlou/scheduler/actions/
workflows/ci.yml/badge.svg?branch=main)
![Release](https://img.shields.io/github/v/release/barlou/scheduler)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Security](https://img.shields.io/badge/security-gitleaks-red)

> **Schedule, coordinate, and scale your data workflows — without cloud functions, without infrastructure complexity, without endless refactoring.**

---

## What is this?

This is a custom orchestration framework built on top of [Apache Airflow](https://airflow.apache.org/). It turns Airflow from a scheduling tool into a full **workflow execution platform** that can run jobs locally or spin up cloud instances on demand, automatically, with a single configuration file per project.

If you have ever dealt with any of the following, this framework was built for you:

- Cron jobs scattered across servers with no visibility into what ran, when, and whether it succeeded
- AWS Lambda functions hitting timeout limits on long-running jobs
- Azure Function Apps that are painful to debug and expensive to run for compute-heavy workloads
- Data pipelines that need to run in sequence but have no coordination mechanism
- A growing number of projects that each need their own scheduler

---

## The problem it solves

### For business stakeholders

| Problem | Without this framework | With this framework |
|---|---|---|
| Running a data pipeline | Manually trigger scripts or manage cron | Fully automated, scheduled, monitored |
| Scaling a heavy job | Pay for Lambda/Function App limits or manage EC2 manually | Automatically spins up the right server, runs the job, shuts it down |
| Visibility | No central view of what ran | One Airflow dashboard showing all pipelines across all projects |
| Cost control | Always-on compute or unpredictable serverless billing | Pay only for the exact compute time used |
| Adding a new project | Set up a new scheduler, cron, or cloud function | Add one config file — framework handles the rest |

### For technical teams

| Problem | Without this framework | With this framework |
|---|---|---|
| Dependency management | Global packages, version conflicts between projects | Each module gets its own isolated virtual environment, created fresh per run |
| Multi-step pipelines | Scripts calling other scripts, fragile bash chains | Declarative step ordering via cron priority — Airflow handles sequencing |
| Cloud provider lock-in | Boto3 calls hardcoded throughout the codebase | Provider abstraction layer — swap AWS for GCP or OVH by changing one line |
| Environment consistency | Works on my machine | Same venv creation + execution on local server and remote EC2 |
| Secret management | Credentials in code or environment files | AWS SSM at runtime — nothing sensitive in the artifact or the repository |

---

## How it works — for business stakeholders

Think of this framework as a **smart coordinator** that knows:

1. **What needs to run** — each project declares its jobs in a simple config file
2. **In what order** — the schedule time determines priority within a pipeline
3. **Where to run it** — locally on the server for lightweight jobs, or on a fresh cloud instance for heavy compute
4. **When to clean up** — cloud instances are terminated automatically when the job finishes

You get a central dashboard (Airflow UI) that shows every pipeline, every run, every success and failure — across all your projects — in one place.

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
│   ├── config_loader.py      # reads and validates airflow_job.yml
│   ├── segment_resolver.py   # determines where each step runs
│   ├── dag_factory.py        # builds Airflow DAGs dynamically
│   ├── venv_manager.py       # manages per-module virtual environments
│   ├── executors/
│   │   ├── local_executor.py # runs steps on the Airflow server
│   │   └── cloud_executor.py # runs steps on cloud instances
│   └── providers/
│       ├── aws/provider.py   # AWS EC2 + SSM implementation
│       ├── gcp/provider.py   # GCP (stub — future)
│       ├── azure/provider.py # Azure (stub — future)
│       └── ovh/provider.py   # OVH (stub — future)
└── dags/
    └── dag_factory_loader.py # Airflow entry point — scans + registers DAGs
```

### The config file — `airflow_job.yml`

Each project declares one file at `airflow/airflow_job.yml`. This is the **only thing a project needs to configure** to participate in the framework.

```yaml
airflow_id: rl_pipeline          # which pipeline this module belongs to
schedule: "0 1 * * *"            # cron — determines execution order
description: "Ingest crypto data"
enabled: true

execution:
  mode: local                    # local | cloud

job:
  module: data_ingestion
  entry_point: src/Crypto/main.py
  config_path: config/config.json

retry:
  attempts: 1
  delay_minutes: 5

alerts:
  on_failure: true
```

### Pipeline assembly — automatic

The framework scans all deployed projects for `airflow_job.yml` files, groups them by `airflow_id`, sorts by cron schedule, and builds the pipeline automatically.

```
Projects deployed with airflow_id: rl_pipeline

  data_ingestion      schedule: 0 1 * * *   → step 1
  data_transformation schedule: 0 2 * * *   → step 2
  data_exposition     schedule: 0 3 * * *   → step 3

Result: one DAG called rl_pipeline
  data_ingestion → data_transformation → data_exposition
  Triggers at 01:00 every day
```

No code change in the framework — just deploy the project with the config file.

### Virtual environment isolation

Every module gets a **fresh, isolated virtual environment** for every run:

```
Before each step:
  python3 -m venv /tmp/venv_data_ingestion
  pip install -r requirements.txt

Run the job:
  /tmp/venv_data_ingestion/bin/python src/main.py

After each step (always — even on failure):
  rm -rf /tmp/venv_data_ingestion
```

This means:
- No version conflicts between modules
- No stale dependencies from previous runs
- Identical behaviour on the local server and on remote EC2 instances

---

## Cloud execution — the key differentiator

### Why not Lambda or Function Apps?

| Constraint | AWS Lambda | Azure Function App | This framework |
|---|---|---|---|
| Max execution time | 15 minutes | 10 minutes (consumption plan) | Unlimited |
| Memory limit | 10 GB | 14 GB | Instance size of your choice |
| GPU support | No | No | Yes — any EC2 instance type |
| Cold start | Yes | Yes | Configurable startup |
| Debugging | CloudWatch logs only | Application Insights | Full stdout in Airflow UI |
| Cost model | Per invocation + duration | Per invocation + duration | Per second of EC2 runtime |
| Long ML training jobs | Not possible | Not possible | Native support |

### Execution segments — intelligent routing

The framework automatically determines which steps run where using a segment assignment algorithm:

**Rule: when a step declares `mode: cloud`, all preceding steps in the pipeline are pulled onto the same instance.**

```
Pipeline: 10 modules

Module 1   local  ─┐
Module 2   cloud-A─┘← CLOUD-A launched, modules 1+2 run on it
Module 3   local   │  inherited by CLOUD-A segment
Module 4   local   │  inherited by CLOUD-A segment
Module 5   local   │  inherited by CLOUD-A segment
Module 6   local   │  inherited by CLOUD-A segment
Module 7   local   │  inherited by CLOUD-A segment
Module 8   cloud-B─┘← CLOUD-A terminated, CLOUD-B launched (in parallel)
Module 9   local   │  inherited by CLOUD-B segment
Module 10  local  ─┘← CLOUD-B terminated at end
```

**Parallel terminate and launch** — when transitioning between cloud segments, the previous instance is terminated at the same time the next one is launched. No waiting. No double billing beyond a few seconds of overlap.

**`force_terminate`** — close a cloud segment early and let the next EC2 declaration start fresh:

```yaml
# Module 4 closes EC2-A early
execution:
  mode: cloud
  server:
    force_terminate: true   # terminate after this module
```

### Multi-cloud ready

The provider abstraction means switching cloud provider requires changing **one line** in `airflow_job.yml`:

```yaml
# Current
server:
  provider: aws

# Switch to GCP when ready
server:
  provider: gcp
```

Zero changes in the framework code. The provider implements the same interface (`launch`, `wait_ready`, `run_commands`, `terminate`) regardless of cloud.

---

## Real-world use cases

### Data engineering pipelines

```yaml
# data_ingestion/airflow/airflow_job.yml
airflow_id: crypto_pipeline
schedule: "0 1 * * *"
execution:
  mode: local
```

```yaml
# data_transformation/airflow/airflow_job.yml
airflow_id: crypto_pipeline
schedule: "0 2 * * *"
execution:
  mode: local
```

```yaml
# data_exposition/airflow/airflow_job.yml
airflow_id: crypto_pipeline
schedule: "0 3 * * *"
execution:
  mode: local
```

Result: a daily pipeline that ingests, transforms, and stores data — fully automated, monitored, with retry on failure.

---

### Machine learning training

```yaml
# model_training/airflow/airflow_job.yml
airflow_id: ml_pipeline
schedule: "0 2 * * 0"       # weekly on Sunday
execution:
  mode: cloud
  server:
    provider: aws
    instance_type: g4dn.xlarge   # GPU instance
    terminate_after: true
job:
  module: model_training
  entry_point: src/train.py
```

Result: every Sunday at 2am, a GPU instance spins up, trains the model, and shuts down. You pay for roughly 2-3 hours of GPU compute per week instead of keeping a GPU instance running 24/7.

---

### Replacing scheduled Lambda functions

```yaml
# report_generator/airflow/airflow_job.yml
airflow_id: reporting_pipeline
schedule: "0 8 * * 1-5"     # weekdays at 8am
execution:
  mode: local                # lightweight — no need for EC2
job:
  module: report_generator
  entry_point: src/generate.py
```

Runs in under a minute, no timeout concerns, full logs in the Airflow UI, automatic retry on failure. No Lambda configuration, no IAM policies for Lambda, no cold start delays.

---

### Multi-environment pipelines

The same `airflow_job.yml` works across INT, UAT, and production. The framework resolves environment-specific config at deploy time from SSM — the config file itself contains no environment-specific values.

---

## Adding a new project — what it takes

For a developer adding a new project to the framework:

**1. Create `airflow/airflow_job.yml` at the project root**

```yaml
airflow_id: my_new_pipeline
schedule: "0 4 * * *"
enabled: true
execution:
  mode: local
job:
  module: my_module
  entry_point: src/main.py
  config_path: config/config.json
retry:
  attempts: 1
  delay_minutes: 5
alerts:
  on_failure: true
```

**2. Set `need_airflow_job: true` in `cicd.config.yml`**

```yaml
components:
  airflow_job:
    enabled: true
```

**3. Deploy**

The CICD pipeline packages the `airflow_job.yml` with the deployment. On the next Airflow scheduler heartbeat (every 30 seconds), the new DAG appears in the Airflow UI automatically.

**Total time: under 5 minutes. Zero changes to the framework.**

---

## Monitoring and observability

The Airflow UI provides out-of-the-box:

- **DAG view** — visual graph of pipeline steps and their dependencies
- **Run history** — every execution with success/failure status and duration
- **Task logs** — full stdout/stderr from each step, streamed in real time
- **Retry tracking** — how many retries occurred and why
- **Duration trends** — spot when a job starts taking longer than usual

Every pipeline run also produces a **summary task** that prints a formatted report:

```
════════════════════════════════════════════════════════════
Pipeline summary: rl_pipeline
════════════════════════════════════════════════════════════
  Total steps : 3
  Run at      : 2025-01-15T01:00:00
────────────────────────────────────────────────────────────
  ✅ data_ingestion        12.3s  [local]
  ✅ data_transformation   45.1s  [local]
  ✅ data_exposition       8.7s  [local]
────────────────────────────────────────────────────────────
  Total duration: 66.1s
════════════════════════════════════════════════════════════
```

---

## Security

- **No credentials in code** — all secrets fetched from AWS SSM at runtime
- **No credentials in artifacts** — deployment packages contain only templates with `{{ PLACEHOLDER }}` values
- **Isolated environments** — each module runs in its own venv, cleaned up after
- **IMDSv2 enforced** — EC2 instances use the latest metadata service version
- **SSM over SSH** — remote command execution uses AWS SSM, no open SSH ports required on job instances

---

## Version pinning — no forced upgrades

Each project pins the exact version of the scheduler framework it uses:

```yaml
# cicd.config.yml
components:
  airflow:
    version: "scheduler-v1.2.0"   # pinned — upgrade on your own schedule
    repo: "barlou/scheduler"
```

A framework update never breaks existing projects. Teams upgrade when ready, test in INT/UAT, and promote to production through the normal CICD flow.

---

## Supported providers

| Provider | Status | Notes |
|---|---|---|
| AWS EC2 + SSM | ✅ Production ready | Full implementation |
| GCP Compute Engine | 🔜 Planned | Interface defined, implementation pending |
| Azure VM | 🔜 Planned | Interface defined, implementation pending |
| OVH Cloud | 🔜 Planned | Interface defined, implementation pending |
| Local server | ✅ Production ready | Default mode, no cloud account needed |

---

## Requirements

**Server running Airflow:**
- Ubuntu 22.04+
- Python 3.11+
- AWS CLI configured with appropriate IAM role
- 2GB RAM minimum (4GB recommended)

**Projects using the framework:**
- `airflow/airflow_job.yml` at project root
- `requirements.txt` at module root
- Entry point script accepting `--config` argument

**Cloud execution (EC2 mode):**
- IAM role with EC2 and SSM permissions on the Airflow server
- AMI with Python 3.11 and SSM agent pre-installed
- VPC with appropriate security groups

---

## Quick reference — `airflow_job.yml` fields

| Field | Required | Description |
|---|---|---|
| `airflow_id` | Yes | Pipeline group — modules sharing this ID form one DAG |
| `dag_id` | Yes | Unique DAG name in Airflow UI |
| `schedule` | Yes | Cron expression — determines execution order within pipeline |
| `enabled` | No | Default `true` — set `false` to disable without removing the file |
| `execution.mode` | Yes | `local` or `cloud` |
| `execution.server.provider` | If cloud | `aws`, `gcp`, `azure`, `ovh` |
| `execution.server.instance_type` | If cloud | e.g. `t3.large`, `g4dn.xlarge` |
| `execution.server.force_terminate` | No | Terminate instance after this step, default `false` |
| `job.module` | Yes | Module name matching `~/deployments/{module}/` |
| `job.entry_point` | Yes | Relative path to script inside module directory |
| `job.config_path` | Yes | Relative path to config file inside module directory |
| `retry.attempts` | No | Default `1` |
| `retry.delay_minutes` | No | Default `5` |
| `alerts.on_failure` | No | Default `true` |
| `alerts.on_retry` | No | Default `false` |
| `alerts.on_success` | No | Default `false` |

---

## Related repositories

| Repository | Purpose |
|---|---|
| `barlou/CICD` | Reusable CI/CD workflow templates |
| `barlou/scheduler` | This framework |
| `barlou/tools` | Shared Python utilities (CloudClient, Logger, ArchiveManager) |

---

<<<<<<< HEAD
*Built with Apache Airflow 2.9.0 — [Airflow documentation](https://airflow.apache.org/docs/)*
=======
*Built with Apache Airflow 2.9.0 — [Airflow documentation](https://airflow.apache.org/docs/)*
>>>>>>> develop
