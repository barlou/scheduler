# dags/dag_factory_loader.py
#
# ─────────────────────────────────────────────────────────────────────────────
# Airflow DAG loader — entry point read by Airflow from dags_folder
#
# Airflow scans dags_folder every dag_dir_list_interval seconds (30s default).
# Every .py file found is imported. Any DAG object in the module's globals()
# is registered automatically.
#
# This file does three things:
#   1. Ensures the framework package is importable
#   2. Calls build_all_dags() to scan deployments and build DAGs
#   3. Registers all built DAGs into globals() for Airflow to pick up
#
# Adding a new project requires zero changes here —
# just deploy the project with an airflow/airflow_job.yml and
# Airflow will pick it up on the next scan cycle.
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# 1. Ensure framework is importable
# ─────────────────────────────────────────────────────────────────────────────
# The framework lives at ~/airflow/framework/
# Airflow does not add AIRFLOW_HOME to sys.path automatically

_AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow"))
_FRAMEWORK_ROOT = _AIRFLOW_HOME.parent   # parent of airflow/ = home dir

if str(_FRAMEWORK_ROOT) not in sys.path:
    sys.path.insert(0, str(_FRAMEWORK_ROOT))

if str(_AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(_AIRFLOW_HOME))


# ─────────────────────────────────────────────────────────────────────────────
# 2. Resolve deployments base
# ─────────────────────────────────────────────────────────────────────────────
# Default: ~/deployments
# Override via AIRFLOW_DEPLOYMENTS_BASE env var if needed

_DEPLOYMENTS_BASE = Path(
    os.environ.get(
        "AIRFLOW_DEPLOYMENTS_BASE",
        str(Path.home() / "deployments"),
    )
)

# Optional: restrict to a single pipeline for debugging
# Set AIRFLOW_PIPELINE_ID=rl_pipeline to only load that pipeline
_PIPELINE_ID: str | None = os.environ.get("AIRFLOW_PIPELINE_ID") or None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Build all DAGs and register in globals()
# ─────────────────────────────────────────────────────────────────────────────

def _load() -> dict:
    """
    Import dag_factory, build all DAGs, return dag_id → DAG mapping.
    Isolated in a function so import errors surface cleanly in Airflow logs
    rather than crashing the entire loader silently.
    """
    try:
        from framework.dag_factory import build_all_dags
    except ImportError as e:
        print(
            f"[dag_factory_loader] ERROR: Could not import framework.\n"
            f"  AIRFLOW_HOME     : {_AIRFLOW_HOME}\n"
            f"  sys.path entries : {sys.path[:5]}\n"
            f"  Error            : {e}"
        )
        traceback.print_exc()
        return {}

    try:
        return build_all_dags(
            deployments_base= _DEPLOYMENTS_BASE,
            airflow_id=       _PIPELINE_ID,
        )
    except Exception as e:
        print(
            f"[dag_factory_loader] ERROR: build_all_dags failed.\n"
            f"  Deployments base : {_DEPLOYMENTS_BASE}\n"
            f"  Pipeline filter  : {_PIPELINE_ID or 'all'}\n"
            f"  Error            : {e}"
        )
        traceback.print_exc()
        return {}


# ── Register DAGs into module globals ─────────────────────────────────────────
# Airflow scans globals() of every file in dags_folder looking for DAG objects.
# Any key whose value is a DAG instance is registered automatically.

_built_dags = _load()

for _dag_id, _dag in _built_dags.items():
    globals()[_dag_id] = _dag

if _built_dags:
    print(
        f"[dag_factory_loader] Registered {len(_built_dags)} DAG(s): "
        f"{list(_built_dags.keys())}"
    )
else:
    print(
        f"[dag_factory_loader] No DAGs registered.\n"
        f"  Deployments base : {_DEPLOYMENTS_BASE}\n"
        f"  Pipeline filter  : {_PIPELINE_ID or 'all'}\n"
        f"  Check that modules are deployed with airflow/airflow_job.yml"
    )