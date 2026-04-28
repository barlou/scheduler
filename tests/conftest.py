"""Shared fixtures and factory helpers for the test suite."""
import pytest
from pathlib import Path

from framework.config_loader import (
    AirflowJobConfig,
    AlertConfig,
    ExecutionConfig,
    JobConfig,
    RetryConfig,
    ServerConfig,
)
from framework.providers.base_provider import ServerInstance


# ─────────────────────────────────────────────────────────────────────────────
# Factory helpers (plain functions — usable directly in test modules)
# ─────────────────────────────────────────────────────────────────────────────

def make_server_config(
    force_terminate: bool = False,
    provider: str = "aws",
    instance_type: str = "t3.large",
) -> ServerConfig:
    return ServerConfig(
        provider=provider,
        instance_type=instance_type,
        region="eu-west-1",
        ami_id="ami-test123",
        subnet_id="subnet-test123",
        security_group_id="sg-test123",
        iam_instance_profile="test-profile",
        startup_timeout_minutes=10,
        force_terminate=force_terminate,
    )


def make_local_config(
    module: str = "my_module",
    schedule: str = "0 1 * * *",
    dag_id: str = "test_dag",
    airflow_id: str = "test_pipeline",
    enabled: bool = True,
) -> AirflowJobConfig:
    return AirflowJobConfig(
        airflow_id=airflow_id,
        dag_id=dag_id,
        schedule=schedule,
        execution=ExecutionConfig(mode="local"),
        job=JobConfig(
            module=module,
            entry_point="src/main.py",
            config_path="config/config.json",
        ),
        enabled=enabled,
        source_path=Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


def make_cloud_config(
    module: str = "cloud_module",
    schedule: str = "0 2 * * *",
    dag_id: str = "test_dag",
    airflow_id: str = "test_pipeline",
    force_terminate: bool = False,
    instance_type: str = "t3.large",
) -> AirflowJobConfig:
    server = make_server_config(
        force_terminate=force_terminate,
        instance_type=instance_type,
    )
    return AirflowJobConfig(
        airflow_id=airflow_id,
        dag_id=dag_id,
        schedule=schedule,
        execution=ExecutionConfig(mode="cloud", server=server),
        job=JobConfig(
            module=module,
            entry_point="src/main.py",
            config_path="config/config.json",
        ),
        source_path=Path(f"/fake/{module}/airflow/airflow_job.yml"),
    )


def make_server_instance(
    instance_id: str = "i-0abc123def456",
    is_ready: bool = True,
    provider: str = "aws",
    region: str = "eu-west-1",
) -> ServerInstance:
    return ServerInstance(
        instance_id=instance_id,
        provider=provider,
        region=region,
        public_ip="1.2.3.4",
        private_ip="10.0.0.1",
        is_ready=is_ready,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pytest fixtures (wrappers around helpers)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def local_config():
    return make_local_config()


@pytest.fixture
def cloud_config():
    return make_cloud_config()


@pytest.fixture
def server_config():
    return make_server_config()


@pytest.fixture
def server_instance():
    return make_server_instance()
