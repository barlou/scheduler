# framework/executors/cloud_executor.py
from __future__ import annotations

import time, threading 

from airflow import DAG
from airflow.operators.python import PythonOperator

from framework.config_loader import AirflowJobConfig, ServerConfig
from framework.executors.base_executor import BaseExecutor, TaskResult
from framework.providers.base_provider import BaseProvider, ServerInstance
from framework.segment_resolver import ExecutionSegment
from framework.venv_manager import VenvManager

# ─────────────────────────────────────────────────────────────────────────────
# Parallel terminate + launch helper
# ─────────────────────────────────────────────────────────────────────────────

def _parallel_terminate_and_launch(
    current_provider: BaseProvider,
    current_instance: ServerInstance,
    next_provider:    BaseProvider,
) -> ServerInstance:
    """
    Terminate current instance and launch next instance in parallel.
    Waits for next instance to be ready before returning.
    
    Timeline:
        t=0     terminate current (async)   +   launch next (async)
        t=?     next instance ready         ->  return next instance 
        t=?     current instance terminated (background thread - fire and forget)

    Args:
        current_provider (BaseProvider): provider managing the instance to terminate
        current_instance (ServerInstance): instance to terminate
        next_provider (BaseProvider): provider for the new instance 

    Returns:
        ServerInstance: for the next segment
    """
    next_instance_holder: list[ServerInstance] = []
    launch_error_holder:  list[Exception]      = []
    term_error_holder:    list[Exception]      = []
    
    def _terminate() -> None:
        try: 
            current_provider.terminate_safe(current_instance)
        except Exception as e:
            term_error_holder.append(e)
    
    def _launch_and_wait() -> None:
        try:
            instance = next_provider.launch_and_wait()
            next_instance_holder.append(instance)
        except Exception as e:
            launch_error_holder.append(e)
    
    # Start both operations in parallel
    terminate_thread = threading.Thread(target=_terminate,       daemon=True)
    launch_thread    = threading.Thread(target=_launch_and_wait, daemon=True)
    
    terminate_thread.start()
    launch_thread.start()
    
    # Wait for launch to complete - this is the blocker 
    # Terminate runs in background - we do not wait for it 
    # (it will complete on its own, instance billing stops regardless)
    launch_thread.join()
    
    # Surface launch errors immediately - pipeline cannot continue 
    if launch_error_holder:
        raise RuntimeError(
            f"  [cloud] Failed to launch next instance: {launch_error_holder}"
        ) from launch_error_holder[0]
        
    # Log termination warnings but do not block on them 
    if term_error_holder:
        print(
            f"  [cloud] WARNING: termination of "
            f"{current_instance.instance_id} had an error: "
            f"{term_error_holder[0]}\n"
            f"  Manual cleanup may be required."
        )
    
    return next_instance_holder[0]

# ─────────────────────────────────────────────────────────────────────────────
# Step callable — runs inside Airflow task on cloud instance via provider
# ─────────────────────────────────────────────────────────────────────────────

def _run_cloud_step(
    module:        str,
    entry_point:   str,
    config_path:   str,
    dag_id:        str,
    server_config: dict,
    **context,
) -> dict:
    """
    Airflow task callable for a single cloud step.
    
    Fetches the running instance from XCom (set by the segment setup task),
    builds venv commands, runs them via provider, and pushes TaskResult to XCom

    Args:
        module (str): module name 
        entry_point (str): relative script path
        config_path (str): relative config path
        dag_id (str): pipeline DAG ID
        server_config (dict): dict representation of ServiceConfig (passed as dict -
            ServerConfig is not XCom-serializable)
        **context: Airflow task content

    Returns:
        dict: TaskResult.to_dict()
    """
    from framework.config_loader import ServerConfig
    from framework.providers.base_provider import BaseProvider, ServerInstance
    
    ti = context["ti"]
    
    # Reconstruct ServerConfig from dict 
    cfg = ServerConfig(**server_config)
    
    # Fetch active instance from XCom
    # The segment setup task pushed instance data under key "active_instance"
    instance_data = ti.xcom_pull(key="active_instance")
    if not instance_data:
        raise RuntimeError(
            f"  [cloud] No active instance found in XCom for module '{module}'"
            f"Segment setup task may have failed."
        )
    
    instance = ServerInstance(**instance_data)
    
    print(
        f"\n[cloud] Starting step\n"
        f"  DAG      : {dag_id}\n"
        f"  Module.  : {module}\n"
        f"  Instance : {instance}\n"
        f"  Provider : {cfg.provider}\n"
        f"  Script   : {entry_point}\n"
        f"  Config   : {config_path}"
    )
    start = time.monotonic()
    provider = BaseProvider.from_config(cfg)
    manager = VenvManager(module)
    
    try:
        # Build remote shell command using venv_manager
        commands = manager.build_remote_command(entry_point, config_path)
        
        # Execute on the cloud instance via provider
        output = provider.run_command(instance, commands)
        
        duration = time.monotonic() - start
        result = TaskResult(
            module = module,
            success = True,
            output = output,
            duration_s = round(duration, 2),
            instance_id = instance.instance_id,
        )
        print(
            f"\n[cloud] Step completed\n"
            f"  Module   : {module}\n"
            f"  Instance : {instance}\n"
            f"  Duration : {duration:.1f}s"
        )
    except Exception as e:
        duration = time.monotonic() - start
        error    = str(e)
        result   = TaskResult(
            module = module, 
            success = False,
            error = error,
            duration_s = round(duration, 2),
            instance_id = instance.instance_id,
        )
        print(
            f"\n[cloud] Step FAILED\n"
            f"  Module : {module}\n"
            f"  Instance : {instance.instance_id}\n"
            f"  Error : {error}"
        )
        raise RuntimeError(
            f"  [cloud] Module '{module}' failed on {instance.instance_id}: {error}"
        ) from e
        
    return result.to_dict()

def _setup_segment(
    server_config:      dict, 
    prev_instance_data: dict | None, 
    is_first_cloud:     bool,
    dag_id:             str, 
    **context,
) -> None:
    """
    Airflow task callable - runs before the first step of a cloud segment.
    
    Responsibilities:
        - if first cloud segment: launch a new instance 
        - if subsequent cloud segment: parallel terminate previous + launch new 
        - Push active instance to XCom for step tasks to read 

    Args:
        server_config (dict): dict of ServerConfig for this segment 
        prev_instance_data (dict | None): dict of previous ServerInstance if any 
        is_first_cloud (bool): True if no previous cloud segment exist 
        dag_id (str): pipeline DAG ID for logging
        **context: Airflow task context
    """
    from framework.config_loader import ServerConfig
    from framework.providers.base_provider import BaseProvider, ServerInstance
    
    ti = context["ti"]
    cfg = ServerConfig(**server_config)
    
    print(
        f"\n[cloud] Segment setup\n"
        f"  DAG : {dag_id}\n"
        f"  Provider : {cfg.provider}\n"
        f"  Type : {cfg.instance_type}\n"
        f"  Region : {cfg.region}\n"
        f"  First : {is_first_cloud}"
    )
    
    provider = BaseProvider.from_config(cfg)
    
    if is_first_cloud or prev_instance_data is None:
        # First cloud segment - just launch
        print(f"    [cloud] Launching first instance...")
        instance = provider.launch_and_wait()
    else:
        # Subsequent cloud segment - parallel terminate + launch
        print(f"    [cloud] Segment handoff - parallel terminate + launch...")
        
        prev_instance = ServerInstance(**prev_instance_data)
        
        # Build provider for previous instance using its own config
        # stored in instance metadata
        prev_server_config = ServerConfig(
            **prev_instance.metadata.get("server_config", server_config)
        )
        prev_provider = BaseProvider.from_config(prev_server_config)
        
        instance = _parallel_terminate_and_launch(
            current_provider = prev_provider,
            current_instance = prev_instance,
            next_provider = provider, 
        )
    
    # Store server_config in metadata so next segment can build prev_provider
    instance.metadata["server_config"] = server_config
    # Push active instance to XCom - step tasks read this
    ti.xcom_push(key="active_instance", value=vars(instance))
    print(f"    [cloud] Active instance pushed to XCom: {instance.instance_id}")

def _teardown_segment(
    server_config: dict,
    force:         bool,
    is_last:       bool,
    dag_id:        str,
    **context,
) -> None:
    """
    Airflow task callable - runs after the last step of a cloud segment.
    
    Terminate the instance if:
        - force_terminate is True, OR
        - is_last is True (end of pipeline)
    
    If neither, instance stays running for the next segment's parallel terminate/launch to handle

    Args:
        server_config (dict): dict of ServerConfig for this segment
        force (bool): True if force_terminate was set on this segment 
        is_last (bool): True if this is the last segment in the pipeline 
        dag_id (str): pipeline DAG ID
        **context: Airflow task context 
    """
    from framework.config_loader import ServerConfig
    from framework.providers.base_provider import BaseProvider, ServerInstance
    
    ti = context['ti']
    cfg = ServerConfig(**server_config)
    
    instance_data = ti.xcom_pull(key="active_instance")
    if not instance_data:
        print(
            f"  [cloud] No active instance in XCom during teardown"
            f"- May have already been terminated"
        )
        return 
    
    instance = ServerInstance(**instance_data)
    provider = BaseProvider.from_config(cfg)
    
    should_terminate = force or is_last
    
    if should_terminate:
        print(
            f"\n[cloud] Segment teardown - terminating\n"
            f"  Instance : {instance.instance_id}\n"
            f"  Force : {force}\n"
            f"  Is last : {is_last}"
        )
        provider.terminate_safe(instance)
        
        # Clear XCom so next segment knows there is no active instance 
        ti.xcom_push(key="active_instance", value=None)
        print(f"    [cloud] Instance terminated and XCom cleared")
    
    else:
        print(
            f"\n[cloud] Segment teardown - keeping instance alive\n"
            f"  Instance : {instance.instance_id}\n"
            f"  Reason : next segment will handle parallel terminate/launch"
        )

# ─────────────────────────────────────────────────────────────────────────────
# CloudExecutor
# ─────────────────────────────────────────────────────────────────────────────

class CloudExecutor(BaseExecutor):
    """
    Runs all steps in a segment on a cloud instance via the provider
    Task structure per segment

      [setup]              ← launch or parallel terminate+launch
         ↓
      [step_1]             ← _run_cloud_step
         ↓
      [step_2]             ← _run_cloud_step
         ↓
      [step_N]             ← _run_cloud_step
         ↓
      [teardown]           ← terminate if force or is_last

    XCom flow:
      setup pushes  → active_instance
      steps pull    ← active_instance
      teardown pulls← active_instance, then clears it

    Args:
        segment:          resolved ExecutionSegment
        is_first_cloud:   True if no cloud segment precedes this one
                          used to decide launch vs parallel terminate+launch
        prev_instance:    ServerInstance from previous segment if any
    """

def __init__(
    self,
    segment: ExecutionSegment,
    is_first_cloud: bool = True,
    prev_instance: ServerInstance | None = None,
):
    super().__init__(segment)
    self.is_first_cloud = is_first_cloud
    self.prev_instance = prev_instance
    
def build_tasks(
    self, 
    dag: DAG,
    upstream_task: PythonOperator | None = None,
) -> PythonOperator:
    """
    Build setup -> steps -> teardown task chain for this segment

    Args:
        dag (DAG): Dag to attach tasks to
        upstream_task (PythonOperator | None, optional): last task of previous segment. Defaults to None.

    Returns:
        PythonOperator: teardown task - used to chain next segment 
    """
    self._log_segment_summary()
    
    server_config = self._server_config_as_dict()
    prev_data = vars(self.prev_instance) if self.prev_instance else None
    
    # Setup task
    setup_task = self._make_python_task(
        dag = dag,
        task_id = self._segment_task_id("setup"),
        callable = _setup_segment,
        op_kwargs = {
            "server_config" : server_config,
            "prev_instance_data" : prev_data,
            "is_first_cloud" : self.is_first_cloud,
            "dag_id" : self.segment.steps[0].dag_id,
        },
        step = self.segment.steps[0]
    )
    
    # Connect to previous segment 
    if upstream_task is not None:
        setup_task.set_upstream(upstream_task)
    
    # Step tasks
    step_tasks: list[PythonOperator] = []
    
    for step in self.segment.steps:
        task = self._make_python_task(
            dag =     dag,
            task_id = self._task_id(step),
            callable = _run_cloud_step,
            op_kwargs = {
                "module" : step.job.module,
                "entry_point" : step.job.entry_point,
                "config_path" : step.job.config_path,
                "dag_id" : step.dag_id,
                "server_config" : server_config,
            },
            step = step,
        )
        step_tasks.append(task)
    
    # Chain setup -> step_1 -> step_2 -> ... -> step_N
    self._chain_tasks(step_tasks, upstream_task=setup_task)
    
    # Teardown task
    teardown_task = self._make_python_task(
        dag = dag,
        task_id = self._segment_task_id("teardown"),
        callable = _teardown_segment,
        op_kwargs = {
            "server_config": server_config, 
            "force": self.segment.force_terminate,
            "is_last": self.segment.is_last,
            "dag_id": self.segment.steps[0].dag_id,
        },
        step = self.segment.steps[-1],
    )
    
    # Chain last step -> teardown 
    teardown_task.set_upstream(step_tasks[-1])
    
    return teardown_task

# Private 
def _server_config_as_dict(self) -> dict:
    """
    Convert ServerConfig to a plain dict for XCom and op_kwargs.
    ServerConfig dataclass is not directly XCom-serializable
    """
    cfg = self.segment.server
    return {
        "provider":                cfg.provider,
        "instance_type":           cfg.instance_type, 
        "region":                  cfg.region, 
        "ami_id":                  cfg.ami_id,
        "subnet_id":               cfg.subnet_id,
        "security_group_id":       cfg.security_group_id,
        "iam_instance_profile":    cfg.iam_instance_profile,
        "startup_timeout_minutes": cfg.startup_timeout_minutes,
        "force_terminate":         cfg.force_terminate,
    }

def _segment_task_id(self, role: str) -> str:
    """
    Generate task ID for setup/teardown tasks.
    Format: segment_{provider}_{instance_type}__{role}
    e.g.:   segment_aws_t3large__setup
    """
    cfg = self.segment.server
    base = (
        f"segment_{cfg.provider}_"
        f"{cfg.instance_type.replace('.', '').replace('-', '_')}"
    )
    return f"{base}__{role}"