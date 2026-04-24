# Framework/segment_resolver.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

from croniter import croniter 

from framework.config_loader import AirflowJobConfig, ServerConfig

# ─────────────────────────────────────────────────────────────────────────────
# Output dataclass — one segment = one server context
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExecutionSegment:
    """
    A contiguous group of pipeline steps sharing the same server context
    
    Attribute:
        server:          None = airflow server (local), ServerConfig = cloud instance
        steps:           ordered list of modules running in this segment
        force_terminate: if True, terminate this instance after the last step 
                         even if subsequent segments could reuse it 
        is_last:         True if this is the final segment in the pipeline used 
                         by cloud_executor to know when to terminate
    """
    server:          ServerConfig | None
    steps:           list[AirflowJobConfig] = field(default_factory=list)
    force_terminate: bool                   = False
    is_last:         bool                   = False

    @property
    def is_local(self) -> bool:
        return self.server is None
    
    @property
    def is_cloud(self) -> bool:
        return self.server is not None 
    
    @property
    def provider(self) -> str | None:
        return self.server.provider if self.server else None
    
    def __repr__(self) -> str:
        server_label = (
            f"{self.server.provider}:{self.server.instance_type}"
            if self.server else "local"
        )
        steps_name = [s.job.module for s in self.steps]
        return (
            f"ExecutionSegment("
            f"server={server_label}, "
            f"steps={steps_name}, "
            f"force_terminate={self.force_terminate}, "
            f"is_last={self.is_last})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Cron helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cron_sort_key(cfg: AirflowJobConfig) -> float:
    """
    Return the next schedule timestamp for a cron expression.
    Used to sort modules within a pipeline by execution order
    
    Earlier cron = lower timestamp = runs first
    """
    try:
        return croniter(cfg.schedule).get_next(float)
    except Exception:
        raise ValueError(
            f"Invalid cron expression '{cfg.schedule}'"
            f"in {cfg.source_path}"
        )
        
def sort_by_cron(configs: list[AirflowJobConfig]) -> list[AirflowJobConfig]:
    """Sort a list of jobs config by their cron schedule ascending."""
    return sorted(configs, key=_cron_sort_key)

# ─────────────────────────────────────────────────────────────────────────────
# Core segment resolution logic
# ─────────────────────────────────────────────────────────────────────────────

def resolve_segments(
    sorted_configs: list[AirflowJobConfig],
) -> list[ExecutionSegment]:
    """
    Give a cron-sorted list of modules belonging to one pipeline, 
    resolve them into ExecutionSegments applying these rules:
    
    RULE 1 - Cloud backwards pull:
        When a module declares mode=cloud, all preceding modules back to the pipeline 
        start OR the previous force_terminate boundary are pull into the same server segment
    
    RULE 2 - Segment inheritance:
        After an instance declaration, subsequent local modules inherit that instance
        segment until the next instance declaration or force_terminate
    
    RULE 3 - force_terminate:
        Closes the current segment after this module
        The next instance declaration still applies backwards pull from this boundary
    
    RULE 4 - Parallel terminate/launch:
        Segment boundaries where a new instance starts while a previous one is running are flagged here (force_terminate=True on the closing segment).
        cloud_executor handles the parallelism 
        
    RULE 5 - Local before first instance:
        Modules before the first instance declaration that get pulled into an instance segment
        are marked as pulled - they run on the instance, not locally
        
    Args:
        sorted_configs (list[AirflowJobConfig]): cron-sorted list of AirflowJobConfig
                                        all belonging to the same pipeline (airflow_id)

    Returns:
        list[ExecutionSegment]: in execution order
            Empty list if sorted_config is empty
    """
    if not sorted_configs:
            return []
    
    if len(sorted_configs) == 1:
        cfg = sorted_configs[0]
        server = cfg.execution.server if cfg.execution.mode == "cloud" else None
        return [ExecutionSegment(
            server=          server,
            steps=           [cfg],
            force_terminate= (
                server is not None and 
                (cfg.execution.server.force_terminate if server else False)
            ),
            is_last=         True,
        )]
        
    # ── Phase 1: assign raw segment index to each module ──────────────────
    # We scan forward and track segment boundaries.
    # Each entry: (config, segment_id, server_at_declaration | None)

    # First pass — find all EC2 declaration positions and force_terminate
    # positions to determine segment boundaries.

    # We work with a mutable list of (config, assigned_server)
    # Initial assignment: everyone gets None (local)
    assignments: list[tuple[AirflowJobConfig, ServerConfig | None]] = [
        (cfg, None) for cfg in sorted_configs
    ]
    
    # Track the last force_terminate boundary index
    # Backwards pull never crosses a force_terminate boundary 
    last_boundary: int = 0
    i = 0
    while i < len(assignments):
        cfg, _ = assignments[i]
        
        if cfg.execution.mode == "cloud":
            server = cfg.execution.server
            
            # -- Backwards pull 
            # Pull all preceding steps back to last_boundary onto this server
            for j in range(last_boundary, i):
                prev_cfg, prev_server = assignments[j]
                # Only pull if not already on a different cloud server
                # (a previous instance segment takes priority)
                if prev_server is None:
                    assignments[j] = (prev_cfg, server)
            
            # -- Assign this step
            assignments[i] = (cfg, server)
            
            # -- Forward inheritance
            # Assign following local steps to this server until:
            # - another instance declaration is found (it will handle its own pull)
            # - a force_terminate is encountered 
            j = i + 1
            while j < len(assignments):
                next_cfg, next_server = assignments[j]
                if next_cfg.execution.mode == "cloud":
                    # next instance will handle its own backwards pull
                    break
                if next_server is None:
                    assignments[j] = (next_cfg, server)
                    
                # If this step has force_terminate, stop inheritance here 
                # force_terminate lives on the server config of the instance module 
                # but we check the currently assigned server for the step 
                if (next_cfg.execution.server is not None and 
                        next_cfg.execution.server.force_terminate):
                    last_boundary = j
                    break
                j+=1
            
            # Update boundary if this declaration has force_terminate
            if server.force_terminate:
                last_boundary = i + 1
        
        i += 1
    
    # -- Phase 2: group consecutive same-server assignments into segments 
    segments: list[ExecutionSegment] = []
    current_segment: ExecutionSegment | None = None
    
    for idx, (cfg, server) in enumerate(assignments):
        # Determine if we need a new segment
        if current_segment is None:
            current_segment = ExecutionSegment(server=server, steps=[cfg])
        elif _same_server(current_segment.server, server):
            current_segment.steps.append(cfg)
        else:
            # Server changed - close current segment 
            # Check if the last step of closing segment has force_terminate
            last_step = current_segment.steps[1]
            current_segment.force_terminate = _has_force_terminate(last_step)
            segments.append(current_segment)
            current_segment = ExecutionSegment(server=server, steps=[cfg])
    
    # Close final segment
    if current_segment is not None:
        last_step = current_segment.steps[-1]
        current_segment.force_terminate = _has_force_terminate(last_step)
        current_segment.is_last = True
        segments.append(current_segment)
    
    # Mark is_last on the actual last segment
    if segments:
        # Reset all is_last first (only last should be True)
        for seg in segments[:-1]:
            seg.is_last = None
        segments[-1].is_last = True

    return segments

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _same_server(
    a: ServerConfig | None,
    b: ServerConfig | None
) -> bool:
    """
    Two steps share the same serer if both are local (None) or both point to the exact same 
    ServerConfig instance. We use identity check (id) because segment_resolver assigns the same
    ServerConfig object to all steps in a segment
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a is b 

def _has_force_terminate(cfg: AirflowJobConfig) -> bool:
    """Return True if this module's server config has force_terminate=True"""
    return (
        cfg.execution.server is not None and 
        cfg.execution.server.force_terminate
    )

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline-level entry point
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline_segments(
    configs: list[AirflowJobConfig],
) -> list[ExecutionSegment]:
    """
    Full pipeline resolution:
        1. Sort by cron
        2. Resolve segments

    Args:
        configs (list[AirflowJobConfig]): unsorted list or AirflowJobConfig for one pipeline (airflow_id)

    Returns:
        list[ExecutionSegment]: Ordered list of ExecutionSegment ready for executor consumption
    """
    if not configs:
        return []
    sorted_configs = sort_by_cron(configs)
    segments      = resolve_segments(sorted_configs)
    
    # Debug summary
    print(f"\n[segment_resolver] Pipeline: {configs[0].dag_id}")
    print(f"    {len(sorted_configs)} module(s) -> {len(segments)} segment(s)")
    for i, seg in enumerate(segments):
        server_label = (
            f"{seg.server.provider}:{seg.server.instance_type}"
            if seg.server else "local"
        )
        step_names = [s.job.module for s in seg.steps]
        print(
            f"Segment {i + 1}: [{server_label}]"
            f"{step_names}"
            f"{'force_terminate ' if seg.force_terminate else ''}"
            f"{'(last)' if seg.is_last else ''}"
        )
    
    return segments

# ─────────────────────────────────────────────────────────────────────────────
# Standalone test — run this file directly to verify logic
# python3 -m framework.segment_resolver
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from pathlib import Path
    from framework.config_loader import(
        AirflowJobConfig, ExecutionConfig, JobConfig, 
        RetryConfig, AlertConfig, ServerConfig
    )
    
    def _make_local(module: str, schedule: str) -> AirflowJobConfig:
        return AirflowJobConfig(
            airflow_id="test_pipeline",
            dag_id="test_pipeline",
            schedule=schedule,
            execution=ExecutionConfig(mode="local"),
            job=JobConfig(
                module=module,
                entry_point='src/main.py',
                config_path="config/config.json",
            ),
            source_path=Path(f"/fake/{module}/airflow/airflow_job.yml")
        )
        
    def _make_cloud(
        module: str,
        schedule: str,
        instance_type: str = "t3.large",
        force_terminate: bool = False,
    ) -> AirflowJobConfig:
        server = ServerConfig(
            provider="aws",
            instance_type=instance_type,
            region="eu-west-1",
            ami_id="ami-test",
            subnet_id="subnet-test",
            security_group_id="sg-test",
            iam_instance_profile="test-profile",
            force_terminate=force_terminate,
        )
        return AirflowJobConfig(
            airflow_id="test_pipeline",
            dag_id="test_pipeline",
            schedule=schedule,
            execution=ExecutionConfig(mode="cloud", server=server),
            job=JobConfig(
                module=module,
                entry_point="src/main.py",
                config_path="config/config.json",
            ),
            source_path=Path(f"/fake/{module}/airflow/airflow_job.yml")
        )
        
    print("=" * 60)
    print("TEST 1 - your current pipeline (all local)")
    print("=" * 60)
    t1 = build_pipeline_segments([
        _make_local("data_ingestion",      "0 1 * * *"),
        _make_local("data_transformation", "0 2 * * *"),
        _make_local("data_database",       "0 3 * * *"),
    ])
    
    print("\n" + "=" * 60)
    print("TEST 2 - instance on step 2 pulls step 1 onto INSTANCE-A")
    print("=" * 60)
    t2 = build_pipeline_segments([
        _make_local("step1", "0 1 * * *"),
        _make_cloud("step2", "0 2 * * *", "t3.large"),
        _make_local("step3", "0 3 * * *"),
    ])
    
    print("\n" + "=" * 60)
    print("TEST 3 - 10 modules, cloud on 2 and 8")
    print("=" * 60)
    t3 = build_pipeline_segments([
        _make_local("m1",  "0 1 * * *"),
        _make_cloud("m2",  "0 2 * * *", "t3.large"),
        _make_local("m3",  "0 3 * * *"),
        _make_local("m4",  "0 4 * * *"),
        _make_local("m5",  "0 5 * * *"),
        _make_local("m6",  "0 6 * * *"),
        _make_local("m7",  "0 7 * * *"),
        _make_cloud("m8",  "0 8 * * *", "g4dn.xlarge"),
        _make_local("m9",  "0 9 * * *"),
        _make_local("m10", "0 10 * * *"),
    ])
    
    print("\n" + "=" * 60)
    print("TEST 4 - force_terminate on m4, instance on m7 pulls m5+m6")
    print("=" * 60)
    t4 = build_pipeline_segments([
        _make_local("m1", "0 1 * * *"),
        _make_cloud("m2", "0 2 * * *", "t3.large"),
        _make_local("m3", "0 3 * * *"),
        _make_cloud("m4", "0 4 * * *", "g4dn.xlarge", force_terminate=True),
        _make_local("m5", "0 5 * * *"),
        _make_local("m6", "0 6 * * *"),
        _make_cloud("m7", "0 7 * * *", "t3.large"),
        _make_local("m8", "0 8 * * *"),
    ])