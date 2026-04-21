# framework/venv_manager.py
from __future__ import annotations

import shutil, subprocess, sys
from pathlib import Path 

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

VENV_BASE_DIR   = Path("/tmp")
VENV_PREFIX     = "venv_"
DEPLOYMENTS     = Path.home() / "deployments"

# ─────────────────────────────────────────────────────────────────────────────
# VenvManager
# ─────────────────────────────────────────────────────────────────────────────

class VenvManager:
    """
    Manages per-module virtual environments.
    
    Lifecycle per module step:
        1. create()     - python3 -m venv /tmp/venv_{module}
        2. install()    - pip install -r requirements.txt
        3. run()        - /tmp/venv_{module}/bin/python entry_point --config config
        4. cleanup()    - rm -rf /tmp/venv_{module}
        
    Works identically on the local Airflow server and on a remote instance.
    On instance, the same commands are sent via SSM - no code change needed.
    """
    
    def __init__(self, module: str, deployment_base: Path = DEPLOYMENTS):
        self.module     = module
        self.deployment_base = deployment_base
        self.venv_path       = VENV_BASE_DIR / f"{VENV_PREFIX}{module}"
        self.module_dir      = deployment_base / module
        self.python          = self.venv_path / "bin" / "python"
        self.pip             = self.venv_path / "bin" / "pip"
        
    # Public lifecycle method
    
    def create(self) -> None:
        """
        Create a fresh virtual environment for this module.
        If one already exists at the same path, remove it first
        to guarantee a clean state.
        """
        if self.venv_path.exists():
            print(f"    [venv] removing stale env: {self.venv_path}")
            shutil.rmtree(self.venv_path)
            
        print(f"    [venv] creating: {self.venv_path}")
        self._run_command(
            [sys.executable, "-m", "venv", str(self.venv_path)],
            context="create venv",
        )
        print(f"    [venv] created: {self.venv_path}")
    
    def install(self) -> None:
        """
        Install module dependencies from requirements.txt 
        Fails clearly if requirements.txt is missing
        """
        requirements = self.module_dir / "requirements.txt"
        
        if not requirements.exists():
            raise FileNotFoundError(
                f"requirements.txt not found for module '{self.module}' "
                f"at {requirements}\n"
                f"Every module must ship a requirements.txt in its deployment."
            )
        print(f"    [venv] installing dependencies from {requirements}")
        self._run_command(
            [
                str(self.pip),
                "install",
                "--requirement", str(requirements),
                "--quiet",
                "--no-cache-dir",
            ],
            context=f"pip install for {self.module}",
        )
        print(f"    [venv] dependencies installed")
    
    def run(
        self,
        entry_point: str,
        config_path: str,
        extra_env:   dict[str, str] | None = None,
    ) -> None:
        """
        Run the module(s entry point inside the venv.)

        Args:
            entry_point (str): relative path to the script, e.g, src/main.py
            config_path (str): relative path to the config, e.g, config/config.json
            extra_env (dict[str, str] | None, optional): optional extra environment variables for the process. Defaults to None.
        """
        script = self.module_dir / entry_point
        config = self.module_dir / config_path
        
        if not script.exists():
            raise FileNotFoundError(
                f"entry_point not found for module '{self.module}': {script}"
            )

        if not config.exists():
            raise FileNotFoundError(
                f"config_path not found for module '{self.module}': {config}"
            )
            
        import os
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.module_dir)
        env["MODULE_DIR"] = str(self.module_dir)
        
        if extra_env:
            env.update(extra_env)
        print(f"    [venv] runnning: {script} --config {config}")
        self._run_command(
            [
                str(self.python),
                str(script),
                "--config", str(config),
            ],
            context=f"run {self.module}",
            cwd=str(self.module_dir),
            env=env,
        )
        print(f"    [venv] completed: {self.module}")
        
    def cleanup(self) -> None:
        """
        Remove the virtual environment after the module has run.
        Safe to call even if the venv does not exist 
        """
        if self.venv_path.exists():
            shutil.rmtree(self.venv_path)
            print(f"    [venv] cleaned up: {self.venv_path}")
        else:
            print(f"    [venv] nothing to clean: {self.venv_path}")
    
    # Convenience method - full lifecycle in one call
    
    def execute(
        self, 
        entry_point: str, 
        config_path: str,
        extra_env:   dict[str, str] | None = None
    ) -> None:
        """
        Full lifecycle: create -> install -> run -> cleanup
        Cleanup always run even if run() raises an execution

        Args:
            entry_point (str): relative path to the script
            config_path (str): relative path to the config file 
            extra_env (dict[str, str] | None, optional): extra env variables (optional). Defaults to None.
        """
        try:
            self.create()
            self.install()
            self.run(entry_point, config_path, extra_env)
        finally:
            self.cleanup()
    
    # Remote command builder - used by cloud_executor via SSM
    
    def build_remote_command(
        self,
        entry_point: str, 
        config_path: str,
    ) -> list[str]:
        """
        Build the shell command to run this module on a remote server via SSM
        Returns a list of shell commands that SSM executes sequentially
        
        The remote server must have:
            - python3 available
            - the module deployed at ~/deployments/{module}/

        Args:
            entry_point (str): relative path to the script
            config_path (str): relative path to the config file

        Returns:
            list[str]: List of shell command string for SSM RunCommand
        """
        venv         = str(self.venv_path)
        module_dir   = f"~/deployments/{self.module}"
        python       = f"{venv}/bin/python"
        pip          = f"{venv}/bin/pip"
        requirements = f"{module_dir}/requirements.txt"
        script       = f"{module_dir}/{entry_point}"
        config       = f"{module_dir}/{config_path}"
        
        return [
            f"set -euo pipefail",
            
            # Remove stale venv if present 
            f"[ -d {venv} ] && rm -rf {venv} || true",
            
            # Create fresh venv
            f"python3 -m venv {venv}",
            
            # Install dependencies 
            f"{pip} install --requirement {requirements} --quiet --no-cache-dir",
            
            # Run the module
            f"cd {module_dir} && "
            f"PYTHONPATH={module_dir} "
            f"MODULE_DIR={module_dir} "
            f"{python} {script} --config {config}",
            
            "Cleanup venv"
            f"rm -rf {venv}",
        ]
    
    # Internal
    def _run_command(
        self,
        cmd:     list[str],
        context: str,
        cwd:     str | None = None,
        env:     dict | None = None,
    ) -> None:
        """
        Run a subprocess command, streaming output in real time
        Raises RunTimeError with clear message on non-zero exit
        """
        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            text=True,
            capture_output=False # stream stdout/stderr directly 
        )
        
        if result.returncode != 0:
            raise RuntimeError(
                f"[venv] command failed during '{context}' "
                f"for module '{self.module}' "
                f"(exit code {result.returncode})"
            )