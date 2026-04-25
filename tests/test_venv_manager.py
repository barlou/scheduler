"""Tests for framework/venv_manager.py"""
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from framework.venv_manager import VENV_BASE_DIR, VENV_PREFIX, VenvManager


# ─────────────────────────────────────────────────────────────────────────────
# __init__
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerInit:
    def test_venv_path_uses_prefix_and_module(self):
        mgr = VenvManager("my_module")
        assert mgr.venv_path == VENV_BASE_DIR / f"{VENV_PREFIX}my_module"

    def test_python_path_inside_venv(self):
        mgr = VenvManager("my_module")
        assert str(mgr.python).endswith("bin/python")
        assert str(mgr.venv_path) in str(mgr.python)

    def test_pip_path_inside_venv(self):
        mgr = VenvManager("my_module")
        assert str(mgr.pip).endswith("bin/pip")
        assert str(mgr.venv_path) in str(mgr.pip)

    def test_module_dir_uses_deployment_base(self, tmp_path):
        mgr = VenvManager("my_module", deployment_base=tmp_path)
        assert mgr.module_dir == tmp_path / "my_module"

    def test_custom_deployment_base(self, tmp_path):
        mgr = VenvManager("proj", deployment_base=tmp_path)
        assert mgr.deployment_base == tmp_path


# ─────────────────────────────────────────────────────────────────────────────
# create()
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerCreate:
    def test_calls_python_venv(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        with patch.object(mgr, "_run_command") as mock_run:
            with patch.object(mgr.venv_path, "exists", return_value=False):
                mgr.create()
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert sys.executable in cmd
        assert "-m" in cmd
        assert "venv" in cmd
        assert str(mgr.venv_path) in cmd

    def test_removes_stale_venv_before_creating(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        with patch("shutil.rmtree") as mock_rmtree:
            with patch.object(mgr.venv_path, "exists", return_value=True):
                with patch.object(mgr, "_run_command"):
                    mgr.create()
        mock_rmtree.assert_called_once_with(mgr.venv_path)

    def test_no_rmtree_when_venv_does_not_exist(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        with patch("shutil.rmtree") as mock_rmtree:
            with patch.object(mgr.venv_path, "exists", return_value=False):
                with patch.object(mgr, "_run_command"):
                    mgr.create()
        mock_rmtree.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# install()
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerInstall:
    def test_raises_file_not_found_when_requirements_missing(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        (tmp_path / "m").mkdir()  # module dir exists, but no requirements.txt
        with pytest.raises(FileNotFoundError, match="requirements.txt"):
            mgr.install()

    def test_calls_pip_install_with_requirements(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        module_dir = tmp_path / "m"
        module_dir.mkdir()
        (module_dir / "requirements.txt").write_text("pandas\n")
        with patch.object(mgr, "_run_command") as mock_run:
            mgr.install()
        cmd = mock_run.call_args[0][0]
        assert str(mgr.pip) in cmd
        assert "install" in cmd
        assert "--requirement" in cmd
        assert str(module_dir / "requirements.txt") in cmd

    def test_passes_no_cache_dir_flag(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        module_dir = tmp_path / "m"
        module_dir.mkdir()
        (module_dir / "requirements.txt").write_text("")
        with patch.object(mgr, "_run_command") as mock_run:
            mgr.install()
        cmd = mock_run.call_args[0][0]
        assert "--no-cache-dir" in cmd


# ─────────────────────────────────────────────────────────────────────────────
# run()
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerRun:
    def _setup_module(self, tmp_path: Path) -> VenvManager:
        mgr = VenvManager("m", deployment_base=tmp_path)
        module_dir = tmp_path / "m"
        module_dir.mkdir()
        (module_dir / "src").mkdir()
        (module_dir / "src" / "main.py").write_text("print('hello')")
        (module_dir / "config").mkdir()
        (module_dir / "config" / "config.json").write_text("{}")
        return mgr

    def test_raises_when_script_missing(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        (tmp_path / "m").mkdir()
        (tmp_path / "m" / "config").mkdir()
        (tmp_path / "m" / "config" / "config.json").write_text("{}")
        with pytest.raises(FileNotFoundError, match="entry_point"):
            mgr.run("src/main.py", "config/config.json")

    def test_raises_when_config_missing(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        module_dir = tmp_path / "m"
        module_dir.mkdir()
        (module_dir / "src").mkdir()
        (module_dir / "src" / "main.py").write_text("pass")
        with pytest.raises(FileNotFoundError, match="config_path"):
            mgr.run("src/main.py", "config/config.json")

    def test_calls_python_with_script_and_config(self, tmp_path):
        mgr = self._setup_module(tmp_path)
        with patch.object(mgr, "_run_command") as mock_run:
            mgr.run("src/main.py", "config/config.json")
        cmd = mock_run.call_args[0][0]
        assert str(mgr.python) in cmd
        assert "--config" in cmd

    def test_sets_pythonpath_and_module_dir_env(self, tmp_path):
        mgr = self._setup_module(tmp_path)
        with patch.object(mgr, "_run_command") as mock_run:
            mgr.run("src/main.py", "config/config.json")
        env = mock_run.call_args[1].get("env") or mock_run.call_args[0][3]
        assert "PYTHONPATH" in env
        assert "MODULE_DIR" in env

    def test_extra_env_merged_into_environment(self, tmp_path):
        mgr = self._setup_module(tmp_path)
        with patch.object(mgr, "_run_command") as mock_run:
            mgr.run("src/main.py", "config/config.json", extra_env={"MY_KEY": "MY_VAL"})
        # env is passed as keyword arg 'env'
        _, kwargs = mock_run.call_args
        env = kwargs.get("env") or mock_run.call_args[0][3]
        assert env.get("MY_KEY") == "MY_VAL"


# ─────────────────────────────────────────────────────────────────────────────
# cleanup()
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerCleanup:
    def test_removes_venv_when_it_exists(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        with patch("shutil.rmtree") as mock_rmtree:
            with patch.object(mgr.venv_path, "exists", return_value=True):
                mgr.cleanup()
        mock_rmtree.assert_called_once_with(mgr.venv_path)

    def test_silent_when_venv_does_not_exist(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        with patch("shutil.rmtree") as mock_rmtree:
            with patch.object(mgr.venv_path, "exists", return_value=False):
                mgr.cleanup()
        mock_rmtree.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# execute()
# ─────────────────────────────────────────────────────────────────────────────

class TestVenvManagerExecute:
    def test_full_lifecycle_called_in_order(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        calls = []
        with patch.object(mgr, "create",  side_effect=lambda: calls.append("create")):
            with patch.object(mgr, "install", side_effect=lambda: calls.append("install")):
                with patch.object(mgr, "run",     side_effect=lambda *a, **k: calls.append("run")):
                    with patch.object(mgr, "cleanup", side_effect=lambda: calls.append("cleanup")):
                        mgr.execute("src/main.py", "config/config.json")
        assert calls == ["create", "install", "run", "cleanup"]

    def test_cleanup_called_even_when_run_raises(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cleanup_called = []
        with patch.object(mgr, "create"):
            with patch.object(mgr, "install"):
                with patch.object(mgr, "run", side_effect=RuntimeError("boom")):
                    with patch.object(mgr, "cleanup", side_effect=lambda: cleanup_called.append(True)):
                        with pytest.raises(RuntimeError, match="boom"):
                            mgr.execute("src/main.py", "config/config.json")
        assert cleanup_called == [True]

    def test_cleanup_called_even_when_install_raises(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cleanup_called = []
        with patch.object(mgr, "create"):
            with patch.object(mgr, "install", side_effect=FileNotFoundError("no req")):
                with patch.object(mgr, "cleanup", side_effect=lambda: cleanup_called.append(True)):
                    with pytest.raises(FileNotFoundError):
                        mgr.execute("src/main.py", "config/config.json")
        assert cleanup_called == [True]


# ─────────────────────────────────────────────────────────────────────────────
# build_remote_command()
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildRemoteCommand:
    def test_returns_list_of_strings(self, tmp_path):
        mgr = VenvManager("my_module", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        assert isinstance(cmds, list)
        assert all(isinstance(c, str) for c in cmds)

    def test_first_command_sets_pipefail(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        assert any("pipefail" in c for c in cmds)

    def test_contains_venv_creation(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        combined = " ".join(cmds)
        assert "python3 -m venv" in combined

    def test_contains_pip_install(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        combined = " ".join(cmds)
        assert "pip" in combined
        assert "install" in combined

    def test_contains_module_entry_point(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        combined = " ".join(cmds)
        assert "src/main.py" in combined
        assert "config/config.json" in combined

    def test_cleanup_command_concatenation_bug(self, tmp_path):
        """
        Known bug: the cleanup command string literal 'Cleanup venv' is
        concatenated with the rm -rf command without a separator, producing
        the broken shell command 'Cleanup venvrm -rf ...'.
        """
        mgr = VenvManager("m", deployment_base=tmp_path)
        cmds = mgr.build_remote_command("src/main.py", "config/config.json")
        # The last command contains the concatenated broken string
        last_cmd = cmds[-1]
        assert "Cleanup venv" in last_cmd  # documents the bug
        assert "rm -rf" in last_cmd        # proves they were merged


# ─────────────────────────────────────────────────────────────────────────────
# _run_command()
# ─────────────────────────────────────────────────────────────────────────────

class TestRunCommand:
    def test_raises_runtime_error_on_non_zero_exit(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        failed_result = MagicMock()
        failed_result.returncode = 1
        with patch("subprocess.run", return_value=failed_result):
            with pytest.raises(RuntimeError, match="command failed"):
                mgr._run_command(["false"], context="test context")

    def test_no_error_on_zero_exit(self, tmp_path):
        mgr = VenvManager("m", deployment_base=tmp_path)
        ok_result = MagicMock()
        ok_result.returncode = 0
        with patch("subprocess.run", return_value=ok_result):
            mgr._run_command(["true"], context="test context")  # no exception

    def test_uses_capture_output_false(self, tmp_path):
        """Output should stream directly, not be captured."""
        mgr = VenvManager("m", deployment_base=tmp_path)
        ok_result = MagicMock(returncode=0)
        with patch("subprocess.run", return_value=ok_result) as mock_run:
            mgr._run_command(["echo", "hi"], context="test")
        _, kwargs = mock_run.call_args
        assert kwargs.get("capture_output") is False
