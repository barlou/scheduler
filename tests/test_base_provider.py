"""Tests for framework/providers/base_provider.py"""
import pytest
from unittest.mock import MagicMock, patch

from framework.providers.base_provider import BaseProvider, ServerInstance
from framework.config_loader import ServerConfig
from tests.conftest import make_server_config, make_server_instance


# ─────────────────────────────────────────────────────────────────────────────
# Concrete test double — implements all abstract methods
# ─────────────────────────────────────────────────────────────────────────────

class _FakeProvider(BaseProvider):
    def __init__(self, config: ServerConfig):
        super().__init__(config)
        self._launched_instance: ServerInstance | None = None
        self._terminate_called = False

    def launch(self) -> ServerInstance:
        self._launched_instance = ServerInstance(
            instance_id="i-fake001",
            provider=self.config.provider,
            region=self.config.region,
            is_ready=False,
        )
        return self._launched_instance

    def wait_ready(self, instance: ServerInstance) -> ServerInstance:
        instance.is_ready = True
        instance.public_ip = "1.2.3.4"
        return instance

    def run_command(self, instance: ServerInstance, commands: list) -> str:
        return "fake output"

    def terminate(self, instance: ServerInstance) -> None:
        self._terminate_called = True


# ─────────────────────────────────────────────────────────────────────────────
# ServerInstance
# ─────────────────────────────────────────────────────────────────────────────

class TestServerInstance:
    def test_required_fields(self):
        inst = ServerInstance(instance_id="i-abc", provider="aws", region="eu-west-1")
        assert inst.instance_id == "i-abc"
        assert inst.provider == "aws"
        assert inst.region == "eu-west-1"

    def test_default_optional_fields(self):
        inst = ServerInstance(instance_id="i-abc", provider="aws", region="eu-west-1")
        assert inst.public_ip == ""
        assert inst.private_ip == ""
        assert inst.metadata == {}
        assert inst.is_ready is False

    def test_full_instantiation(self):
        inst = ServerInstance(
            instance_id="i-xyz",
            provider="gcp",
            region="europe-west1",
            public_ip="8.8.8.8",
            private_ip="10.0.0.5",
            metadata={"zone": "b"},
            is_ready=True,
        )
        assert inst.is_ready is True
        assert inst.metadata["zone"] == "b"

    def test_repr_contains_key_fields(self):
        inst = make_server_instance()
        r = repr(inst)
        assert "i-0abc123def456" in r
        assert "aws" in r
        assert "eu-west-1" in r

    def test_repr_shows_public_ip_when_set(self):
        inst = ServerInstance(
            instance_id="i-1",
            provider="aws",
            region="us-east-1",
            public_ip="54.0.0.1",
        )
        assert "54.0.0.1" in repr(inst)

    def test_metadata_is_independent_per_instance(self):
        inst1 = ServerInstance(instance_id="i-1", provider="aws", region="us-east-1")
        inst2 = ServerInstance(instance_id="i-2", provider="aws", region="us-east-1")
        inst1.metadata["key"] = "value"
        assert "key" not in inst2.metadata


# ─────────────────────────────────────────────────────────────────────────────
# BaseProvider.validate_ready
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateReady:
    def test_passes_when_instance_is_ready(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance(is_ready=True)
        provider.validate_ready(inst)  # should not raise

    def test_raises_when_instance_not_ready(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance(is_ready=False)
        with pytest.raises(ValueError, match="not ready"):
            provider.validate_ready(inst)

    def test_error_message_contains_instance_id(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance(instance_id="i-notready", is_ready=False)
        with pytest.raises(ValueError, match="i-notready"):
            provider.validate_ready(inst)


# ─────────────────────────────────────────────────────────────────────────────
# BaseProvider.terminate_safe
# ─────────────────────────────────────────────────────────────────────────────

class TestTerminateSafe:
    def test_calls_terminate(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance()
        provider.terminate_safe(inst)
        assert provider._terminate_called is True

    def test_swallows_exception_from_terminate(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance()

        # Override terminate to raise
        provider.terminate = MagicMock(side_effect=RuntimeError("AWS error"))
        provider.terminate_safe(inst)  # should not re-raise

    def test_logs_warning_on_terminate_failure(self, capsys):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        inst = make_server_instance(instance_id="i-broken")
        provider.terminate = MagicMock(side_effect=RuntimeError("termination failed"))
        provider.terminate_safe(inst)
        output = capsys.readouterr().out
        assert "WARNING" in output or "termination failed" in output


# ─────────────────────────────────────────────────────────────────────────────
# BaseProvider.launch_and_wait
# ─────────────────────────────────────────────────────────────────────────────

class TestLaunchAndWait:
    def test_calls_launch_then_wait_ready(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        instance = provider.launch_and_wait()
        assert instance is not None
        assert instance.is_ready is True

    def test_returned_instance_has_public_ip(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        instance = provider.launch_and_wait()
        assert instance.public_ip == "1.2.3.4"

    def test_launch_called_before_wait_ready(self):
        cfg = make_server_config()
        provider = _FakeProvider(cfg)
        call_order = []
        original_launch = provider.launch
        original_wait = provider.wait_ready

        def tracked_launch():
            call_order.append("launch")
            return original_launch()

        def tracked_wait(inst):
            call_order.append("wait_ready")
            return original_wait(inst)

        provider.launch = tracked_launch
        provider.wait_ready = tracked_wait
        provider.launch_and_wait()
        assert call_order == ["launch", "wait_ready"]


# ─────────────────────────────────────────────────────────────────────────────
# BaseProvider.from_config (factory)
# ─────────────────────────────────────────────────────────────────────────────

class TestFromConfig:
    def test_aws_returns_aws_provider(self):
        from framework.providers.aws.provider import AWSProvider
        cfg = make_server_config(provider="aws")
        with patch("boto3.client"):  # avoid real AWS calls
            provider = BaseProvider.from_config(cfg)
        assert isinstance(provider, AWSProvider)

    def test_unsupported_provider_raises_value_error(self):
        cfg = make_server_config(provider="aws")
        cfg_bad = ServerConfig(
            provider="digitalocean",
            instance_type="s-1vcpu-1gb",
            region="nyc3",
            ami_id="ubuntu-20-04",
            subnet_id="subnet-x",
            security_group_id="sg-x",
            iam_instance_profile="n/a",
        )
        # Bypass Literal type check for testing the factory error path
        object.__setattr__(cfg_bad, "provider", "digitalocean")
        with pytest.raises((ValueError, AttributeError)):
            BaseProvider.from_config(cfg_bad)

    def test_gcp_provider_file_is_empty_stub(self):
        """GCPProvider class does not exist — the stub file is empty."""
        cfg = make_server_config(provider="gcp")
        object.__setattr__(cfg, "provider", "gcp")
        with pytest.raises((ImportError, AttributeError)):
            BaseProvider.from_config(cfg)

    def test_azure_provider_file_is_empty_stub(self):
        """AzureProvider class does not exist — the stub file is empty."""
        cfg = make_server_config(provider="azure")
        object.__setattr__(cfg, "provider", "azure")
        with pytest.raises((ImportError, AttributeError)):
            BaseProvider.from_config(cfg)

    def test_ovh_provider_file_is_empty_stub(self):
        """OVHProvider class does not exist — the stub file is empty."""
        cfg = make_server_config(provider="ovh")
        object.__setattr__(cfg, "provider", "ovh")
        with pytest.raises((ImportError, AttributeError)):
            BaseProvider.from_config(cfg)
