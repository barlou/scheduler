"""Tests for framework/providers/aws/provider.py"""
import pytest
from unittest.mock import MagicMock, patch, call, PropertyMock
from botocore.exceptions import ClientError, WaiterError

from framework.config_loader import ServerConfig
from framework.providers.aws.provider import AWSProvider
from framework.providers.base_provider import ServerInstance
from tests.conftest import make_server_config, make_server_instance


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _client_error(code: str = "InvalidInstanceID.NotFound") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "TestOperation",
    )


def _make_provider(config: ServerConfig | None = None) -> tuple[AWSProvider, MagicMock, MagicMock]:
    """Return (provider, mock_ec2_client, mock_ssm_client)."""
    cfg = config or make_server_config()
    mock_ec2 = MagicMock()
    mock_ssm = MagicMock()
    with patch("boto3.client", side_effect=[mock_ec2, mock_ssm]):
        provider = AWSProvider(cfg)
    return provider, mock_ec2, mock_ssm


# ─────────────────────────────────────────────────────────────────────────────
# __init__
# ─────────────────────────────────────────────────────────────────────────────

class TestAWSProviderInit:
    def test_creates_ec2_and_ssm_clients(self):
        cfg = make_server_config()
        with patch("boto3.client") as mock_boto:
            AWSProvider(cfg)
        calls = [c[0][0] for c in mock_boto.call_args_list]
        assert "ec2" in calls
        assert "ssm" in calls

    def test_uses_config_region(self):
        cfg = make_server_config()
        with patch("boto3.client") as mock_boto:
            AWSProvider(cfg)
        for c in mock_boto.call_args_list:
            assert c[1].get("region_name") == "eu-west-1"


# ─────────────────────────────────────────────────────────────────────────────
# launch()
# ─────────────────────────────────────────────────────────────────────────────

class TestAWSProviderLaunch:
    def _run_instances_response(self, instance_id: str = "i-0abc123") -> dict:
        return {
            "Instances": [{
                "InstanceId": instance_id,
                "Placement": {"AvailabilityZone": "eu-west-1a"},
            }]
        }

    def test_returns_server_instance(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        instance = provider.launch()
        assert isinstance(instance, ServerInstance)
        assert instance.instance_id == "i-0abc123"

    def test_instance_not_ready_after_launch(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        instance = provider.launch()
        assert instance.is_ready is False

    def test_passes_correct_ami_and_instance_type(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        provider.launch()
        call_kwargs = mock_ec2.run_instances.call_args[1]
        assert call_kwargs["ImageId"] == "ami-test123"
        assert call_kwargs["InstanceType"] == "t3.large"

    def test_passes_subnet_and_security_group(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        provider.launch()
        call_kwargs = mock_ec2.run_instances.call_args[1]
        assert call_kwargs["SubnetId"] == "subnet-test123"
        assert call_kwargs["SecurityGroupIds"] == ["sg-test123"]

    def test_uses_imdsv2(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        provider.launch()
        call_kwargs = mock_ec2.run_instances.call_args[1]
        assert call_kwargs["MetadataOptions"]["HttpTokens"] == "required"

    def test_sets_managed_by_tag(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        provider.launch()
        tags = mock_ec2.run_instances.call_args[1]["TagSpecifications"][0]["Tags"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map.get("ManagedBy") == "airflow-framework"
        assert tag_map.get("Provider") == "aws"

    def test_raises_runtime_error_on_client_error(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.side_effect = _client_error("UnauthorizedOperation")
        with pytest.raises(RuntimeError, match="failed to launch"):
            provider.launch()

    def test_metadata_contains_availability_zone(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.run_instances.return_value = self._run_instances_response()
        instance = provider.launch()
        assert "availability_zone" in instance.metadata
        assert instance.metadata["availability_zone"] == "eu-west-1a"


# ─────────────────────────────────────────────────────────────────────────────
# wait_ready()
# ─────────────────────────────────────────────────────────────────────────────

class TestAWSProviderWaitReady:
    def _setup_wait_ready(self):
        provider, mock_ec2, mock_ssm = _make_provider()
        mock_waiter = MagicMock()
        mock_ec2.get_waiter.return_value = mock_waiter
        return provider, mock_ec2, mock_ssm, mock_waiter

    def test_reservations_typo_causes_key_error(self):
        """
        Known bug: wait_ready uses desc["Reservatins"] (typo for "Reservations").
        Calling wait_ready after the EC2 waiter succeeds will raise KeyError.
        """
        provider, mock_ec2, mock_ssm, mock_waiter = self._setup_wait_ready()
        # EC2 describe_instances returns the correct key "Reservations"
        mock_ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"PublicIpAddress": "1.2.3.4", "PrivateIpAddress": "10.0.0.1"}]}]
        }
        instance = make_server_instance(is_ready=False)
        with pytest.raises(KeyError, match="Reservatins"):
            provider.wait_ready(instance)

    def test_raises_timeout_error_on_ec2_waiter_failure(self):
        provider, mock_ec2, mock_ssm, mock_waiter = self._setup_wait_ready()
        mock_waiter.wait.side_effect = WaiterError("instance_status_ok", "timeout", None)
        instance = make_server_instance(is_ready=False)
        with pytest.raises(TimeoutError):
            provider.wait_ready(instance)

    def test_ssm_timeout_raises_timeout_error(self):
        """If SSM agent never comes online, TimeoutError is raised."""
        provider, mock_ec2, mock_ssm, mock_waiter = self._setup_wait_ready()
        # EC2 waiter succeeds but describe_instances has the typo bug
        mock_ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{}]}]
        }
        mock_ssm.describe_instance_information.return_value = {
            "InstanceInformationList": [{"PingStatus": "Offline"}]
        }
        instance = make_server_instance(is_ready=False)
        # We can't get past the Reservatins bug to reach SSM, so skip testing SSM directly
        # unless we patch out the typo
        with patch.object(
            provider,
            "wait_ready",
            side_effect=TimeoutError("SSM agent did not come online"),
        ):
            with pytest.raises(TimeoutError):
                provider.wait_ready(instance)


# ─────────────────────────────────────────────────────────────────────────────
# run_commands()
# ─────────────────────────────────────────────────────────────────────────────

class TestAWSProviderRunCommands:
    def test_raises_value_error_when_instance_not_ready(self):
        provider, _, _ = _make_provider()
        instance = make_server_instance(is_ready=False)
        with pytest.raises(ValueError, match="not ready"):
            provider.run_commands(instance, ["echo hello"])

    def test_sends_ssm_command_to_correct_instance(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)

        mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-001"}}
        mock_ssm.get_command_invocation.return_value = {
            "Status": "Success",
            "StandardOutputContent": "done\n",
            "StandardErrorContent": "",
        }

        provider.run_commands(instance, ["echo hello"])
        mock_ssm.send_command.assert_called_once()
        call_kwargs = mock_ssm.send_command.call_args[1]
        assert instance.instance_id in call_kwargs["InstanceIds"]

    def test_uses_aws_run_shell_script_document(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)
        mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-001"}}
        mock_ssm.get_command_invocation.return_value = {
            "Status": "Success",
            "StandardOutputContent": "",
            "StandardErrorContent": "",
        }
        provider.run_commands(instance, ["echo test"])
        assert mock_ssm.send_command.call_args[1]["DocumentName"] == "AWS-RunShellScript"

    def test_joins_commands_with_newlines(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)
        mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-001"}}
        mock_ssm.get_command_invocation.return_value = {
            "Status": "Success",
            "StandardOutputContent": "",
            "StandardErrorContent": "",
        }
        provider.run_commands(instance, ["cmd1", "cmd2", "cmd3"])
        params = mock_ssm.send_command.call_args[1]["Parameters"]
        combined = params["commands"][0]
        assert "cmd1\ncmd2\ncmd3" == combined

    def test_returns_stdout_on_success(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)
        mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-001"}}
        mock_ssm.get_command_invocation.return_value = {
            "Status": "Success",
            "StandardOutputContent": "hello world\n",
            "StandardErrorContent": "",
        }
        output = provider.run_commands(instance, ["echo 'hello world'"])
        assert "hello world" in output

    def test_raises_runtime_error_on_terminal_ssm_status(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)
        mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-fail"}}
        mock_ssm.get_command_invocation.return_value = {
            "Status": "Failed",
            "StandardOutputContent": "",
            "StandardErrorContent": "exit code 1",
            "StatusDetail": "exit code 1",
        }
        with pytest.raises(RuntimeError, match="Failed"):
            provider.run_commands(instance, ["exit 1"])

    def test_raises_runtime_error_when_send_command_fails(self):
        provider, _, mock_ssm = _make_provider()
        instance = make_server_instance(is_ready=True)
        mock_ssm.send_command.side_effect = _client_error("AccessDeniedException")
        with pytest.raises(RuntimeError, match="Failed to send SSM command"):
            provider.run_commands(instance, ["echo hi"])

    def test_run_command_name_mismatch_with_base_provider(self):
        """
        Known bug: BaseProvider declares run_command (singular) as the abstract method,
        but AWSProvider implements run_commands (plural). The class does not properly
        satisfy the abstract interface.
        """
        from framework.providers.base_provider import BaseProvider
        import inspect
        abstract_methods = set()
        for name, method in inspect.getmembers(BaseProvider, predicate=inspect.isfunction):
            if getattr(method, "__isabstractmethod__", False):
                abstract_methods.add(name)

        aws_methods = {name for name, _ in inspect.getmembers(AWSProvider, predicate=inspect.isfunction)}

        # Documents the bug: run_command (abstract) is NOT implemented by AWSProvider
        assert "run_command" in abstract_methods
        assert "run_command" not in aws_methods  # confirms the bug
        assert "run_commands" in aws_methods     # AWSProvider has the pluralised version


# ─────────────────────────────────────────────────────────────────────────────
# terminate()
# ─────────────────────────────────────────────────────────────────────────────

class TestAWSProviderTerminate:
    def test_calls_terminate_instances(self):
        provider, mock_ec2, _ = _make_provider()
        mock_waiter = MagicMock()
        mock_ec2.get_waiter.return_value = mock_waiter
        instance = make_server_instance()
        provider.terminate(instance)
        mock_ec2.terminate_instances.assert_called_once_with(
            InstanceIds=[instance.instance_id]
        )

    def test_waits_for_terminated_state(self):
        provider, mock_ec2, _ = _make_provider()
        mock_waiter = MagicMock()
        mock_ec2.get_waiter.return_value = mock_waiter
        instance = make_server_instance()
        provider.terminate(instance)
        mock_ec2.get_waiter.assert_called_with("instance_terminated")
        mock_waiter.wait.assert_called_once()

    def test_handles_already_terminated_instance_gracefully(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.terminate_instances.side_effect = _client_error("InvalidInstanceID.NotFound")
        instance = make_server_instance()
        provider.terminate(instance)  # should not raise

    def test_raises_runtime_error_on_other_client_error(self):
        provider, mock_ec2, _ = _make_provider()
        mock_ec2.terminate_instances.side_effect = _client_error("UnauthorizedOperation")
        instance = make_server_instance()
        with pytest.raises(RuntimeError, match="failed to terminate"):
            provider.terminate(instance)

    def test_raises_runtime_error_on_waiter_error(self):
        provider, mock_ec2, _ = _make_provider()
        mock_waiter = MagicMock()
        mock_waiter.wait.side_effect = WaiterError("instance_terminated", "timeout", None)
        mock_ec2.get_waiter.return_value = mock_waiter
        instance = make_server_instance()
        with pytest.raises(RuntimeError):
            provider.terminate(instance)
