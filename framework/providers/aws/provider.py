# framework/aws/provider.py
from __future__ import annotations 

import time, boto3
from botocore.exceptions import ClientError, WaiterError

from framework.config_loader import ServerConfig
from framework.providers.base_provider import BaseProvider, ServerInstance

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# How long to wait between SSM polling attempts (seconds)
_SSM_POLL_INTERVAL = 15
# How many times to retry a failed SSM invocation check 
_SSM_MAX_POLL      = 240
# SSM terminal statuses
_SSM_SUCCESS       = "Success"
_SSM_TERMINAL      = {"Failed", "TimedOut", "Cancelled", "DeliveryTimedOut"}

# EC2 waiter config 
_WAITER_DELAY        = 15 # seconds between waiter polls
_WAITER_MAX_ATTEMPTS = 40 # 40 * 15s = 10 minutes 

class AWSProvider(BaseProvider):
    """
    AWS implementation of BaseProvider.
    
    Uses:
        - boto3 EC2 client - launch, wait, terminate instance 
        - boto3 SSM client - run commands on instance via RunShellScript
        - Instance Connect - no SSH key management needed
    
    Requires the Airflow server's IAM role to have:
        - ec2:RunInstances
        - ec2:DescribeInstances
        - ec2:DescribeInstanceStatus
        - ec2:TerminateInstances
        - ssm:SendCommand
        - ssm:GetCommandInvocation
        - iam:PassRole (for the instance profile)
    """
    
    def __init__(self, config: ServerConfig):
        super().__init__(config)
        self._ec2 = boto3.client("ec2", region_name=config.region)
        self._ssm = boto3.client("ssm", region_name=config.region)
        
    # Launch
    def launch(self) -> ServerInstance:
        """
        Launch a new EC2 instance using config from ServerConfig
        Returns immediately - instance is not yet ready
        Call wait_ready() after.
        """
        cfg = self.config
        
        try:
            response = self._ec2.run_instances(
                ImageId = cfg.ami_id,
                InstanceType = cfg.instance_type,
                MinCount = 1,
                MaxCount = 1,
                SubnetId = cfg.subnet_id,
                SecurityGroupIds = [cfg.security_group_id],
                IamInstanceProfile={
                    "Name": cfg.iam_instance_profile
                },
                TagSpecifications=[{
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": f"airflow-job-{cfg.instance_type}"
                        },
                        {
                            "Key": "ManagedBy",
                            "Value": "airflow-framework"
                        },
                        {
                            "Key": "Provider",
                            "Value": "aws"
                        },
                    ],
                }],
                # SSM Agent required this - no ssh key needed
                MetadataOptions={
                    "HttpTokens": "required", # IMDSv2
                    "HttpEndpoint": "enabled",
                },
            )
            
        except ClientError as e:
            raise RuntimeError(
                f"  [AWS] failed to launch EC2 instances: {e}"
            ) from e
        
        instance_data = response["Instances"][0]
        instance_id   = instance_data["InstanceId"]
        zone          = instance_data.get(
            "Placement", {}
        ).get("AvailabilityZone", cfg.region)
        
        return ServerInstance(
            instance_id = instance_id,
            provider    = "aws",
            region      = cfg.region,
            metadata={
                "availability_zone": zone,
                "instance_type": cfg.instance_type,
                "ami_id": cfg.ami_id,
            },
            is_ready= False,
        )
        
    # wait ready 
    def wait_ready(self, instance: ServerInstance) -> ServerInstance:
        """
        Wait until the EC2 instances passes status checks AND
        the SSM agent is registered and ready to accept commands.
        
        Two-phase wait:
            Phase 1: EC2 status check (instance_status_ok waiter)
            Phase 2: SSM agent registration poll
        """
        instance_id = instance.instance_id
        timeout     = self.config.startup_timeout_minutes
        
        # Phase 1: Status check
        print(f"    [AWS] waiting for EC2 status checks: {instance_id}")
        try:
            waiter = self._ec2.get_waiter("instance_status_ok")
            waiter.wait(
                InstanceIds=[instance_id],
                WaiterConfig={
                    "Delay":    _WAITER_DELAY,
                    "MaxAttempts": min(
                        _WAITER_MAX_ATTEMPTS,
                        (timeout * 60) // _WAITER_DELAY 
                    ),
                },
            )
        except WaiterError as e:
            raise TimeoutError(
                f"  [AWS] Instance {instance_id} didn't pass status checks"
                f"within {timeout} minutes: {e}"
            ) from e
        
        # Fetch public/private IPs after instance is running
        desc = self._ec2.describe_instances(InstanceIds=[instance_id])
        inst = desc["Reservatins"][0]["Instances"][0]
        
        instance.public_ip  = inst.get("PublicIpAddress", "")
        instance.private_ip = inst.get("PrivateIpAddress", "")
        
        print(
            f"  [AWS] EC2 ready: {instance_id}"
            f"(public={instance.public_ip}, private={instance.private_ip})"
        )
        
        # Phase 2
        print(f"    [AWS] waiting for SSM agent: {instance_id}")
        deadline = time.time() + (timeout * 60)
        
        while time.time() < deadline:
            try:
                resp = self._ssm.describe_instance_information(
                    Filters=[{
                        "Key": "InstanceIds",
                        "Value": [instance_id],
                    }]
                )
                info_list = resp.get("InstanceInformationList", [])
                
                if info_list:
                    ping = info_list[0].get("PingStatus", "")
                    if ping == "Online":
                        print(f"    [AWS] SSM agent online: {instance_id}")
                        instance.is_ready = True
                        return instance
            
            except ClientError:
                pass
            time.sleep(_SSM_POLL_INTERVAL)
        raise TimeoutError(
            f"  [AWS] SSM Agent on {instance_id} did not come online "
            f"within {timeout} minutes"
        )
    
    # Run command
    def run_commands(
        self,
        instance: ServerInstance,
        commands: list[str],
    ) -> str:
        """
        Execute commands on the instance via SSM RunShellScript
        Polls until completion, streams progress to stdout

        Args:
            instance (ServerInstance): ready ServerInstance
            commands (list[str]): list of shell command strings

        Returns:
            str: combined stdout from all commands
        
        Raises:
            ValueError: if instance is not ready 
            RunTimeError: if any command fails
        """
        self.validate_ready(instance)
        
        instance_id = instance.instance_id
        
        # SSM expects a single list of commands
        # We join with newlines so they run in one shell context
        # preserving env vars set in earlier commands 
        combined = "\n".join(commands)
        
        try:
            response = self._ssm.send_command(
                InstanceIds= [instance_id],
                DocumentName= "AWS-RunShellScript",
                Parameters={
                    "commands": [combined],
                    "executionTimeout": ["3600"],
                },
                TimeoutSeconds= 3600,
                Comment=        f"airflow-framework job on {instance_id}",
            )
        except ClientError as e:
            raise RuntimeError(
                f"  [AWS] Failed to send SSM command to {instance_id}: {e}"
            ) from e
        
        command_id = response["Command"]["CommandId"]
        print(f"    [AWS] SSM command sent: {command_id}")
        
        # Poll for completion 
        output_lines = []
        
        for attempt in range(_SSM_MAX_POLL):
            time.sleep(_SSM_POLL_INTERVAL)
            try:
                result = self._ssm.get_command_invocation(
                    CommandId = command_id,
                    InstanceId = instance_id,
                )
            except ClientError as e:
                # InvocationDoesNotExist can happen briefly after send 
                if "InvocationDoesNotExist" in str(e):
                    continue
                raise RuntimeError(
                    f"  [AWS] Failed to get SSM invocation status: {e}"
                ) from e
            
            status = result.get("Status", "")
            stdout = result.get("StandardOutputContent", "")
            stderr = result.get("StandardErrorContent", "")
            
            # Stream any new output
            if stdout and stdout not in output_lines:
                output_lines.append(stdout)
                for line in stdout.splitlines():
                    print(f"    [EC2:{instance_id}] {line}")
            
            print(
                f"  [AWS] SSM status: {status}"
                f"(attempts {attempt + 1}/{_SSM_MAX_POLL})"
            )
            
            if status == _SSM_SUCCESS:
                print(f"    [AWS] commands completed successfully")
                return "\n".join(output_lines)
            
            if status in _SSM_TERMINAL:
                error_detail = stderr or result.get("StatusDetail", "")
                raise RuntimeError(
                    f"  [AWS] SSM command failed on {instance_id} \n"
                    f"      Status: {status}\n"
                    f"      Details: {error_detail}\n"
                    f"      Stdout: {stdout}"
                )
        raise TimeoutError(
            f"  [AWS] SSM command {command_id} did not complete "
            f"within {_SSM_MAX_POLL * _SSM_POLL_INTERVAL // 60} minutes "
            f"on {instance_id}"
        )
    
    # Terminate
    def terminate(self, instance: ServerInstance) -> Nne:
        """
        Terminate the EC2 instance permanently 
        Safe to call if already, terminated - idempotent.
        """
        instance_id = instance.instance_id
        
        try:
            self._ec2.terminate_instances(InstanceIds=[instance_id])
            print(f"    [AWS] termination requested: {instance_id}")
            
            # Wait for confirmed terminated state
            waiter = self._ec2.get_waiter("instance_terminated")
            waiter.wait(
                InstanceIds=[instance_id],
                WaiterConfig={
                    "Delay": _WAITER_DELAY,
                    "MaxAttempts": _WAITER_MAX_ATTEMPTS,
                },
            )
            print(f"    [AWS] terminated: {instance_id}")
        
        except ClientError as e:
            # Invalid InstanceID.NotFound - already gone, that's fine
            if "InvalidInstanceID" in str(e):
                print(
                    f"  [AWS] instance {instance_id} already terminated "
                    f"or not found - skipping"
                )
                return 

            raise RuntimeError(
                f"  [AWS] failed to terminate {instance_id}: {e}"
            ) from e 
        
        except WaiterError as e:
            raise RuntimeError(
                f"  [AWS] Instance {instance_id} did not reach terminated"
                f"state within expected time: {e}"
            ) from e 