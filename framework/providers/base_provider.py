# framwork/providers/base_provider.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from framework.config_loader import ServerConfig

# ─────────────────────────────────────────────────────────────────────────────
# Server state — returned by launch() and used by ec2_executor
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ServerInstance:
    """
    Represents a running cloud server instance.
    Provider-agnostic - cloud_executor only interacts with this dataclass, 
    never with provier-specific objects directly 
    
    Attributes:
        instance_id:    cloud provider instance identifier
                        AWS  → "i-0abc123def456"
                        GCP  → "my-instance-name"
                        OVH  → "server-uuid"
                        Azure→ "vm-resource-id"
        public_ip:      public IP address for SSH if needed
        private_ip:     private IP address
        provider:       which provider manages this instance
        region:         region/zone where instance is running
        metadata:       provider-specific extra info (tags, zone, etc.)
                        stored here to avoid polluting the base dataclass
        is_ready:       True once wait_ready() confirms instance is usable
    """
    instance_id: str
    provider:    str
    region:      str
    public_ip:   str                    = ""
    private_ip:  str                    = ""
    metadata:    dict[str, Any]         = field(default_factory=dict)
    is_ready:    bool                   = False
    
    def __repr__(self) -> str:
        return(
            "ServerInstance("
            f"id={self.instance_id}, "
            f"provider={self.provider}, "
            f"region={self.region}, "
            f"ip={self.public_ip or self.private_ip}, "
            f"ready={self.is_ready})"
        )
        
# ─────────────────────────────────────────────────────────────────────────────
# Base provider — all cloud providers implement this interface
# ─────────────────────────────────────────────────────────────────────────────

class BaseProvider(ABC):
    """
    Abstract base class for cloud server providers.
    
    Each provider (AWS, GCP, Azure, OVH) implements:
        - launch() spin up a new instance
        - wait_ready() block until instance accepts commands
        - run_commands() execute shell commands on the instance 
        - terminate() destroy the instance 
    
    cloud_executor interacts only with this interface - 
    switching cloud provider requires zero changes to the executor
    """
    
    def __init__(self, config: ServerConfig):
        self.config = config
    
    # Abstract method - must be implemented by each provider
    
    @abstractmethod
    def launch(self) -> ServerInstance:
        """
        Spin up a new server instance

        Returns:
            ServerInstance: with instance_id populated
            is_ready will be False - can wait_ready() after
        
        Raises:
            RunTimeError: if the instance couldn't be launched 
        """
        ...
    
    @abstractmethod
    def wait_ready(self, instance: ServerInstance) -> ServerInstance:
        """
        Block until the instance is ready to accept commands.
        Updates instance.is_ready = True on success.

        Args:
            instance (ServerInstance): ServerInstance returned by launch()

        Returns:
            ServerInstance: same ServerInstance with is_ready=True and IPs populated
            
        Raises:
            TimeoutError: if instance doesn't become ready with config.startup_timeout_minutes
            RuntimeError: if instance enters a failed state
        """
        ...
        
        
    @abstractmethod
    def run_command(
        self,
        instance: ServerInstance,
        commands: list[str],
    ) -> str:
        """
        Execute a list of shell commands on the running instance.
        Commands run sequentially - if one fails the rest are skipped

        Args:
            instance (ServerInstance): ready ServerInstance (is_ready must be True)
            commands (list[str]): list of shell command strings

        Returns:
            str: Combined stdout output from all commands
            
        Raises:
            RuntimeError: if any command exits non-zero
            ValueError:   if instance.is_ready is False
        """
        ...
        
    @abstractmethod
    def terminate(self, instance: ServerInstance) -> None:
        """
        Destroy the instance permanently 
        Safe to call even if the instance is already terminated

        Args:
            instance (ServerInstance): ServerInstance to terminate
        
        Raises:
            RuntimeError: if termination fails after retries.
        """
        ...
        
    # Concret methods - shared across all providers
    
    def launch_and_wait(self) -> ServerInstance:
        """
        Convenience: launch() then wait_ready() in one call
        used by cloud_executor when launching a new segment

        Returns:
            ServerInstance: ready ServerInstance
        """
        print(
            f"  [provider: {self.config.provider}] launching."
            f"{self.config.instance_type} in {self.config.region}"
        )
        instance = self.launch()
        print(f"    [provider:{self.config.provider}] launched: {instance.instance_id}")
        print(f"    [provider:{self.config.provider}] waiting for read...")
        instance = self.wait_ready(instance)
        print(f"    [provider:{self.config.provider}] ready: {instance}")
        
        return instance 

    def terminate_safe(self, instance: ServerInstance) -> None:
        """
        Terminate with error swallowing - used in finally blocks 
        where we want to guarantee cleanup even if something else failed
        Logs the error but doesn't raise
        """
        try:
            self.terminate(instance)
            print(
                f"  [provider:{self.config.provider}]"
                f"terminated: {instance.instance_id}"
            )
        except Exception as e:
            print(
                f"  [provider:{self.config.provider}] WARNING: "
                f"termination failed for {instance.instance_id}: {e}\n"
                f"  Manual cleanup may be required"
            )
    
    def validate_ready(self, instance: ServerInstance) -> None:
        """
        Guard - raises ValueError if instance is not ready
        Called at teh start of run_command() by all implementation
        """
        if not instance.is_ready:
            raise ValueError(
                f"Cannot run commands on instance {instance.instance_id} "
                f"- instance is not ready. Call wait_ready() first."
            )
        
    # Factory method - instantiate correct provider from config
    
    @staticmethod
    def from_config(config: ServerConfig) -> "BaseProvider":
        """
        Instantiate the correct provider implementation based on config.provider.

        Args:
            config (ServerConfig): ServerConfig with provider field set

        Returns:
            BaseProvider: Concrete provider instance 
        
        Raises:
            ValueError: if provider is not supporter
        """
        provider = config.provider.lower()
        
        if provider == "aws":
            from framework.providers.aws.provider import AWSProvider
            return AWSProvider(config)
        elif provider == "gcp":
            from framework.providers.gcp.provider import GCPProvider
            return GCPProvider(config)
        elif provider == "azure":
            from framework.providers.azure.provider import AzureProvider
            return AzureProvider(config)
        elif provider == "ovh":
            from framework.providers.ovh.provider import OVHProvider
            return OVHProvider(config)
        else:  
            raise ValueError(
                f"Unsupported provider: '{provider}'."
                f"Must be one of: aws, gcp, azure, ovh"
            )