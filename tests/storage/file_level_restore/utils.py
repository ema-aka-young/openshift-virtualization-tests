from __future__ import annotations

import base64
import logging
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ocp_resources.config_map import ConfigMap
from ocp_resources.deployment import Deployment
from ocp_resources.resource import NamespacedResource
from pyhelper_utils.shell import run_command, run_ssh_commands
from timeout_sampler import TimeoutSampler

from utilities.constants import TIMEOUT_2MIN, TIMEOUT_5MIN, TIMEOUT_5SEC

if TYPE_CHECKING:
    from kubernetes.dynamic import DynamicClient

    from utilities.virt import VirtualMachineForTests

LOGGER = logging.getLogger(__name__)

_MANIFESTS_DIR = Path(__file__).parent / "manifests"

FILE_RESTORE_OPERATOR_NAMESPACE = "file-restore"
FILE_RESTORE_SSH_CONFIGMAP_NAME = "vm-file-restore-operator-ssh"
FILE_RESTORE_OPERATOR_DEPLOYMENT_NAME = "vm-file-restore-operator"
FILERESTORE_SCRIPT_PATH = str(_MANIFESTS_DIR / "filerestore.sh")
FILE_RESTORE_OPERATOR_INSTALL_YAML = str(_MANIFESTS_DIR / "install.yaml")
BACKUP_MOUNT_PREFIX = "/backup"
_OPERATOR_REPO_RAW_URL = "https://raw.githubusercontent.com/kubevirt/vm-file-restore-operator/refs/heads/main"
SETUP_SCRIPT_URL = f"{_OPERATOR_REPO_RAW_URL}/guest-helpers/linux/setup.sh"


class VirtualMachineFileRestore(NamespacedResource):
    """KubeVirt VirtualMachineFileRestore custom resource."""

    api_group: str = "filerestore.kubevirt.io"

    class Phase:
        NEW = "New"
        INIT = "Init"
        HOTPLUGGING = "Hotplugging"
        WAITING_FOR_ATTACHMENT = "WaitingForAttachment"
        SSH_CONNECTING = "SSHConnecting"
        RESTORING = "Restoring"
        VOLUME_READY = "VolumeReady"
        CLEANUP = "Cleanup"
        SUCCEEDED = "Succeeded"
        FAILED = "Failed"

    def __init__(
        self,
        target_vm_name: str,
        source_snapshot_name: str | None = None,
        source_pvc_name: str | None = None,
        source_path: str | None = None,
        target_path: str | None = None,
        source_partition: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a VirtualMachineFileRestore resource.

        Args:
            target_vm_name: Name of the target VirtualMachine.
            source_snapshot_name: VolumeSnapshot name to restore from.
            source_pvc_name: PVC name to restore from.
            source_path: Path on the source volume to restore.
            target_path: Destination path on the target VM.
            source_partition: Partition number on the source volume.
            **kwargs: Additional arguments passed to NamespacedResource.
        """
        super().__init__(**kwargs)
        self._target_vm_name = target_vm_name
        self._source_snapshot_name = source_snapshot_name
        self._source_pvc_name = source_pvc_name
        self._source_path = source_path
        self._target_path = target_path
        self._source_partition = source_partition

    def to_dict(self) -> None:
        super().to_dict()
        if not self.kind_dict and not self.yaml_file:
            spec: dict[str, Any] = {
                "target": {
                    "apiGroup": "kubevirt.io",
                    "kind": "VirtualMachine",
                    "name": self._target_vm_name,
                },
                "source": {},
            }

            if self._source_snapshot_name:
                spec["source"]["snapshot"] = {"name": self._source_snapshot_name}
            elif self._source_pvc_name:
                spec["source"]["pvc"] = {"name": self._source_pvc_name}

            if self._source_path is not None:
                spec["sourcePath"] = self._source_path
            if self._target_path is not None:
                spec["targetPath"] = self._target_path
            if self._source_partition is not None:
                spec["sourcePartition"] = self._source_partition

            self.res.setdefault("spec", {}).update(spec)


def wait_for_file_restore_phase(
    file_restore: VirtualMachineFileRestore,
    target_phase: str,
    timeout: int = TIMEOUT_5MIN,
) -> None:
    """Wait for VirtualMachineFileRestore to reach a target phase.

    Args:
        file_restore: The VirtualMachineFileRestore resource to monitor.
        target_phase: The phase to wait for (e.g. Phase.VOLUME_READY, Phase.SUCCEEDED).
        timeout: Maximum wait time in seconds.

    Raises:
        AssertionError: If the restore reaches Failed phase.
    """
    LOGGER.info(f"Waiting for VirtualMachineFileRestore '{file_restore.name}' to reach phase '{target_phase}'")
    for sample in TimeoutSampler(
        wait_timeout=timeout,
        sleep=TIMEOUT_5SEC,
        func=lambda: file_restore.instance.get("status", {}),
    ):
        phase = sample.get("phase")
        LOGGER.info(f"VirtualMachineFileRestore '{file_restore.name}' phase: {phase}")
        if phase == target_phase:
            return
        if phase == VirtualMachineFileRestore.Phase.FAILED:
            error_message = sample.get("errorMessage", "Unknown error")
            raise AssertionError(f"VirtualMachineFileRestore '{file_restore.name}' failed: {error_message}")


def install_file_restore_operator(install_yaml_path: str) -> None:
    """Install the file-restore operator from a manifest file.

    Args:
        install_yaml_path: Path to the operator install.yaml manifest.
    """
    LOGGER.info(f"Installing file-restore operator from {install_yaml_path}")
    run_command(command=shlex.split(f"oc apply -f {install_yaml_path}"))

    LOGGER.info("Waiting for file-restore operator deployment to be ready")
    deployment = Deployment(
        name=FILE_RESTORE_OPERATOR_DEPLOYMENT_NAME,
        namespace=FILE_RESTORE_OPERATOR_NAMESPACE,
    )
    deployment.wait_for_replicas(timeout=TIMEOUT_5MIN)
    LOGGER.info("File-restore operator deployment is ready")


def uninstall_file_restore_operator(install_yaml_path: str) -> None:
    """Uninstall the file-restore operator.

    Args:
        install_yaml_path: Path to the operator install.yaml manifest.
    """
    LOGGER.info(f"Uninstalling file-restore operator from {install_yaml_path}")
    run_command(
        command=shlex.split(f"oc delete -f {install_yaml_path} --ignore-not-found"),
    )


def get_operator_ssh_public_key(admin_client: DynamicClient) -> str:
    """Retrieve the SSH public key from the file-restore operator's ConfigMap.

    Args:
        admin_client: Kubernetes admin client.

    Returns:
        The operator's SSH public key string.
    """
    LOGGER.info("Waiting for file-restore operator SSH ConfigMap")
    for sample in TimeoutSampler(
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
        func=lambda: (
            ConfigMap(
                name=FILE_RESTORE_SSH_CONFIGMAP_NAME,
                namespace=FILE_RESTORE_OPERATOR_NAMESPACE,
                client=admin_client,
            ).instance
        ),
    ):
        if sample:
            ssh_public_key = sample.data.get("ssh-publickey", "")
            if ssh_public_key:
                LOGGER.info("Retrieved file-restore operator SSH public key")
                return ssh_public_key
    raise RuntimeError("Failed to retrieve file-restore operator SSH public key")


def configure_vm_for_file_restore(
    vm: VirtualMachineForTests,
    ssh_public_key: str,
    filerestore_script_path: str | None = None,
) -> None:
    """Configure a VM for file-restore operator operations.

    Downloads and runs the upstream setup.sh from the vm-file-restore-operator
    GitHub repository. The setup script creates the filerestore user, installs
    the SSH key with command restriction, configures sudoers, and downloads
    the filerestore.sh helper script.

    Optionally overwrites the helper script with a local version containing
    fixes not yet merged upstream.

    Args:
        vm: The running VM to configure.
        ssh_public_key: The operator's SSH public key to install.
        filerestore_script_path: Optional local path to filerestore.sh to
            override the version downloaded by setup.sh.
    """
    LOGGER.info(f"Configuring VM '{vm.name}' via upstream setup.sh")
    setup_command = f"sudo bash -c 'curl -sL {SETUP_SCRIPT_URL} | bash -s -- \"{ssh_public_key}\"'"
    run_ssh_commands(
        host=vm.ssh_exec,
        commands=shlex.split(setup_command),
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
    )

    if filerestore_script_path:
        LOGGER.info(f"Overriding filerestore.sh with local version from {filerestore_script_path}")
        with open(filerestore_script_path) as script_file:
            script_content = script_file.read()

        encoded_script = base64.b64encode(script_content.encode()).decode()
        install_script_command = (
            f"sudo bash -c 'echo {encoded_script} | base64 -d"
            f" > /usr/local/bin/filerestore.sh && chmod +x /usr/local/bin/filerestore.sh'"
        )
        run_ssh_commands(
            host=vm.ssh_exec,
            commands=shlex.split(install_script_command),
            wait_timeout=TIMEOUT_2MIN,
            sleep=TIMEOUT_5SEC,
        )

    LOGGER.info(f"VM '{vm.name}' configured for file-restore operator")


def vm_root_disk_pvc_name(vm: VirtualMachineForTests) -> str:
    """Extract the PVC name of the VM's root disk.

    Args:
        vm: The VirtualMachine to inspect.

    Returns:
        The PVC name backing the VM's root disk.

    Raises:
        ValueError: If no root disk PVC is found.
    """
    for volume in vm.instance.spec.template.spec.volumes:
        if hasattr(volume, "dataVolume"):
            return volume.dataVolume.name
        if hasattr(volume, "persistentVolumeClaim"):
            return volume.persistentVolumeClaim.claimName
    raise ValueError(f"No root disk PVC found for VM '{vm.name}'")
