"""
File-level restore operator tests.

Validates restoring specific files from a VolumeSnapshot or PVC to a running VM
using the VirtualMachineFileRestore CR.
"""

import pytest

from tests.storage.file_level_restore.utils import (
    BACKUP_MOUNT_PREFIX,
    VirtualMachineFileRestore,
    wait_for_file_restore_phase,
)
from utilities.storage import run_command_on_vm_and_check_output


@pytest.mark.usefixtures("file_restore_operator")
class TestFileRestoreDataDisk:
    @pytest.mark.polarion("CNV-xxx1")
    def test_manual_file_restore_from_data_disk_snapshot(
        self,
        admin_client,
        namespace,
        file_restore_vm_with_data_disk,
        data_disk_snapshot,
        test_file_on_data_disk,
    ):
        """Test manual restore from a data disk VolumeSnapshot source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM with a formatted and mounted data disk, configured with filerestore user and helper script
            - VolumeSnapshot of the data disk containing a test file

        Steps:
            1. Create a VirtualMachineFileRestore CR with VolumeSnapshot source, without sourcePath (manual mode)
            2. Wait for VolumeReady phase

        Expected:
            The backup volume is mounted and the test file
            is accessible at the backup path
        """
        test_file_path, test_file_content = test_file_on_data_disk
        with VirtualMachineFileRestore(
            name="test-file-restore-data-disk",
            namespace=namespace.name,
            target_vm_name=file_restore_vm_with_data_disk.name,
            source_snapshot_name=data_disk_snapshot.name,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.VOLUME_READY,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm_with_data_disk,
                command=f"cat {BACKUP_MOUNT_PREFIX}-{data_disk_snapshot.name}{test_file_path}",
                expected_result=test_file_content,
            )


@pytest.mark.usefixtures("file_restore_operator")
class TestFileRestore:
    @pytest.mark.polarion("CNV-xxx2")
    def test_manual_file_restore_from_snapshot(
        self,
        admin_client,
        namespace,
        file_restore_vm,
        vm_disk_snapshot,
        test_file_on_vm,
    ):
        """Test manual restore from VolumeSnapshot source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM configured with filerestore user and helper script
            - VolumeSnapshot of the VM's root disk containing a test file

        Steps:
            1. Create a VirtualMachineFileRestore CR with VolumeSnapshot source, without sourcePath (manual mode)
            2. Wait for VolumeReady phase

        Expected:
            The backup volume is mounted and the test file
            is accessible at the backup path
        """
        test_file_path, test_file_content = test_file_on_vm
        with VirtualMachineFileRestore(
            name="test-file-restore-snapshot",
            namespace=namespace.name,
            target_vm_name=file_restore_vm.name,
            source_snapshot_name=vm_disk_snapshot.name,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.VOLUME_READY,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm,
                command=f"cat {BACKUP_MOUNT_PREFIX}-{vm_disk_snapshot.name}{test_file_path}",
                expected_result=test_file_content,
            )

    @pytest.mark.polarion("CNV-xxx3")
    def test_manual_file_restore_from_pvc(
        self,
        admin_client,
        namespace,
        file_restore_vm,
        backup_pvc,
        test_file_on_vm,
    ):
        """Test manual restore from PVC source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM configured with filerestore user and helper script
            - PVC created from a VolumeSnapshot of the VM's root disk containing a test file

        Steps:
            1. Create a VirtualMachineFileRestore CR with PVC source, without sourcePath (manual mode)
            2. Wait for VolumeReady phase

        Expected:
            The backup volume is mounted and the test file
            is accessible at the backup path
        """
        test_file_path, test_file_content = test_file_on_vm
        with VirtualMachineFileRestore(
            name="test-file-restore-pvc",
            namespace=namespace.name,
            target_vm_name=file_restore_vm.name,
            source_pvc_name=backup_pvc.name,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.VOLUME_READY,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm,
                command=f"cat {BACKUP_MOUNT_PREFIX}-{backup_pvc.name}{test_file_path}",
                expected_result=test_file_content,
            )
