"""
File-level restore operator tests.

Validates restoring specific files from a VolumeSnapshot or PVC to a running VM
using the VirtualMachineFileRestore CR.
"""

import pytest

from tests.storage.file_level_restore.conftest import TEST_FILE_PATH
from tests.storage.file_level_restore.utils import (
    BACKUP_MOUNT_PREFIX,
    VirtualMachineFileRestore,
    wait_for_file_restore_phase,
)
from utilities.storage import run_command_on_vm_and_check_output


@pytest.mark.usefixtures("enabled_declarative_hotplug_volumes", "file_restore_operator")
class TestFileRestoreDataDisk:
    @pytest.mark.polarion("CNV-xxx1")
    def test_automatic_file_restore_from_data_disk_snapshot(
        self,
        admin_client,
        namespace,
        file_restore_vm_with_data_disk,
        data_disk_snapshot,
        deleted_test_file_on_data_disk,
    ):
        """Test automatic restore from a data disk VolumeSnapshot source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM with a formatted and mounted data disk, configured with filerestore user and helper script
            - VolumeSnapshot of the data disk containing a test file
            - Test file deleted from the data disk after snapshot

        Steps:
            1. Create a VirtualMachineFileRestore CR with VolumeSnapshot source and sourcePath (automatic mode)
            2. Wait for Succeeded phase

        Expected:
            The test file is restored to its original location
            on the data disk with the original content
        """
        relative_path, test_file_content = deleted_test_file_on_data_disk
        with VirtualMachineFileRestore(
            name="test-file-restore-data-disk",
            namespace=namespace.name,
            target_vm_name=file_restore_vm_with_data_disk.name,
            source_snapshot_name=data_disk_snapshot.name,
            source_path=relative_path,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.SUCCEEDED,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm_with_data_disk,
                command=f"cat {relative_path}",
                expected_result=test_file_content,
            )


@pytest.mark.usefixtures("enabled_declarative_hotplug_volumes", "file_restore_operator")
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

    @pytest.mark.polarion("CNV-xxx4")
    def test_automatic_file_restore_from_snapshot(
        self,
        admin_client,
        namespace,
        file_restore_vm,
        vm_disk_snapshot,
        deleted_test_file,
    ):
        """Test automatic restore from VolumeSnapshot source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM configured with filerestore user and helper script
            - VolumeSnapshot of the VM's root disk containing a test file
            - Test file deleted from the VM after snapshot

        Steps:
            1. Create a VirtualMachineFileRestore CR with VolumeSnapshot source and sourcePath (automatic mode)
            2. Wait for Succeeded phase

        Expected:
            The test file is restored to its original location
            with the original content
        """
        test_file_path, test_file_content = deleted_test_file
        with VirtualMachineFileRestore(
            name="test-auto-restore-snapshot",
            namespace=namespace.name,
            target_vm_name=file_restore_vm.name,
            source_snapshot_name=vm_disk_snapshot.name,
            source_path=TEST_FILE_PATH,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.SUCCEEDED,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm,
                command=f"cat {test_file_path}",
                expected_result=test_file_content,
            )

    @pytest.mark.polarion("CNV-xxx5")
    def test_automatic_file_restore_from_pvc(
        self,
        admin_client,
        namespace,
        file_restore_vm,
        backup_pvc,
        deleted_test_file,
    ):
        """Test automatic restore from PVC source.

        Preconditions:
            - File-restore operator installed in the cluster
            - Running RHEL VM configured with filerestore user and helper script
            - PVC created from a VolumeSnapshot of the VM's root disk containing a test file
            - Test file deleted from the VM after snapshot

        Steps:
            1. Create a VirtualMachineFileRestore CR with PVC source and sourcePath (automatic mode)
            2. Wait for Succeeded phase

        Expected:
            The test file is restored to its original location
            with the original content
        """
        test_file_path, test_file_content = deleted_test_file
        with VirtualMachineFileRestore(
            name="test-auto-restore-pvc",
            namespace=namespace.name,
            target_vm_name=file_restore_vm.name,
            source_pvc_name=backup_pvc.name,
            source_path=TEST_FILE_PATH,
            client=admin_client,
        ) as file_restore:
            wait_for_file_restore_phase(
                file_restore=file_restore,
                target_phase=VirtualMachineFileRestore.Phase.SUCCEEDED,
            )
            run_command_on_vm_and_check_output(
                vm=file_restore_vm,
                command=f"cat {test_file_path}",
                expected_result=test_file_content,
            )
