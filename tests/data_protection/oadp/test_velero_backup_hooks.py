import logging

import pytest

from utilities.constants import TIMEOUT_15MIN
from utilities.oadp import VeleroBackup, get_velero_backup_logs
from utilities.virt import wait_for_running_vm

LOGGER = logging.getLogger(__name__)

HOOK_LOG_PATTERN = "freeze"

pytestmark = pytest.mark.usefixtures("skip_if_no_storage_class_for_snapshot")


class TestVeleroBackupHookOptOut:
    """
    Tests for Velero backup hook opt-out with paused VMs and full backup/restore.

    STP: https://github.com/RedHatQE/openshift-virtualization-tests-design-docs/pull/116

    Preconditions:
        - OADP operator installed and configured
        - Velero configured with default backup storage location
    """

    @pytest.mark.polarion("CNV-16267")
    def test_backup_paused_vm_hooks_disabled(
        self,
        admin_client,
        namespace_for_backup,
        rhel_vm_with_hooks_opt_out,
    ):
        """
        Test that Velero backup of a paused VM completes with hooks disabled.

        Preconditions:
            - VM deployed with kubevirt.io/skip-backup-hooks: "true"

        Steps:
            1. Pause the running VM
            2. Run Velero backup targeting the VM namespace
            3. Wait for Velero backup to complete

        Expected:
            - Backup completes with status Completed
        """
        rhel_vm_with_hooks_opt_out.vmi.pause(wait=True)
        LOGGER.info(f"VM {rhel_vm_with_hooks_opt_out.name} paused")

        with VeleroBackup(
            name="backup-paused-optout",
            client=admin_client,
            included_namespaces=[namespace_for_backup.name],
        ) as backup:
            LOGGER.info(f"Backup {backup.name} completed with hooks disabled on paused VM")

    @pytest.mark.polarion("CNV-16268")
    @pytest.mark.usefixtures("velero_restore_vm_with_hooks_opt_out")
    def test_full_backup_restore_hooks_disabled(
        self,
        rhel_vm_with_hooks_opt_out,
    ):
        """
        Test that full Velero backup and restore completes with hooks disabled.

        Preconditions:
            - Running VM deployed with kubevirt.io/skip-backup-hooks: "true"

        Steps:
            1. Run Velero backup targeting the VM namespace
            2. Delete the VM and its namespace
            3. Restore from backup
            4. Wait for VM to reach Running state

        Expected:
            - VM is Running
        """
        wait_for_running_vm(
            vm=rhel_vm_with_hooks_opt_out,
            wait_until_running_timeout=TIMEOUT_15MIN,
            check_ssh_connectivity=False,
        )

    @pytest.mark.polarion("CNV-16269")
    def test_backup_paused_vm_default_hooks(
        self,
        admin_client,
        namespace_for_backup,
        rhel_vm_with_default_hooks,
    ):
        """
        Test that Velero backup of a paused VM attempts hooks by default.

        Preconditions:
            - VM deployed without opt-out annotation

        Steps:
            1. Pause the running VM
            2. Run Velero backup targeting the VM namespace
            3. Wait for Velero backup to complete
            4. Check Velero backup logs for hook execution entries

        Expected:
            - Backup completes with status Completed
            - Backup logs contain freeze/unfreeze hook entries
        """
        rhel_vm_with_default_hooks.vmi.pause(wait=True)
        LOGGER.info(f"VM {rhel_vm_with_default_hooks.name} paused")

        with VeleroBackup(
            name="backup-paused-default",
            client=admin_client,
            included_namespaces=[namespace_for_backup.name],
        ) as backup:
            LOGGER.info(f"Backup {backup.name} completed with default hooks on paused VM")

        backup_logs = get_velero_backup_logs(backup_name=backup.name, client=admin_client)
        assert HOOK_LOG_PATTERN in backup_logs.lower(), (
            f"Backup {backup.name} logs do not contain hook entries but hooks should be enabled"
        )
