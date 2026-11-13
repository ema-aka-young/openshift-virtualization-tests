"""Regression coverage for Windows unfreeze timeouts during repeated Velero backups."""

from json import loads
from logging import getLogger

from pytest import mark

from tests.data_protection.oadp.utils import FILE_PATH_FOR_WINDOWS_BACKUP
from utilities.constants import ADP_NAMESPACE, TIMEOUT_5MIN
from utilities.infra import get_pod_by_name_prefix
from utilities.oadp import VeleroBackup
from utilities.storage import verify_file_in_windows_vm, write_file_windows_vm
from utilities.virt import get_vm_boot_time, wait_for_running_vm

LOGGER = getLogger(name=__name__)
pytestmark = mark.usefixtures("skip_if_no_storage_class_for_snapshot")


@mark.tier3
def test_repeated_backup_unfreezes_windows_vm(admin_client, windows_vm_with_data_volume_template):
    """Repeated snapshots successfully unfreeze the same running Windows guest.

    Preconditions:
        - CNV 4.22 build under verification, with its exact version recorded
        - Running under-test Windows Server 2022 VM with a connected QEMU guest
          agent, working VSS support, and recorded VirtIO and guest-agent versions
        - Snapshot-capable storage and a working Velero backup storage location
        - Pre-backup freeze and post-backup unfreeze hooks enabled for the
          under-test VM, with hook errors configured to fail the backup
        - Hook timeout settings recorded and unchanged throughout the run

    Steps:
        1. Record the under-test VM's identity and guest boot time.
        2. Create a snapshot-based Velero backup of the under-test VM without
           Data Mover and wait for completion before starting another backup.
        3. Check that both freeze and unfreeze hooks succeeded, recording their
           durations and preserving error details if either hook fails.
        4. Check that the guest agent responds and the guest is unfrozen, then
           write and read back fresh data inside the under-test VM.
        5. Repeat steps 2 through 4 until 100 backups have completed on the same
           VM without restarting the VM or its guest agent. Stop on the first
           failure and preserve the backup and hook diagnostics.
        6. Confirm that the VM identity and guest boot time are unchanged.

    Expected:
        - All 100 backups complete with successful freeze and unfreeze hooks,
          leaving the same uninterrupted guest responsive and writable after
          every backup, without unfreeze deadline or stream-cancellation errors.
    """
    vm = windows_vm_with_data_volume_template
    initial_vmi_uid = vm.vmi.instance.metadata.uid
    initial_boot_time = get_vm_boot_time(vm=vm)
    hook_annotations = vm.vmi.virt_launcher_pod.instance.metadata.annotations
    for hook_phase, freeze_option in (("pre", "--freeze"), ("post", "--unfreeze")):
        hook_prefix = f"{hook_phase}.hook.backup.velero.io"
        assert hook_annotations[f"{hook_prefix}/container"] == "compute", (
            f"{hook_phase} hook for VM {vm.name} must execute in the compute container"
        )
        assert loads(hook_annotations[f"{hook_prefix}/command"]) == [
            "/usr/bin/virt-freezer",
            freeze_option,
            "--name",
            vm.name,
            "--namespace",
            vm.namespace,
        ], f"Unexpected {hook_phase} hook command for VM {vm.name}: {hook_annotations}"
        assert hook_annotations.get(f"{hook_prefix}/on-error", "Fail") == "Fail", (
            f"{hook_phase} hook for VM {vm.name} must fail the backup on error"
        )
    LOGGER.info(f"VM {vm.name}: VMI UID={initial_vmi_uid}, boot time={initial_boot_time}, hooks={hook_annotations}")
    LOGGER.info(f"Initial guest-agent information for VM {vm.name}: {vm.vmi.guest_os_info}")
    velero_pod = get_pod_by_name_prefix(dyn_client=admin_client, pod_prefix="velero", namespace=ADP_NAMESPACE)
    for backup_number in range(1, 101):
        LOGGER.info(f"Starting backup {backup_number}/100 of running VM {vm.name}")
        with VeleroBackup(
            name=f"windows-unfreeze-{backup_number}",
            client=admin_client,
            included_namespaces=[vm.namespace],
            wait_complete=False,
        ) as backup:
            try:
                backup.wait_for_status(status=backup.Status.COMPLETED, timeout=TIMEOUT_5MIN)
            finally:
                backup_status = backup.instance.to_dict()["status"]
                LOGGER.info(f"Backup {backup.name} status: {backup_status}")
                backup_logs = velero_pod.execute(command=["./velero", "backup", "logs", backup.name])
                for hook_phase in ("pre", "post"):
                    hook_logs = "\n".join(
                        log_line for log_line in backup_logs.splitlines() if f"hookPhase={hook_phase}" in log_line
                    )
                    LOGGER.info(f"Backup {backup.name} {hook_phase}-backup hook logs:\n{hook_logs}")
                if backup_status["phase"] != backup.Status.COMPLETED:
                    LOGGER.info(f"Backup {backup.name} full logs:\n{backup_logs}")
            assert backup_status.get("csiVolumeSnapshotsCompleted", 0) > 0, (
                f"Backup {backup.name} completed without a CSI volume snapshot: {backup_status}"
            )
            hook_status = backup_status["hookStatus"]
            assert hook_status["hooksAttempted"] == 2 and not hook_status.get("hooksFailed", 0), (
                f"Backup {backup.name} did not complete both freeze/unfreeze hooks successfully: {hook_status}"
            )
            wait_for_running_vm(vm=vm)
            file_path = FILE_PATH_FOR_WINDOWS_BACKUP.replace(".txt", f"_{backup_number}.txt")
            write_file_windows_vm(vm=vm, file_path=file_path, content=backup.name)
            verify_file_in_windows_vm(windows_vm=vm, file_name_with_path=file_path, file_content=backup.name)
            assert vm.vmi.instance.metadata.uid == initial_vmi_uid, f"VMI for VM {vm.name} restarted during backup"
            assert get_vm_boot_time(vm=vm) == initial_boot_time, f"Guest VM {vm.name} rebooted during backup"
