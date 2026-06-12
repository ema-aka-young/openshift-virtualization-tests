import logging
import shlex

import pytest
from ocp_resources.datavolume import DataVolume
from ocp_resources.kubevirt import KubeVirt
from ocp_resources.virtual_machine_cluster_instancetype import VirtualMachineClusterInstancetype
from ocp_resources.virtual_machine_cluster_preference import VirtualMachineClusterPreference
from ocp_resources.volume_snapshot import VolumeSnapshot
from pyhelper_utils.shell import run_ssh_commands

from tests.storage.file_level_restore.utils import (
    FILE_RESTORE_OPERATOR_INSTALL_YAML,
    configure_vm_for_file_restore,
    get_operator_ssh_public_key,
    install_file_restore_operator,
    uninstall_file_restore_operator,
    vm_root_disk_pvc_name,
)
from utilities.constants import (
    OS_FLAVOR_RHEL,
    RHEL10_PREFERENCE,
    TIMEOUT_2MIN,
    TIMEOUT_5MIN,
    TIMEOUT_5SEC,
    U1_SMALL,
)
from utilities.hco import ResourceEditorValidateHCOReconcile
from utilities.storage import (
    add_dv_to_vm,
    data_volume_template_with_source_ref_dict,
    volume_snapshot_class_for_sc,
    wait_for_volume_snapshot_ready_to_use,
    write_file_via_ssh,
)
from utilities.virt import VirtualMachineForTests, running_vm

LOGGER = logging.getLogger(__name__)

TEST_FILE_PATH = "/var/tmp/test-restore.txt"
TEST_FILE_CONTENT = "file-restore-test-content"
DATA_DISK_DEVICE = "/dev/vdc"
DATA_DISK_MOUNT_PATH = "/mnt/data"
BIG_FILE_RELATIVE_PATH = "/big-test-file.bin"
BIG_FILE_SIZE_BYTES = str(1024 * 1024 * 1024)


@pytest.fixture(scope="session")
def enabled_declarative_hotplug_volumes(hyperconverged_resource_scope_session):
    """Enable the declarativeHotplugVolumes feature gate on HyperConverged."""
    with ResourceEditorValidateHCOReconcile(
        patches={
            hyperconverged_resource_scope_session: {"spec": {"featureGates": {"declarativeHotplugVolumes": True}}},
        },
        list_resource_reconcile=[KubeVirt],
        wait_for_reconcile_post_update=True,
    ):
        yield


@pytest.fixture(scope="session")
def file_restore_operator():
    """Installed file-restore operator, uninstalled on teardown."""
    install_file_restore_operator(install_yaml_path=FILE_RESTORE_OPERATOR_INSTALL_YAML)
    yield
    uninstall_file_restore_operator(install_yaml_path=FILE_RESTORE_OPERATOR_INSTALL_YAML)


@pytest.fixture(scope="session")
def file_restore_ssh_public_key(file_restore_operator, admin_client):
    """SSH public key from the file-restore operator's ConfigMap."""
    return get_operator_ssh_public_key(admin_client=admin_client)


@pytest.fixture(scope="module")
def file_restore_vm(
    admin_client,
    namespace,
    rhel10_data_source_scope_session,
    snapshot_storage_class_name_scope_module,
    file_restore_ssh_public_key,
):
    """Running RHEL10 VM configured with the filerestore user, SSH key, and helper script."""
    with VirtualMachineForTests(
        name="file-restore-test-vm",
        namespace=namespace.name,
        client=admin_client,
        os_flavor=OS_FLAVOR_RHEL,
        vm_instance_type=VirtualMachineClusterInstancetype(
            client=admin_client,
            name=U1_SMALL,
        ),
        vm_preference=VirtualMachineClusterPreference(
            client=admin_client,
            name=RHEL10_PREFERENCE,
        ),
        data_volume_template=data_volume_template_with_source_ref_dict(
            data_source=rhel10_data_source_scope_session,
            storage_class=snapshot_storage_class_name_scope_module,
        ),
    ) as vm:
        running_vm(vm=vm)
        configure_vm_for_file_restore(
            vm=vm,
            ssh_public_key=file_restore_ssh_public_key,
            # filerestore_script_path=FILERESTORE_SCRIPT_PATH,
        )
        yield vm


@pytest.fixture()
def test_file_on_vm(file_restore_vm):
    """Test file written to the VM's root filesystem. Yields (path, content)."""
    write_file_via_ssh(
        vm=file_restore_vm,
        filename=TEST_FILE_PATH,
        content=TEST_FILE_CONTENT,
    )
    yield TEST_FILE_PATH, TEST_FILE_CONTENT


@pytest.fixture()
def vm_disk_snapshot(
    file_restore_vm, test_file_on_vm, namespace, admin_client, snapshot_storage_class_name_scope_module
):
    """VolumeSnapshot of the VM's root disk, taken while the VM is stopped."""
    file_restore_vm.stop(wait=True)
    pvc_name = vm_root_disk_pvc_name(vm=file_restore_vm)
    vsc_name = volume_snapshot_class_for_sc(
        sc_name=snapshot_storage_class_name_scope_module,
        client=admin_client,
    )
    LOGGER.info(f"Creating VolumeSnapshot of PVC '{pvc_name}' with VolumeSnapshotClass '{vsc_name}'")
    with VolumeSnapshot(
        name="file-restore-snapshot",
        namespace=namespace.name,
        source={"persistentVolumeClaimName": pvc_name},
        volume_snapshot_class_name=vsc_name,
    ) as snapshot:
        wait_for_volume_snapshot_ready_to_use(
            namespace=namespace.name,
            name=snapshot.name,
        )
        LOGGER.info("VolumeSnapshot is ready, restarting VM")
        file_restore_vm.start(wait=True)
        running_vm(vm=file_restore_vm, wait_for_interfaces=True, check_ssh_connectivity=True)
        yield snapshot


@pytest.fixture()
def backup_pvc(vm_disk_snapshot, namespace):
    """PVC created from the VM root disk VolumeSnapshot."""
    LOGGER.info(f"Creating DataVolume from VolumeSnapshot '{vm_disk_snapshot.name}'")
    with DataVolume(
        name="file-restore-backup-pvc",
        namespace=namespace.name,
        source_dict={"snapshot": {"name": vm_disk_snapshot.name, "namespace": namespace.name}},
        api_name="storage",
    ) as data_volume:
        data_volume.wait_for_dv_success()
        LOGGER.info(f"Backup PVC '{data_volume.name}' ready")
        yield data_volume


@pytest.fixture()
def deleted_test_file(file_restore_vm, vm_disk_snapshot, test_file_on_vm):
    """Test file deleted from the running VM after snapshot. Yields (path, content)."""
    test_file_path, test_file_content = test_file_on_vm
    LOGGER.info(f"Deleting test file '{test_file_path}' from VM for automatic restore test")
    run_ssh_commands(
        host=file_restore_vm.ssh_exec,
        commands=shlex.split(f"rm -f {test_file_path}"),
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
    )
    yield test_file_path, test_file_content


@pytest.fixture()
def deleted_test_file_on_data_disk(file_restore_vm_with_data_disk, data_disk_snapshot, test_file_on_data_disk):
    """Test file deleted from the data disk after snapshot. Yields (relative_path, content)."""
    relative_path, test_file_content = test_file_on_data_disk
    full_path = f"{DATA_DISK_MOUNT_PATH}{relative_path}"
    LOGGER.info(f"Deleting test file '{full_path}' from data disk for automatic restore test")
    run_ssh_commands(
        host=file_restore_vm_with_data_disk.ssh_exec,
        commands=shlex.split(f"rm -f {full_path}"),
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
    )
    yield relative_path, test_file_content


@pytest.fixture()
def big_file_on_data_disk(file_restore_vm_with_data_disk):
    """1GB test file written to the data disk. Yields (relative_path, size_bytes_str)."""
    full_path = f"{DATA_DISK_MOUNT_PATH}{BIG_FILE_RELATIVE_PATH}"
    LOGGER.info(f"Creating 1GB test file at '{full_path}'")
    run_ssh_commands(
        host=file_restore_vm_with_data_disk.ssh_exec,
        commands=shlex.split(f"dd if=/dev/zero of={full_path} bs=1M count=1024 oflag=direct"),
        wait_timeout=TIMEOUT_5MIN,
        sleep=TIMEOUT_5SEC,
    )
    run_ssh_commands(
        host=file_restore_vm_with_data_disk.ssh_exec,
        commands=shlex.split("sync"),
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
    )
    yield BIG_FILE_RELATIVE_PATH, BIG_FILE_SIZE_BYTES


@pytest.fixture()
def big_file_data_disk_snapshot(
    file_restore_vm_with_data_disk,
    big_file_on_data_disk,
    blank_data_disk,
    namespace,
    admin_client,
    snapshot_storage_class_name_scope_module,
):
    """VolumeSnapshot of the data disk PVC containing the 1GB test file."""
    vsc_name = volume_snapshot_class_for_sc(
        sc_name=snapshot_storage_class_name_scope_module,
        client=admin_client,
    )
    LOGGER.info(f"Creating VolumeSnapshot of data disk PVC '{blank_data_disk.name}' with big file")
    with VolumeSnapshot(
        name="file-restore-big-file-snapshot",
        namespace=namespace.name,
        source={"persistentVolumeClaimName": blank_data_disk.name},
        volume_snapshot_class_name=vsc_name,
    ) as snapshot:
        wait_for_volume_snapshot_ready_to_use(
            namespace=namespace.name,
            name=snapshot.name,
        )
        yield snapshot


@pytest.fixture()
def deleted_big_file_on_data_disk(file_restore_vm_with_data_disk, big_file_data_disk_snapshot, big_file_on_data_disk):
    """1GB file deleted from the data disk after snapshot. Yields (relative_path, size_bytes_str)."""
    relative_path, size_bytes_str = big_file_on_data_disk
    full_path = f"{DATA_DISK_MOUNT_PATH}{relative_path}"
    LOGGER.info(f"Deleting big test file '{full_path}' from data disk")
    run_ssh_commands(
        host=file_restore_vm_with_data_disk.ssh_exec,
        commands=shlex.split(f"rm -f {full_path}"),
        wait_timeout=TIMEOUT_2MIN,
        sleep=TIMEOUT_5SEC,
    )
    yield relative_path, size_bytes_str


@pytest.fixture()
def multiple_files_on_data_disk(file_restore_vm_with_data_disk):
    """Multiple test files written to the data disk. Yields list of (relative_path, content)."""
    files = []
    for idx in range(1, 4):
        relative_path = f"/concurrent-file-{idx}.txt"
        content = f"concurrent-content-{idx}"
        write_file_via_ssh(
            vm=file_restore_vm_with_data_disk,
            filename=f"{DATA_DISK_MOUNT_PATH}{relative_path}",
            content=content,
        )
        files.append((relative_path, content))
    yield files


@pytest.fixture()
def concurrent_data_disk_snapshot(
    file_restore_vm_with_data_disk,
    multiple_files_on_data_disk,
    blank_data_disk,
    namespace,
    admin_client,
    snapshot_storage_class_name_scope_module,
):
    """VolumeSnapshot of the data disk PVC containing multiple test files."""
    vsc_name = volume_snapshot_class_for_sc(
        sc_name=snapshot_storage_class_name_scope_module,
        client=admin_client,
    )
    LOGGER.info(f"Creating VolumeSnapshot of data disk PVC '{blank_data_disk.name}' with concurrent files")
    with VolumeSnapshot(
        name="file-restore-concurrent-snapshot",
        namespace=namespace.name,
        source={"persistentVolumeClaimName": blank_data_disk.name},
        volume_snapshot_class_name=vsc_name,
    ) as snapshot:
        wait_for_volume_snapshot_ready_to_use(
            namespace=namespace.name,
            name=snapshot.name,
        )
        yield snapshot


@pytest.fixture()
def deleted_concurrent_files_on_data_disk(
    file_restore_vm_with_data_disk, concurrent_data_disk_snapshot, multiple_files_on_data_disk
):
    """Multiple files deleted from the data disk after snapshot. Yields list of (relative_path, content)."""
    for relative_path, _ in multiple_files_on_data_disk:
        full_path = f"{DATA_DISK_MOUNT_PATH}{relative_path}"
        LOGGER.info(f"Deleting test file '{full_path}' from data disk")
        run_ssh_commands(
            host=file_restore_vm_with_data_disk.ssh_exec,
            commands=shlex.split(f"rm -f {full_path}"),
            wait_timeout=TIMEOUT_2MIN,
            sleep=TIMEOUT_5SEC,
        )
    yield multiple_files_on_data_disk


@pytest.fixture(scope="class")
def blank_data_disk(admin_client, namespace, snapshot_storage_class_name_scope_module):
    """Blank 5Gi DataVolume for use as a secondary data disk."""
    with DataVolume(
        name="file-restore-data-disk",
        namespace=namespace.name,
        source="blank",
        size="5Gi",
        storage_class=snapshot_storage_class_name_scope_module,
        client=admin_client,
        api_name="storage",
    ) as data_volume:
        yield data_volume


@pytest.fixture(scope="class")
def file_restore_vm_with_data_disk(
    admin_client,
    namespace,
    rhel10_data_source_scope_session,
    snapshot_storage_class_name_scope_module,
    file_restore_ssh_public_key,
    blank_data_disk,
):
    """Running RHEL10 VM with a hotplugged ext4 data disk mounted at /mnt/data."""
    with VirtualMachineForTests(
        name="file-restore-data-disk-vm",
        namespace=namespace.name,
        client=admin_client,
        os_flavor=OS_FLAVOR_RHEL,
        vm_instance_type=VirtualMachineClusterInstancetype(
            client=admin_client,
            name=U1_SMALL,
        ),
        vm_preference=VirtualMachineClusterPreference(
            client=admin_client,
            name=RHEL10_PREFERENCE,
        ),
        data_volume_template=data_volume_template_with_source_ref_dict(
            data_source=rhel10_data_source_scope_session,
            storage_class=snapshot_storage_class_name_scope_module,
        ),
    ) as vm:
        add_dv_to_vm(vm=vm, dv_name=blank_data_disk.name)
        running_vm(vm=vm)
        configure_vm_for_file_restore(
            vm=vm,
            ssh_public_key=file_restore_ssh_public_key,
            # filerestore_script_path=FILERESTORE_SCRIPT_PATH,
        )
        LOGGER.info(f"Formatting and mounting data disk at {DATA_DISK_MOUNT_PATH}")
        for cmd in [
            f"sudo mkfs.ext4 {DATA_DISK_DEVICE}",
            f"sudo mkdir -p {DATA_DISK_MOUNT_PATH}",
            f"sudo mount {DATA_DISK_DEVICE} {DATA_DISK_MOUNT_PATH}",
            f"sudo chmod 777 {DATA_DISK_MOUNT_PATH}",
        ]:
            run_ssh_commands(
                host=vm.ssh_exec,
                commands=shlex.split(cmd),
                wait_timeout=TIMEOUT_2MIN,
                sleep=TIMEOUT_5SEC,
            )
        yield vm


@pytest.fixture()
def test_file_on_data_disk(file_restore_vm_with_data_disk):
    """Test file written to the data disk. Yields (relative_path, content)."""
    relative_path = "/test-restore.txt"
    write_file_via_ssh(
        vm=file_restore_vm_with_data_disk,
        filename=f"{DATA_DISK_MOUNT_PATH}{relative_path}",
        content=TEST_FILE_CONTENT,
    )
    yield relative_path, TEST_FILE_CONTENT


@pytest.fixture()
def data_disk_snapshot(
    file_restore_vm_with_data_disk,
    test_file_on_data_disk,
    blank_data_disk,
    namespace,
    admin_client,
    snapshot_storage_class_name_scope_module,
):
    """VolumeSnapshot of the data disk PVC containing the test file."""
    vsc_name = volume_snapshot_class_for_sc(
        sc_name=snapshot_storage_class_name_scope_module,
        client=admin_client,
    )
    LOGGER.info(f"Creating VolumeSnapshot of data disk PVC '{blank_data_disk.name}'")
    with VolumeSnapshot(
        name="file-restore-data-disk-snapshot",
        namespace=namespace.name,
        source={"persistentVolumeClaimName": blank_data_disk.name},
        volume_snapshot_class_name=vsc_name,
    ) as snapshot:
        wait_for_volume_snapshot_ready_to_use(
            namespace=namespace.name,
            name=snapshot.name,
        )
        yield snapshot
