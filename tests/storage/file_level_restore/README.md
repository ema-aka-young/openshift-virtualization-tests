# File-Level Restore Tests

Tests POC for the [vm-file-restore-operator](https://github.com/kubevirt/vm-file-restore-operator), which restores individual files from a VolumeSnapshot or PVC to a running VM using the `VirtualMachineFileRestore` CR.

## Restore Modes

- **Manual** — hotplugs the backup volume and mounts it read-only inside the guest. The user browses and copies files manually. The CR reaches the `VolumeReady` phase.
- **Automatic** — same as manual, but the operator also copies files from a `sourcePath` back to the guest root filesystem and cleans up. The CR reaches the `Succeeded` phase.

## Prerequisites

- A snapshot-capable StorageClass (tests use `snapshot_storage_class_name_scope_module`)
- The `declarativeHotplugVolumes` feature gate (enabled automatically by the `enabled_declarative_hotplug_volumes` fixture)

## Directory Layout

| File | Purpose |
|------|---------|
| `test_file_level_restore.py` | Test cases for manual and automatic restore from both VolumeSnapshot and PVC sources |
| `conftest.py` | Fixtures: VM provisioning, snapshot creation, data disk setup, test file lifecycle |
| `utils.py` | `VirtualMachineFileRestore` resource class, operator install/uninstall, VM configuration helpers |
| `manifests/install.yaml` | Operator deployment manifest |
| `manifests/filerestore.sh` | Local override of the guest helper script (see below) |

## Local `filerestore.sh` Override

The tests carry a local copy of the `filerestore.sh` guest helper script instead of relying on the version downloaded by the upstream `setup.sh`. This allows shipping fixes ahead of upstream merges — for example, XFS `nouuid` mount handling required when mounting a snapshot of a RHEL root disk (XFS) alongside the already-mounted original.

The override is injected into the VM by `configure_vm_for_file_restore()` in `utils.py`, which base64-encodes the script and writes it over SSH after the upstream setup completes.

## Running

```bash
uv run pytest tests/storage/file_level_restore/
```
