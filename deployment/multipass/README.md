# Multipass test VM (macOS → Linux arm64)

Use this to run an **Ubuntu arm64** VM on **Apple Silicon** so you can exercise `deployment/ansible` without a Raspberry Pi on the desk. The guest architecture matches **64-bit Pi** builds (`GOOS=linux GOARCH=arm64`).

## What to install on your Mac

1. **Homebrew** (if you do not have it): [https://brew.sh](https://brew.sh)

2. **Xcode Command Line Tools** (often required for Homebrew and local builds):

   ```bash
   xcode-select --install
   ```

3. **Multipass**:

   ```bash
   brew install --cask multipass
   ```

   Official docs: [Install Multipass on macOS](https://canonical.com/multipass/docs/install-macos).

4. **Ansible** on the Mac (control node), e.g.:

   ```bash
   brew install ansible
   ```

First launch may prompt for **hypervisor / network** permissions in System Settings.

## Create the VM

From the repo:

```bash
chmod +x deployment/multipass/create-vm.sh
./deployment/multipass/create-vm.sh
```

Environment overrides (optional):

| Variable | Default | Meaning |
|----------|---------|---------|
| `MULTIPASS_VM_NAME` | `lina-edge-test` | Instance name |
| `MULTIPASS_UBUNTU_RELEASE` | `24.04` | Image alias (`multipass find`) |

The script expects **`~/.ssh/id_ed25519.pub`** or **`~/.ssh/id_rsa.pub`** so Ansible can SSH as `ubuntu` using your normal key.

## Point Ansible at the VM

The script prints an **`[edge]`** snippet. Put it in `deployment/ansible/inventory/hosts` and set **`lina_binaries_dir`** to a folder on your Mac containing Linux **arm64** binaries named `device`, `ledger`, `consumption`, `lightning`.

Then:

```bash
cd deployment/ansible
ansible edge -m ping
ansible-playbook playbooks/site.yml
```

## Notes

- This VM is **not** a Raspberry Pi Zero 2 W: it has more RAM and CPU. It is for **playbook and binary smoke tests**; still validate on real hardware afterward.
- If `ssh ubuntu@<ip>` fails before the script runs, run the script once; it appends your pubkey to `authorized_keys`.
