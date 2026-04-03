#!/usr/bin/env bash
# Create (or start) an Ubuntu arm64 VM via Multipass for testing deployment/ansible.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM="${MULTIPASS_VM_NAME:-lina-edge-test}"
UBUNTU_RELEASE="${MULTIPASS_UBUNTU_RELEASE:-24.04}"

if ! command -v multipass >/dev/null 2>&1; then
  echo "Multipass is not installed. On macOS: brew install --cask multipass"
  echo "See: https://canonical.com/multipass/docs/install-macos"
  exit 1
fi

PUBFILE=""
for candidate in "${HOME}/.ssh/id_ed25519.pub" "${HOME}/.ssh/id_rsa.pub"; do
  if [[ -f "${candidate}" ]]; then
    PUBFILE="${candidate}"
    break
  fi
done
if [[ -z "${PUBFILE}" ]]; then
  echo "No SSH public key found at ~/.ssh/id_ed25519.pub or ~/.ssh/id_rsa.pub."
  echo "Create one with: ssh-keygen -t ed25519 -C \"you@example.com\""
  exit 1
fi

if multipass info "${VM}" &>/dev/null; then
  echo "VM '${VM}' already exists; starting if needed..."
  multipass start "${VM}" 2>/dev/null || true
else
  echo "Launching ${VM} (Ubuntu ${UBUNTU_RELEASE}, arm64 on Apple Silicon)..."
  multipass launch "${UBUNTU_RELEASE}" \
    --name "${VM}" \
    --cpus 2 \
    --memory 2G \
    --disk 15G \
    --cloud-init "${ROOT}/cloud-init.yaml"
fi

echo "Installing your SSH public key into ubuntu@${VM}..."
cat "${PUBFILE}" | multipass exec "${VM}" -- bash -c '
  mkdir -p ~/.ssh
  chmod 700 ~/.ssh
  touch ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  cat >> ~/.ssh/authorized_keys
'

IP="$(multipass info "${VM}" | awk -F": " "/^IPv4:/{print \$2; exit}")"
if [[ -z "${IP}" ]]; then
  echo "Could not read IPv4 for ${VM}. Run: multipass info ${VM}"
  exit 1
fi

echo ""
echo "VM is ready: ${VM} at ${IP}"
echo ""
echo "Add this host to deployment/ansible/inventory/hosts (or merge [edge] vars):"
echo ""
echo "[edge]"
echo "${VM} ansible_host=${IP} ansible_user=ubuntu"
echo ""
echo "[edge:vars]"
echo "lina_binaries_dir=/absolute/path/on/your/mac/to/linux-arm64-binaries"
echo ""
echo "Then from deployment/ansible:"
echo "  ansible-playbook playbooks/site.yml"
echo ""
