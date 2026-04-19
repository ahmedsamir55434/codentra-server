#!/bin/bash

set -euo pipefail

APP_DIR="/opt/codentra-mongodb"

if [ "${EUID}" -ne 0 ]; then
  echo "Run this script with sudo."
  exit 1
fi

apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release ufw

if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "${VERSION_CODENAME}") stable" | \
    tee /etc/apt/sources.list.d/docker.list >/dev/null
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

mkdir -p "${APP_DIR}"

ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

echo "VPS dependencies are ready."
echo "Upload the project to ${APP_DIR} or run ./setup.sh from the uploaded folder."
