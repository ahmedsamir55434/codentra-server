#!/bin/bash

set -euo pipefail

APP_DIR="/opt/codentra-mongodb"
SOURCE_DIR="$(pwd)"

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  echo "Docker Compose is not installed."
  exit 1
fi

if [ ! -f "${SOURCE_DIR}/docker-compose.yml" ]; then
  echo "Run this script from the project folder."
  exit 1
fi

sudo mkdir -p "${APP_DIR}"
if [ "${SOURCE_DIR}" != "${APP_DIR}" ]; then
  sudo cp -R "${SOURCE_DIR}"/. "${APP_DIR}/"
  sudo chown -R "${USER}":"${USER}" "${APP_DIR}"
fi

cd "${APP_DIR}"

mkdir -p uploads uploads/meeting-recordings uploads/admin-team uploads/project-images data

if [ ! -f ".env" ]; then
  cp .env.example .env
  sed -i.bak "s/change_this_strong_password/$(openssl rand -hex 24)/" .env
  sed -i.bak "s/change_this_session_secret_32_chars/$(openssl rand -hex 32)/" .env
  sed -i.bak "s/change_this_jwt_secret_32_chars/$(openssl rand -hex 32)/" .env
  rm -f .env.bak
fi

${COMPOSE_CMD} up -d --build

echo
${COMPOSE_CMD} ps
echo
echo "Deployment finished."
echo "Application path: ${APP_DIR}"
echo "Open: http://$(hostname -I | awk '{print $1}')"
