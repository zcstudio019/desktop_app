#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/srv/loan-assistant/current}"
VENV_DIR="${VENV_DIR:-$APP_DIR/.venv}"

echo "==> Deploying loan assistant from ${APP_DIR}"
cd "$APP_DIR"

if [ ! -f ".env.production" ]; then
  echo "ERROR: .env.production not found in ${APP_DIR}"
  exit 1
fi

python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

pip install --upgrade pip
pip install -r requirements-prod.txt

if command -v npm >/dev/null 2>&1; then
  npm ci
  npm run build
else
  echo "WARNING: npm not found, skipped frontend build."
fi

set -a
source .env.production
set +a

python -m backend.init_db

sudo systemctl daemon-reload
sudo systemctl enable loan-assistant-api
sudo systemctl restart loan-assistant-api
sudo nginx -t
sudo systemctl restart nginx

echo "==> Deploy completed"
