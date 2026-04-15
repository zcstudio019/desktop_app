#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/root/desktop_app}"
VENV_DIR="${VENV_DIR:-$APP_DIR/venv}"

echo "========== 进入项目目录 =========="
cd "$APP_DIR"

echo "========== 拉取最新代码 =========="
git pull

echo "========== 准备 Python 虚拟环境 =========="
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

echo "========== 安装后端依赖 =========="
pip install --upgrade pip
pip install -r requirements-prod.txt

if command -v npm >/dev/null 2>&1; then
  echo "========== 安装前端依赖 =========="
  npm install --legacy-peer-deps
  echo "========== 构建前端 =========="
  npm run build
else
  echo "WARNING: npm not found, skipped frontend build."
fi

if [ -f "$APP_DIR/.env" ]; then
  echo "========== 加载环境变量 =========="
  set -a
  source "$APP_DIR/.env"
  set +a
fi

echo "========== 初始化数据库 =========="
python -m backend.init_db

echo "========== 重启后端服务 =========="
systemctl restart loan-assistant-api

echo "========== 重启 Chat Celery 服务 =========="
systemctl restart loan-assistant-celery-chat

echo "========== 重启 Heavy Celery 服务 =========="
systemctl restart loan-assistant-celery-heavy

echo "========== 检查并重启 Nginx =========="
nginx -t
systemctl restart nginx

echo "========== 部署完成 =========="
