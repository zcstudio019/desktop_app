# Linux 正式部署手册

## 1. 目标架构

- `Nginx`：提供前端静态资源，并把 `/api` 反向代理到 FastAPI Web。
- `Web`：`FastAPI + Uvicorn`，Linux 下以 `systemd + 多 worker` 运行。
- `Queue`：`Redis` 服务化运行，作为 Celery broker / result backend。
- `Worker`：`Celery Worker` 多并发运行，负责资料提取、风险报告、方案匹配、申请表生成。
- `Data`：现有 `MySQL / RDS` 继续沿用。
- `状态中心`：继续复用 `async_jobs` 表，前端轮询和最近任务列表无需换协议。

## 2. 推荐目录结构

```text
/opt/loan-assistant/
  app/                  # 代码、虚拟环境、构建产物
    .venv/
    backend/
    dist/
    src/
  logs/                 # 业务补充日志目录（如需要）
  run/                  # 运行期文件、临时 socket / pid

/etc/loan-assistant/
  env/
    loan-assistant.env  # 统一环境变量文件
```

说明：
- 代码目录、环境文件目录、日志目录分开，方便权限控制和运维排障。
- `systemd` 主要把日志交给 `journald`，`/opt/loan-assistant/logs` 作为补充目录。

## 3. Web 层

推荐命令：

```bash
/opt/loan-assistant/app/.venv/bin/python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --workers 2
```

正式建议：
- Web 只监听 `127.0.0.1`
- 由 Nginx 对外暴露
- 日志默认走 `journald`

`systemd` 文件：
- `deploy/linux/loan-assistant-web.service`

常用命令：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now loan-assistant-web
sudo systemctl status loan-assistant-web
sudo journalctl -u loan-assistant-web -f
```

## 4. Celery Worker

推荐单实例多并发：

```bash
/opt/loan-assistant/app/.venv/bin/celery -A backend.celery_app.celery_app worker --loglevel=info --concurrency=4 --hostname=loan-assistant@%H
```

如果要扩容，可用：
- `deploy/linux/loan-assistant-worker.service`
- `deploy/linux/loan-assistant-worker@.service`

示例：

```bash
sudo systemctl enable --now loan-assistant-worker
sudo systemctl status loan-assistant-worker
sudo journalctl -u loan-assistant-worker -f
```

多实例：

```bash
sudo systemctl enable --now loan-assistant-worker@1
sudo systemctl enable --now loan-assistant-worker@2
```

Worker 启动后应保留：
- `expected_tasks`
- `registered_tasks`
- `[Worker Health] ready`
- `[Worker Health] ping ok`

健康检查：

```bash
cd /opt/loan-assistant/app
/opt/loan-assistant/app/.venv/bin/python -m backend.scripts.check_worker_health
```

## 5. Redis 服务化

安装后建议使用系统服务运行。

核心建议：
- 只绑定 `127.0.0.1`
- `appendonly yes`
- `supervised systemd`
- 不暴露到公网

模板：
- `deploy/linux/redis-loan-assistant.conf`

状态查看：

```bash
sudo systemctl status redis
redis-cli -h 127.0.0.1 -p 6379 ping
```

## 6. Nginx

模板：
- `deploy/linux/loan-assistant-nginx.conf`

要点：
- `root /opt/loan-assistant/app/dist`
- `/api/` 反代到 `127.0.0.1:8000`
- 轮询接口不需要特别高的超时，因为长任务已迁到 Celery
- `client_max_body_size 50m`

验证：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 7. 环境变量

统一放在：

```text
/etc/loan-assistant/env/loan-assistant.env
```

模板：
- `deploy/linux/loan-assistant.env.example`

### Web 和 Worker 都需要

- `DB_BACKEND`
- `USE_LOCAL_STORAGE`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `DATABASE_URL`（如果使用）
- `JWT_SECRET`
- `DEEPSEEK_API_KEY`
- `REDIS_URL`
- `CELERY_BROKER_URL`
- `CELERY_RESULT_BACKEND`
- `TASK_QUEUE_ENABLED`
- `CELERY_TASK_SOFT_TIME_LIMIT`
- `CELERY_TASK_TIME_LIMIT`
- `CELERY_MAX_RETRIES`
- `CELERY_RETRY_BACKOFF`
- `CELERY_RETRY_JITTER`
- `WORKER_HEALTHCHECK_ENABLED`
- `JOB_STALE_TIMEOUT_SECONDS`

### Web 更关键

- `CORS_ORIGINS`
- 前端构建相关变量（如 `VITE_*`，构建时注入）

### Worker 更关键

- `CELERY_WORKER_CONCURRENCY`

## 8. 安全建议

- Web 只监听 `127.0.0.1`
- Redis 只监听 `127.0.0.1`
- 不暴露 Redis / Celery broker 到公网
- `systemd` 使用独立用户：`loanassistant`
- 环境文件权限建议：`640`
- 避免在日志中输出密钥

## 9. 上线步骤

1. 准备 Linux 服务器与 `loanassistant` 用户。
2. 创建目录：

```bash
sudo mkdir -p /opt/loan-assistant/app /opt/loan-assistant/logs /opt/loan-assistant/run
sudo mkdir -p /etc/loan-assistant/env
sudo chown -R loanassistant:loanassistant /opt/loan-assistant
```

3. 部署代码到 `/opt/loan-assistant/app`
4. 创建虚拟环境并安装依赖：

```bash
cd /opt/loan-assistant/app
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-prod.txt
npm ci
npm run build
```

5. 沿用现有 RDS，执行：

```bash
python -m backend.init_db
```

6. 安装 / 配置 Redis
7. 写入 `/etc/loan-assistant/env/loan-assistant.env`
8. 拷贝 systemd 文件并启动：

```bash
sudo cp deploy/linux/loan-assistant-web.service /etc/systemd/system/
sudo cp deploy/linux/loan-assistant-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now loan-assistant-web
sudo systemctl enable --now loan-assistant-worker
```

9. 配置 Nginx
10. 做 smoke test

## 10. Smoke Test

1. `curl http://127.0.0.1:8000/api/health`
2. `python -m backend.scripts.check_worker_health`
3. 前端登录
4. 分别提一条：
   - 资料提取
   - 风险报告
   - 方案匹配
   - 申请表生成
5. 确认：
   - Worker 收到任务
   - `async_jobs` 正常流转
   - 最近任务列表可见
   - 结果可恢复

## 11. 回滚建议

- 保留上一版 `/opt/loan-assistant/app` 软链接或备份目录
- 回滚时先停：

```bash
sudo systemctl stop loan-assistant-worker
sudo systemctl stop loan-assistant-web
```

- 切回上一版代码后重启
- Redis / RDS 保持不动

## 12. 监控与巡检建议

- `journalctl -u loan-assistant-web -f`
- `journalctl -u loan-assistant-worker -f`
- 每日运行一次：

```bash
python -m backend.scripts.check_worker_health
```

- 巡检重点：
  - `missing_tasks`
  - `ping_ok`
  - `retrying` 是否长期不结束
  - `running` 是否被 stale timeout 收口

## 13. 不需要修改业务代码的部分

这一阶段不需要改：
- 四条 async job 主链算法
- `async_jobs` 表核心设计
- 前端任务列表和轮询协议
- 业务页面交互结构
