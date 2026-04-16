# Loan Assistant 生产环境巡检清单

这份清单用于当前 Linux 正式环境的日常巡检、发版后检查和异常快速定位。

适用范围：
- FastAPI Web：`loan-assistant-api`
- Celery Chat Worker：`loan-assistant-celery-chat`
- Celery Heavy Worker：`loan-assistant-celery-heavy`
- Nginx：`nginx`
- Redis：`redis-server`

当前异步任务主链：
- `backend.tasks.chat_tasks.run_chat_extract_job`
- `backend.tasks.risk_tasks.run_risk_report_job`
- `backend.tasks.scheme_tasks.run_scheme_match_job`
- `backend.tasks.application_tasks.run_application_generate_job`

## 每天巡检

### 1. 检查核心服务状态

```bash
systemctl status loan-assistant-api --no-pager
systemctl status loan-assistant-celery-chat --no-pager
systemctl status loan-assistant-celery-heavy --no-pager
systemctl status nginx --no-pager
systemctl status redis-server --no-pager
```

目标：
- 所有服务均为 `active (running)`

### 2. 检查 API 健康

```bash
curl http://127.0.0.1:8000/api/health
```

目标：
- 返回正常 JSON
- 无超时

### 3. 检查 Worker 健康

```bash
cd /root/desktop_app
source /root/desktop_app/venv/bin/activate
python -m backend.scripts.check_worker_health
```

重点关注：
- `ping_ok: true`
- `missing_tasks: []`
- `registered_tasks` 是否包含四类正式任务

### 4. 检查 Redis 连通性

```bash
redis-cli ping
```

目标：
- 返回 `PONG`

### 5. 检查当前线上版本

```bash
cat /root/desktop_app/package.json | grep '"version"'
JS=$(curl -s http://127.0.0.1/ | grep -o 'assets/index-[^"]*\.js' | head -n 1)
echo "$JS"
curl -s "http://127.0.0.1/$JS" | grep -o 'V1\.0\.[0-9]\+' | head
```

目标：
- 代码版本与页面实际服务版本一致

### 6. 检查近期错误日志

```bash
journalctl -u loan-assistant-api -n 100 --no-pager
journalctl -u loan-assistant-celery-chat -n 100 --no-pager
journalctl -u loan-assistant-celery-heavy -n 100 --no-pager
tail -n 100 /var/log/nginx/error.log
```

重点关注：
- 连续 500 / 502
- Celery `failed` / `timeout` / `retry`
- `missing task`
- Redis 连接异常

### 7. 抽查一条真实任务

每天至少抽查一条：
- 资料提取
- 风险报告
- 方案匹配
- 申请表生成

确认：
- 最近任务列表能看到任务
- 状态能从 `pending -> running/retrying -> success/failed`
- 点击“查看结果 / 继续查看”能够恢复右侧结果

## 每周巡检

### 1. 检查任务异常趋势

建议查看：
- 是否有长期 `running`
- 是否有长期 `retrying`
- 是否有同类任务持续 `failed`
- 是否有 stale running 被自动收口为 `failed`

如果有数据库查询权限，可重点关注最近 7 天：
- 风险报告失败量
- 方案匹配失败量
- 申请表生成失败量
- 资料提取失败量

### 2. 检查 Worker 压力

```bash
journalctl -u loan-assistant-celery-chat -n 200 --no-pager
journalctl -u loan-assistant-celery-heavy -n 200 --no-pager
```

重点关注：
- `retry` 是否异常增多
- `timeout` 是否增多
- Heavy worker 是否持续长时间繁忙

### 3. 检查磁盘空间

```bash
df -h
du -sh /root/desktop_app
du -sh /var/www/desktop_app
du -sh /var/log
```

重点防止：
- 日志写满磁盘
- 历史构建文件堆积

### 4. 检查版本升级记录

```bash
tail -n 20 /root/desktop_app/logs/version-history.log
```

确认：
- 最近版本升级记录连续正常
- 当前线上版本与预期一致

### 5. 检查认证安全

重点确认：
- `/api/auth/me` 无 token 返回 401
- 不再新增随机 `admin_xxxxxxxx`
- 账号管理页没有异常新增管理员账号

## 每次发版后必做

### 1. 执行正式部署

```bash
cd /root/desktop_app
git reset --hard
git pull origin main
bash /root/desktop_app/deploy.sh
```

### 2. 检查部署脚本尾部输出

重点确认：
- `package.json version`
- `served asset`
- `served frontend version`

三者必须能对上。

### 3. 检查服务状态

```bash
systemctl status loan-assistant-api --no-pager
systemctl status loan-assistant-celery-chat --no-pager
systemctl status loan-assistant-celery-heavy --no-pager
systemctl status nginx --no-pager
```

### 4. 检查 Worker 健康

```bash
cd /root/desktop_app
source /root/desktop_app/venv/bin/activate
python -m backend.scripts.check_worker_health
```

### 5. 执行最小 smoke test

至少验证：
- 登录
- 资料提取
- 风险报告
- 方案匹配
- 申请表生成
- 最近任务列表恢复
- 查看结果 / 继续查看
- 页面版本号

## 上线异常时 10 分钟排障

### 1. 检查服务

```bash
systemctl status loan-assistant-api --no-pager
systemctl status loan-assistant-celery-chat --no-pager
systemctl status loan-assistant-celery-heavy --no-pager
systemctl status nginx --no-pager
systemctl status redis-server --no-pager
```

### 2. 检查 API 健康

```bash
curl http://127.0.0.1:8000/api/health
```

### 3. 检查 Worker 健康

```bash
cd /root/desktop_app
source /root/desktop_app/venv/bin/activate
python -m backend.scripts.check_worker_health
```

### 4. 盯日志

```bash
journalctl -u loan-assistant-api -f
journalctl -u loan-assistant-celery-chat -f
journalctl -u loan-assistant-celery-heavy -f
```

### 5. 检查当前页面实际服务版本

```bash
JS=$(curl -s http://127.0.0.1/ | grep -o 'assets/index-[^"]*\.js' | head -n 1)
echo "$JS"
curl -s "http://127.0.0.1/$JS" | grep -o 'V1\.0\.[0-9]\+' | head
```

### 6. 快速回滚

```bash
cd /root/desktop_app
git log --oneline -n 5
git reset --hard <上一个稳定提交>
bash /root/desktop_app/deploy.sh
```

## 巡检重点信号

长期建议重点盯这 5 个信号：
- API 是否持续出现 500 / 502
- Worker 是否有 `missing_tasks`
- Redis 是否稳定 `PONG`
- 是否出现大量 `retrying` / `timeout`
- 页面实际服务版本是否与代码版本一致

## 建议节奏

- 每天：服务状态、API 健康、Worker 健康、错误日志、版本一致性
- 每周：任务失败趋势、磁盘空间、版本升级记录、认证安全
- 每次发版后：完整 smoke test

这份清单的目标不是把运维复杂化，而是让我们在问题出现时更快定位，在发布之后更快确认系统真的健康。
