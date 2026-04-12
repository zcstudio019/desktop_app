# 上线执行手册（ECS + RDS）

这份手册用于当前项目的正式上线，目标环境为：

- 阿里云 ECS
- 阿里云 RDS（MySQL）
- Nginx 提供前端静态资源
- FastAPI 通过 systemd 常驻运行

## 1. 服务器准备

安装基础依赖：

```bash
sudo apt update
sudo apt install -y nginx git curl python3 python3-venv python3-pip nodejs npm
```

## 2. 上传代码

```bash
cd /srv
git clone <你的仓库地址> loan-assistant
cd loan-assistant
```

## 3. 配置环境变量

复制生产模板：

```bash
cp .env.production .env.production.local
vim .env.production.local
```

至少确认这些字段：

```env
VITE_API_BASE=
VITE_DIRECT_JOB_API_BASE=http://121.196.161.155:8000/api

DB_BACKEND=mysql
USE_LOCAL_STORAGE=false
DB_HOST=rm-bp104ue00y26jvb9ioo.rwlb.rds.aliyuncs.com
DB_PORT=3306
DB_USER=dbuser
DB_PASSWORD=Zc15280764540
DB_NAME=db_test1

JWT_SECRET=replace-with-a-long-random-secret
CORS_ORIGINS=http://你的公网IP
DEEPSEEK_API_KEY=your-key
```

说明：

- `VITE_API_BASE` 留空时，其它接口默认走当前站点 `/api`
- `VITE_DIRECT_JOB_API_BASE` 只影响 `POST /api/chat/jobs`
- 如果不想让浏览器直连 `8000`，可清空 `VITE_DIRECT_JOB_API_BASE`

## 4. 安装依赖

```bash
cd /srv/loan-assistant
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-prod.txt
npm ci
```

## 5. 构建前端

```bash
cd /srv/loan-assistant
npm run build
```

说明：

- `npm run build` 现在会自动升级系统版本号
- 版本规则为：
  - `1.0.0 -> 1.0.1`
  - `1.0.1 -> 1.0.2`
  - `1.0.9 -> 1.1.0`
- 如果只是临时构建、不想升级版本号，请使用：

```bash
npm run build:no-bump
```

自动升级脚本位置：

- [auto-bump-version.mjs](/D:/desktop_app/源码/desktop_app/scripts/auto-bump-version.mjs)

## 6. 初始化数据库

```bash
cd /srv/loan-assistant
source .venv/bin/activate
set -a
source .env.production.local
set +a
python -m backend.init_db
```

如果要导入历史 JSON 数据，再执行：

```bash
python -m backend.scripts.migrate_users_json
python -m backend.scripts.migrate_activity_log_json
python -m backend.scripts.migrate_product_cache_json
```

## 7. 配置 systemd

```bash
sudo cp loan-assistant-api.service /etc/systemd/system/loan-assistant-api.service
sudo sed -i 's#EnvironmentFile=/srv/loan-assistant/current/.env.production#EnvironmentFile=/srv/loan-assistant/.env.production.local#g' /etc/systemd/system/loan-assistant-api.service
sudo systemctl daemon-reload
sudo systemctl enable loan-assistant-api
sudo systemctl start loan-assistant-api
```

查看后端状态：

```bash
sudo systemctl status loan-assistant-api
sudo journalctl -u loan-assistant-api -f
```

## 8. 配置 Nginx

```bash
sudo cp nginx.conf /etc/nginx/conf.d/loan-assistant.conf
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx
```

## 9. 阿里云安全组

至少放行：

- `22`
- `80`
- `443`

如果启用了聊天任务直连 `8000`，还要放行：

- `8000`

如果不想开放 `8000`，请清空：

- `VITE_DIRECT_JOB_API_BASE`

## 10. 上线后验证

基础验证：

```bash
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1/api/health
```

浏览器访问：

- `http://你的公网IP`
- `http://你的公网IP/api/health`

功能验证顺序：

1. 登录
2. 工作台统计
3. 客户列表
4. 上传资料
5. 资料汇总
6. 申请表生成
7. 方案匹配
8. AI 对话
9. 风险报告

## 11. 聊天异步任务直连排障

如果 AI 对话里“资料提取任务提交失败”，优先检查：

1. 浏览器 Network 中，`POST /api/chat/jobs` 是否发往：

- `http://121.196.161.155:8000/api/chat/jobs`

2. 如果其它 `/api` 正常，只有创建任务失败：

- 通常是浏览器无法直连 `8000`

3. 服务器检查：

```bash
ss -lntp | grep 8000
curl http://127.0.0.1:8000/api/health
```

4. 公网检查：

```bash
curl http://你的公网IP:8000/api/health
```

5. 如果不想开放 `8000`

- 清空 `VITE_DIRECT_JOB_API_BASE`
- 让 `POST /api/chat/jobs` 继续走 Nginx `/api`

## 12. 常用命令

拉代码：

```bash
cd /srv/loan-assistant
git pull
```

重启后端：

```bash
sudo systemctl restart loan-assistant-api
```

重启 Nginx：

```bash
sudo systemctl restart nginx
```

查看后端日志：

```bash
sudo journalctl -u loan-assistant-api -f
```

查看 Nginx 日志：

```bash
sudo tail -f /var/log/nginx/error.log
```

## 13. 当前上线结论

当前项目已经适合正式部署到：

- 阿里云 ECS
- 阿里云 RDS

当前主业务数据已经走数据库主链，旧 JSON 只保留迁移用途，本地 SQLite 只作为开发 fallback。
