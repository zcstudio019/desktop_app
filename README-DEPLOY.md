# 阿里云 ECS 部署说明

本项目当前技术栈：

- 前端：React 18 + TypeScript + Vite
- 后端：FastAPI + Uvicorn
- 认证：JWT + `data/users.json`
- 业务存储：当前主链路仍以 `backend/services/local_storage_service.py` 的 SQLite 逻辑为核心
- 数据库初始化：`backend/database.py` + `backend/init_db.py` + `backend/db_models.py`

## 推荐部署方案

推荐使用：

1. 前端执行 `npm run build`，产物输出到 `dist/`
2. Nginx 提供前端静态资源
3. Nginx 反向代理 `/api` 到 FastAPI
4. FastAPI 通过 `systemd` 常驻运行

这条路线最适合阿里云 ECS 公网 IP 访问，结构简单、排障方便、后续也容易接域名和 HTTPS。

仓库里同时提供了 `Dockerfile` 和 `docker-compose.yml` 作为备选方案，但默认仍建议先走 `systemd + Nginx`。

## 一、部署前先确认

### 1. 前端命令

- 本地开发启动：`npm run dev`
- 生产构建：`npm run build`

### 2. 后端命令

- 本地开发启动：
  - `python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000`
- 生产环境启动：
  - `python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000`

### 3. 前后端生产衔接方式

- 前端构建后由 Nginx 直接提供 `dist/`
- 前端所有 `/api/*` 请求由 Nginx 转发到 FastAPI
- 推荐生产环境不设置 `VITE_API_BASE`，让前端默认走当前站点同源地址

### 4. 当前部署风险边界

虽然 `backend/database.py` 已经支持 MySQL / RDS 连接，且新增了 `backend/init_db.py` 用于 SQLAlchemy 标准建表，但当前核心客户资料业务服务 `backend/services/local_storage_service.py` 仍是 SQLite 风格实现。  

这意味着：

- **最稳的 ECS 首次上线方式**：继续使用 SQLite 本地文件
- **RDS 切换准备**：数据库基础配置、连接和建表初始化已经补齐
- **尚未完全完成的部分**：客户资料主链路还没有整体重写成 SQLAlchemy Repository 模式

所以，如果你的目标是“今天就稳定上线公网访问”，优先使用 SQLite。  
如果你的目标是“后续逐步切到 RDS”，本次改动已经把数据库连接层和初始化层准备好了。

## 二、服务器准备

建议 ECS 使用 Ubuntu 22.04 / Alibaba Cloud Linux 3，至少 2C2G。

需要安装：

```bash
sudo apt update
sudo apt install -y nginx git curl python3 python3-venv python3-pip nodejs npm
```

如果用 Docker 备选方式，再安装：

```bash
sudo apt install -y docker.io docker-compose-plugin
sudo systemctl enable docker
sudo systemctl start docker
```

## 三、安全组端口

阿里云 ECS 安全组至少开放：

- `22`：SSH
- `80`：HTTP
- `443`：HTTPS（后续接域名证书时需要）

如果你临时想直接访问 FastAPI，也可以临时开放：

- `8000`

但正式上线建议只开放 `80/443`，不要直接暴露 `8000`。

## 四、上传代码

### 方式 1：Git 拉取

```bash
sudo mkdir -p /srv/loan-assistant
sudo chown -R $USER:$USER /srv/loan-assistant
cd /srv/loan-assistant
git clone <你的仓库地址> current
cd current
```

### 方式 2：本地打包上传

本地压缩项目后上传到 ECS，再解压到：

```bash
/srv/loan-assistant/current
```

## 五、配置环境变量

复制并编辑：

```bash
cp .env.production .env.production.local
vim .env.production.local
```

建议实际运行时使用 `.env.production.local`，避免直接改模板。

至少需要改：

- `JWT_SECRET`
- `CORS_ORIGINS`
- `DEEPSEEK_API_KEY`
- `BAIDU_OCR_*`
- `FEISHU_*` / `WIKI_*`（如果你依赖飞书）

### 生产环境建议

如果走 Nginx 同源代理，建议：

```env
VITE_API_BASE=
CORS_ORIGINS=http://你的公网IP
```

## 六、初始化与部署

### 推荐方案：systemd + Nginx

```bash
cd /srv/loan-assistant/current
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-prod.txt
npm ci
npm run build
set -a
source .env.production.local
set +a
python -m backend.init_db
```

安装 systemd 服务：

```bash
sudo cp loan-assistant-api.service /etc/systemd/system/loan-assistant-api.service
sudo sed -i 's#EnvironmentFile=/srv/loan-assistant/current/.env.production#EnvironmentFile=/srv/loan-assistant/current/.env.production.local#g' /etc/systemd/system/loan-assistant-api.service
sudo systemctl daemon-reload
sudo systemctl enable loan-assistant-api
sudo systemctl start loan-assistant-api
```

安装 Nginx 配置：

```bash
sudo cp nginx.conf /etc/nginx/conf.d/loan-assistant.conf
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx
```

访问：

- 前端：`http://你的公网IP`
- 后端健康检查：`http://你的公网IP/api/health`

### 备选方案：Docker / Docker Compose

先在服务器上构建前端：

```bash
npm ci
npm run build
```

然后：

```bash
docker compose --env-file .env.production.local up -d --build
```

说明：

- 当前 `docker-compose.yml` 使用 `host` 网络模式，便于沿用同一份 `nginx.conf`
- 启动前请确保宿主机 `80` 和 `8000` 端口未被其他服务占用

访问：

- `http://你的公网IP`

## 七、RDS（MySQL）连接说明

`.env.production` 样例已经按你的目标 RDS 地址填好了：

```env
USE_LOCAL_STORAGE=false
DB_HOST=rm-bp104ue00y26jvb9ioo.rwlb.rds.aliyuncs.com
DB_PORT=3306
DB_USER=dbuser
DB_PASSWORD=Zc15280764540
DB_NAME=db_test1
```

### 连接测试方法

#### 方式 1：使用 Python / SQLAlchemy

```bash
cd /srv/loan-assistant/current
source .venv/bin/activate
set -a
source .env.production.local
set +a
python -m backend.init_db
```

如果成功，会输出数据库初始化和 `SELECT 1` 测试通过。

#### 方式 2：使用 mysql 客户端

```bash
mysql -h rm-bp104ue00y26jvb9ioo.rwlb.rds.aliyuncs.com -P 3306 -u dbuser -p db_test1
```

## 八、连接失败排查

优先检查：

1. ECS 是否已加入 RDS 白名单
2. RDS 安全组 / 白名单是否允许 ECS 出网 IP
3. `DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME` 是否正确
4. ECS 是否能解析 RDS 域名：
   - `ping rm-bp104ue00y26jvb9ioo.rwlb.rds.aliyuncs.com`
5. ECS 到 3306 是否连通：
   - `telnet rm-bp104ue00y26jvb9ioo.rwlb.rds.aliyuncs.com 3306`
6. MySQL 账号是否有目标库权限
7. `.env.production.local` 是否真的被加载

## 九、常见访问失败排查

### 1. 前端页面能开，但接口报错

优先检查：

- `loan-assistant-api` 服务是否正常运行
- Nginx 是否配置了 `/api/` 反向代理
- `CORS_ORIGINS` 是否正确

命令：

```bash
sudo systemctl status loan-assistant-api
sudo journalctl -u loan-assistant-api -n 100 --no-pager
sudo nginx -t
sudo systemctl status nginx
```

### 2. 前端 build 后仍指向 localhost

当前已修复为：

- 本地 Vite 开发时：默认请求 `http://127.0.0.1:8000`
- 生产环境时：如果未设置 `VITE_API_BASE`，默认请求当前站点 `window.location.origin`

所以生产部署推荐：

- 不设置 `VITE_API_BASE`
- 使用 Nginx 代理 `/api`

### 3. FastAPI 没监听 0.0.0.0

systemd 服务已经固定为：

```bash
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

### 4. Nginx 反代路径错误

当前 `nginx.conf` 已配置：

- 静态资源：`/srv/loan-assistant/current/dist`
- 接口代理：`/api/` -> `http://127.0.0.1:8000/api/`

### 5. 环境变量未生效

systemd 方式下优先检查：

- `/etc/systemd/system/loan-assistant-api.service`
- `EnvironmentFile` 是否指向了正确文件

修改后必须执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart loan-assistant-api
```

### 6. SQLite / MySQL / RDS 切换风险

当前风险真实存在：

- `backend/database.py` 已支持 MySQL / RDS
- `backend/init_db.py` 已支持 SQLAlchemy 标准建表
- 但 `backend/services/local_storage_service.py` 仍是 SQLite 业务实现

因此：

- **公网稳定上线**：优先 SQLite
- **RDS 平滑切换**：需要后续继续把业务存储服务整体迁移到 SQLAlchemy

## 十、常用命令

### 本地构建

```bash
npm run build
python -m backend.init_db
```

### Git 提交

```bash
git add .
git commit -m "chore: add ecs deployment files"
git push
```

### 服务器拉取代码

```bash
cd /srv/loan-assistant/current
git pull
```

### 安装依赖

```bash
cd /srv/loan-assistant/current
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-prod.txt
npm ci

## 十、聊天异步任务直连配置

当前前端默认仍然通过 Nginx 访问大部分 `/api` 接口。
只有“创建聊天异步任务”这一条接口会单独支持直连 FastAPI 8000 端口：

```env
VITE_DIRECT_JOB_API_BASE=http://121.196.161.155:8000/api
```

说明：
- `POST /api/chat/jobs` 会改为请求 `${VITE_DIRECT_JOB_API_BASE}/chat/jobs`
- 其它接口仍然继续走原来的 `/api`，也就是 Nginx 反向代理链路
- 本地开发环境下，如果未配置 `VITE_DIRECT_JOB_API_BASE`，前端会自动回退到：
  - `http://127.0.0.1:8000/api`
- 生产环境下，如果未配置该变量，前端会回退到当前默认直连地址：
  - `http://121.196.161.155:8000/api`

如果线上要启用这条直连，请同时确认：
- ECS 安全组已放行 `8000`（仅当你决定让公网直接访问 8000 时）
- 或者该地址能从浏览器侧访问到 FastAPI 进程
- 如果不需要这条直连，也可以留空并继续只走 Nginx

### 聊天异步任务直连排障

如果 AI 对话里“提交资料提取任务”失败，但其它 `/api` 接口正常，请优先检查这一条直连链路：

1. 浏览器 Network 中查看 `POST /api/chat/jobs`
- 如果它实际请求到了 `http://121.196.161.155:8000/api/chat/jobs`
- 说明前端已经走了直连链路

2. 如果创建任务失败，但状态轮询接口正常
- 这通常表示：
  - Nginx 反代链路是通的
  - 但浏览器无法直接访问 `8000`
- 常见原因：
  - ECS 安全组未放行 `8000`
  - FastAPI 没监听 `0.0.0.0`
  - 服务器防火墙拦截了 `8000`

3. 服务器上检查 FastAPI 是否监听 `8000`
```bash
ss -lntp | grep 8000
```

4. ECS 本机检查接口
```bash
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:8000/api/chat/jobs
```

5. 公网检查 `8000` 是否可达
- 在浏览器或本地命令行检查：
```bash
curl http://你的公网IP:8000/api/health
```

6. 如果你不想开放 `8000`
- 可以去掉 `VITE_DIRECT_JOB_API_BASE`
- 让 `POST /api/chat/jobs` 继续走 Nginx `/api`
- 这样所有接口都只暴露 `80/443`
```

### 启动 / 重启

```bash
sudo systemctl start loan-assistant-api
sudo systemctl restart loan-assistant-api
sudo systemctl restart nginx
```

### 查看日志

```bash
sudo journalctl -u loan-assistant-api -f
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```
