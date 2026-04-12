# Windows 服务器部署与重启清单

这份清单用于 Windows 服务器环境下快速启动、重启和排障当前项目。

项目目录示例：
- `D:\desktop_app\源码\desktop_app`

Python 示例：
- `D:\desktop_app\python\python.exe`

## 1. 启动前端

在项目目录执行：

```powershell
npm run dev -- --host 127.0.0.1
```

如果要后台运行并输出日志：

```powershell
Start-Process -FilePath 'cmd.exe' -ArgumentList '/c','npm run dev -- --host 127.0.0.1 > frontend.dev.log 2>&1' -WorkingDirectory 'D:\desktop_app\源码\desktop_app'
```

## 2. 启动后端

在项目目录执行：

```powershell
D:\desktop_app\python\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

如果要后台运行并输出日志：

```powershell
Start-Process -FilePath 'cmd.exe' -ArgumentList '/c','set PYTHONIOENCODING=utf-8 && D:\desktop_app\python\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 > backend.dev.log 2>&1' -WorkingDirectory 'D:\desktop_app\源码\desktop_app'
```

## 3. 初始化数据库

```powershell
cd D:\desktop_app\源码\desktop_app
D:\desktop_app\python\python.exe -m backend.init_db
```

如果要导入历史 JSON：

```powershell
D:\desktop_app\python\python.exe -m backend.scripts.migrate_users_json
D:\desktop_app\python\python.exe -m backend.scripts.migrate_activity_log_json
D:\desktop_app\python\python.exe -m backend.scripts.migrate_product_cache_json
```

## 4. 检查后端是否正常

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/health
```

## 5. 结束旧进程

查 8000 端口：

```powershell
netstat -aon | findstr :8000
```

结束指定 PID：

```powershell
taskkill /PID <PID> /F
```

## 6. 查看日志

后端：

```powershell
Get-Content "D:\desktop_app\源码\desktop_app\backend.dev.log" -Wait
```

前端：

```powershell
Get-Content "D:\desktop_app\源码\desktop_app\frontend.dev.log" -Wait
```

## 7. 前端构建

```powershell
cd D:\desktop_app\源码\desktop_app
npm run build
```

说明：
- 如果当前机器仍出现 `vite spawn EPERM`，优先使用：
  - `npx tsc --noEmit`
  - 先确认代码层没有类型错误

## 8. 常用重启流程

### 重启后端

```powershell
$line = (netstat -aon | findstr "127.0.0.1:8000" | findstr LISTENING | Select-Object -First 1)
if ($line) {
  $pid = ($line -split '\s+')[-1]
  if ($pid) { taskkill /PID $pid /F }
}
Start-Process -FilePath 'cmd.exe' -ArgumentList '/c','set PYTHONIOENCODING=utf-8 && D:\desktop_app\python\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 > backend.dev.log 2>&1' -WorkingDirectory 'D:\desktop_app\源码\desktop_app'
```

### 重启前端

```powershell
Start-Process -FilePath 'cmd.exe' -ArgumentList '/c','npm run dev -- --host 127.0.0.1 > frontend.dev.log 2>&1' -WorkingDirectory 'D:\desktop_app\源码\desktop_app'
```

## 9. 聊天异步任务直连检查

当前项目里只有“创建聊天异步任务”会单独直连：

- `POST http://121.196.161.155:8000/api/chat/jobs`

其它接口仍然走：
- `/api/...`

如果创建任务失败但其它接口正常，优先检查：
1. 浏览器 Network 里 `POST /api/chat/jobs` 实际打到了哪里
2. `8000` 端口是否可访问
3. 后端日志里是否已创建 job

## 10. 当前推荐

Windows 环境更适合：
- 开发
- 联调
- 快速验证

正式公网部署仍推荐：
- ECS Linux
- Nginx
- systemd
- RDS

## 11. 版本号自动升级

当前系统版本号直接读取 [package.json](/D:/desktop_app/源码/desktop_app/package.json) 中的 `version` 字段，页面显示格式为：

- `V1.0.0`
- `V1.0.1`
- `V1.0.2`
- `V1.0.9`
- `V1.1.0`

在 Windows 环境下，默认构建命令现在也会自动升级版本号：

```powershell
cd D:\desktop_app\源码\desktop_app
npm run build
```

执行时会先自动把版本按以下规则加一：

- `1.0.0 -> 1.0.1`
- `1.0.1 -> 1.0.2`
- `1.0.9 -> 1.1.0`

如果你只是本地临时构建，不想改动版本号，请使用：

```powershell
cd D:\desktop_app\源码\desktop_app
npm run build:no-bump
```

自动升级脚本位置：

- [auto-bump-version.mjs](/D:/desktop_app/源码/desktop_app/scripts/auto-bump-version.mjs)
