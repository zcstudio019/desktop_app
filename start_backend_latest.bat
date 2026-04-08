@echo off
cd /d D:\desktop_app\源码\desktop_app
set PYTHONIOENCODING=utf-8
D:\desktop_app\python\python.exe -c "import sys; sys.path.insert(0, r'D:\desktop_app\源码\desktop_app'); from backend.main import app; import uvicorn; uvicorn.run(app, host='127.0.0.1', port=8000)"
