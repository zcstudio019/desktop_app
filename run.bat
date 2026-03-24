@echo off
chcp 65001 >nul
echo ========================================
echo   贷款申请助手 - 开发环境
echo ========================================

REM 检查 .env 文件
if not exist ".env" (
    if exist ".env.example" (
        echo [提示] 首次运行，正在创建配置文件...
        copy ".env.example" ".env" >nul
        echo 请编辑 .env 文件，填入正确的 API 密钥
        echo 然后重新运行此程序
        echo.
        pause
        exit /b 0
    ) else (
        echo [错误] 未找到配置文件 .env 或 .env.example
        pause
        exit /b 1
    )
)

echo.
echo 前端: http://localhost:5173/
echo 后端: http://localhost:8000/
echo.
echo 正在启动前端开发服务器（新窗口）...
start cmd /k "npm run dev"

echo 正在启动后端服务器...
echo 如需关闭，请按 Ctrl+C 或直接关闭此窗口
echo ========================================
echo.

uvicorn backend.main:app --reload --port 8000
pause
