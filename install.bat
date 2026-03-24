@echo off
chcp 65001 >nul
echo ========================================
echo   贷款申请助手 - 安装程序
echo ========================================
echo.

REM 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.10+
    echo 下载地址: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

echo [1/3] 检测到 Python:
python --version
echo.

echo [2/3] 安装依赖包（首次运行需要几分钟）...
pip install -r requirements.txt -q
if errorlevel 1 (
    echo [错误] 依赖安装失败，请检查网络连接
    pause
    exit /b 1
)
echo 依赖安装完成！
echo.

echo [3/3] 检查配置文件...
if not exist ".env" (
    echo [提示] 未找到 .env 配置文件
    echo 正在从模板创建...
    copy .env.example .env >nul
    echo 请编辑 .env 文件，填入正确的 API 密钥
    echo.
)

echo ========================================
echo   安装完成！
echo   运行 run.bat 启动程序
echo ========================================
pause
