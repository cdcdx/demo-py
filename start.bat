@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

:: 如果在虚拟环境中，则先退出
if defined VIRTUAL_ENV (
    if exist .venv\Scripts\deactivate.bat (
        call .venv\Scripts\deactivate.bat
    )
)

:: 设置Python路径
set PYTHON_HOME=%LOCALAPPDATA%\Programs\Python\Python311
echo %PATH% | findstr /C:"%PYTHON_HOME%" >nul
if %ERRORLEVEL% == 1 (
    echo Adding Python to PATH: %PYTHON_HOME%
    set PATH=%PYTHON_HOME%;%PYTHON_HOME%\Scripts;%PATH%
)

:: 读取配置到环境
set PYTHON_EXE=python.exe
set UVICORN_PORT=8000

:: 从.env文件读取UVICORN_PORT配置
if exist .env (
    for /f "usebackq tokens=*" %%i in (".env") do (
        echo %%i | findstr /C:"UVICORN_PORT=" >nul
        if !ERRORLEVEL! == 0 (
            set "%%i"
        )
    )
)

:: 根据参数执行不同操作
if /i "%1"=="init" (
    :: 初始化虚拟环境和安装依赖
    if exist .venv (
        echo Virtual Environment already exists
        call .venv\Scripts\activate.bat
    ) else (
        echo Installing Virtual Environment ...
        %PYTHON_EXE% -m venv .venv
        call .venv\Scripts\activate.bat
    )
    echo Installing requirements...
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
) else if /i "%1"=="clear" (
    :: 清理缓存文件
    echo Clearing cache files...
    for /d /r %%i in (__pycache__) do (
        if exist "%%i" (
            echo Deleting: %%i
            rmdir /s/q "%%i"
        )
    )
    for /r %%i in (*.pyc) do (
        if exist "%%i" (
            echo Deleting: %%i
            del "%%i"
        )
    )
    echo Cache files cleared.
) else if /i "%1"=="kill" (
    :: 杀死占用端口的进程
    echo Port:%UVICORN_PORT% Occupation Detection ...
    if %UVICORN_PORT% GTR 100 (
        for /f "tokens=5" %%i in ('netstat -ano ^| findstr ":%UVICORN_PORT%" ^| findstr "LISTENING"') do (
            echo Find the Port %UVICORN_PORT% PID: %%i
            taskkill /F /PID %%i
        )
    )
    
    :: 检测并杀死所有Python进程
    echo Process:%PYTHON_EXE% Detection...
    for /f "tokens=1,2" %%i in ('tasklist ^| findstr /i "%PYTHON_EXE%"') do (
        echo Find the process %%i PID: %%j
        taskkill /F /PID %%j
    )
) else if /i "%1"=="log" (
    :: 显示日志文件
    if exist log-main.log (
        type log-main.log
    ) else (
        echo Log file not found.
    )
) else (
    :: 默认运行应用
    echo Port:%UVICORN_PORT% Occupation Detection ...
    if %UVICORN_PORT% GTR 100 (
        netstat -ano | findstr ":%UVICORN_PORT%" | findstr "LISTENING" >nul
        if !ERRORLEVEL! == 0 (
            echo The port %UVICORN_PORT% is already in use!
            exit /b 1
        )
    )

    echo Virtual Environment Activation ...
    if exist .venv\Scripts\activate.bat (
        call .venv\Scripts\activate.bat
    ) else (
        echo Virtual environment not found. Please run 'start.bat init' first.
        exit /b 1
    )

    echo Launching main.py ...
    python main.py %*
)