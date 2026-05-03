@echo off
setlocal

:: Define the script name to search for
set "SCRIPT_NAME=mux_uart_ssh.py"

echo Checking for existing %SCRIPT_NAME% processes...
wmic process where "name='python.exe' and commandline like '%%%SCRIPT_NAME%%%'" get processid,commandline,name
for /f "tokens=1" %%a in ('wmic process where "name='python.exe' and commandline like '%%%SCRIPT_NAME%%%'" get processid ^| findstr [0-9]') do (
    echo ">>" %%a
)

:: Look for python processes running our script
for /f "tokens=1" %%a in ('wmic process where "name='python.exe' and commandline like '%%%SCRIPT_NAME%%%'" get processid ^| findstr [0-9]') do (
    echo Found existing process: %%a
    echo Killing process %%a...
    taskkill /PID %%a /F
)

for /f "tokens=1" %%a in ('wmic process where "name='python3.exe' and commandline like '%%%SCRIPT_NAME%%%'" get processid ^| findstr [0-9]') do (
    echo Found existing process: %%a
    echo Killing process %%a...
    taskkill /PID %%a /F
)

echo.
echo Launching %SCRIPT_NAME% on COM45 with debug logging...
python %SCRIPT_NAME% COM45 --debug

