@echo off
setlocal

REM 从 test.txt 文件中读取第一行，即要杀死的进程的 PID
set /p PID=<rest_pid

REM 杀死进程
taskkill /f /pid %PID% > nul

REM 启动新的进程
E:/windows_install/Miniconda3/envs/addressSearchpy311/python appStart.py --workers 4 --threads 8

endlocal
