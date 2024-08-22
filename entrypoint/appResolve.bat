@echo off
setlocal

REM �� test.txt �ļ��ж�ȡ��һ�У���Ҫɱ���Ľ��̵� PID
set /p PID=<resolve_pid

REM ɱ������
taskkill /f /pid %PID% > nul

REM �����µĽ���
E:/windows_install/Miniconda3/envs/addressSearchpy311/python resolveStarter.py

endlocal
