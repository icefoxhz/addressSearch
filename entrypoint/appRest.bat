@echo off
setlocal

REM �� test.txt �ļ��ж�ȡ��һ�У���Ҫɱ���Ľ��̵� PID
set /p PID=<rest_pid

REM ɱ������
taskkill /f /pid %PID% > nul

REM �����µĽ���
E:/windows_install/Miniconda3/envs/addressSearchpy311/python appStart.py --workers 4 --threads 8

endlocal
