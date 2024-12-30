#!/bin/bash

# 查找进程ID
pids=$(ps -ef | grep resolveStarter | grep -v grep | awk '{print $2}')

# 如果找到进程，则杀死它
if [ -n "$pids" ]; then
    echo "Killing resolveStarter processes: $pids"
    kill -9 $pids
else
    echo "No resolveStarter process found."
fi

# 删除 nohup.out 文件
rm -rf nohup.out

# 后台运行 resolveStarter.py
nohup /opt/miniconda3/envs/addressSearchpy311/bin/python3.11 resolveStarter.py &

