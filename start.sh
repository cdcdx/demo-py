#!/bin/bash

set -e  # 遇到错误时退出

# 如果在虚拟环境中，则先退出
if [ -n "${VIRTUAL_ENV}" ]; then
    deactivate
fi

# 检查进程ID的函数
check_process_id() {
    local process_name="$1"
    if [ "$(uname)" == "Darwin" ]; then
        # macOS系统
        echo -e "\033[34m ps -ef |grep \"$process_name\" |grep -v \"grep\" |awk '{print \$2}' |head -n 1 \033[0m"
        pid=$(ps -ef |grep "$process_name" |grep -v "grep" |awk '{print $2}' |head -n 1)
    else
        # Linux系统
        echo -e "\033[34m ps -aux |grep \"$process_name\" |grep -v \"grep\" |awk '{print \$2}' |head -n 1 \033[0m"
        pid=$(ps -aux |grep "$process_name" |grep -v "grep" |awk '{print $2}' |head -n 1)
    fi
    
    # 检查pid是否为空
    if [ -z "$pid" ]; then
        pid=0
    fi
}

# 检查端口是否被占用的函数
check_port() {
    local port="$1"
    if [ "$(uname)" == "Darwin" ]; then
        # macOS系统
        echo -e "\033[34m netstat -anp tcp -v | grep \".$port \" |awk '{print \$11}' |head -n 1 \033[0m"
        temp=$(netstat -anp tcp -v | grep ".$port " |awk '{print $11}' |head -n 1)
        temp=${temp%/*}
        pid=${temp#*:}
    else
        # Linux系统
        echo -e "\033[34m netstat -tlpn | grep \":$port \" |grep -v \"grep\" |awk '{print \$7}' |head -n 1 \033[0m"
        temp=$(netstat -tlpn | grep ":$port " |grep -v "grep" |awk '{print $7}' |awk -F '/' '{print $1}' |head -n 1)
        pid=${temp%/*}
    fi
    
    # 检查pid是否为空
    if [ -z "$pid" ]; then
        pid=0
    fi
}

# 加载环境变量
if [ -f '.env' ]; then
    source .env
elif [ -f '.env.sample' ]; then
    source .env.sample
else
    export UVICORN_PORT=8000
fi

# 如果SSL证书和密钥文件都存在，则使用443端口
if [ -n "$SSL_CERTFILE" ] && [ -f "$SSL_CERTFILE" ] && [ -n "$SSL_KEYFILE" ] && [ -f "$SSL_KEYFILE" ]; then
    export UVICORN_PORT=443
fi

export PROCESS_MAIN='main.py'

# 设置文件描述符限制
ulimit -n 204800

# 根据第一个参数执行不同操作
case "$1" in
    "init")
        # 初始化虚拟环境和安装依赖
        cnip=$(curl -s cip.cc | grep '中国' | wc -l)
        if [ $cnip -gt 0 ]; then
            ip=$(curl -s ifconfig.me)
            echo "The current ip: $ip belongs to China"
            pyproxy=' -i https://pypi.tuna.tsinghua.edu.cn/simple'
        fi
        
        if [ -d ".venv" ]; then
            echo "Virtual Environment already exists"
            source .venv/bin/activate
            pip install -r requirements.txt $pyproxy
        else
            apt install python3-pip python3.12-venv -y
            echo "Install Virtual Environment..."
            python3 -m venv .venv
            source .venv/bin/activate
            pip install -r requirements.txt $pyproxy
        fi
        ;;
        
    "clear")
        # 清理缓存文件
        find . -type d -name "__pycache__" -exec rm -rf {} +
        ;;
        
    "log")
        # 查看日志
        tail -f log-main.log
        ;;
        
    "kill")
        # 停止进程
        check_port $UVICORN_PORT
        if [ $pid -gt 1 ]; then
            echo -e "\033[34m kill -9 $pid \033[0m"
            kill -9 $pid
            
            if [ "$(uname)" == "Darwin" ]; then
                temp=$(netstat -anp tcp -v | grep ".$UVICORN_PORT " | awk '{print $11}' | sort | uniq | tr '\n' ' ')
                temp=${temp#*:}
                echo -e "\033[34m kill $temp \033[0m"
                [ -n "$temp" ] && kill $temp
            else
                temp=$(netstat -tlpn | grep ":$UVICORN_PORT " | grep -v "grep" | awk '{print $7}' | awk -F '/' '{print $1}' | sort | uniq | tr '\n' ' ')
                echo -e "\033[34m kill -9 $temp \033[0m"
                [ -n "$temp" ] && kill -9 $temp
            fi
        else
            echo -e "\033[31m Port: $UVICORN_PORT is not exist. \033[0m"
        fi
        echo ""
        
        check_process_id $PROCESS_MAIN
        if [ $pid -gt 1 ]; then
            echo -e "\033[34m kill -9 $pid \033[0m"
            kill -9 $pid
        else
            echo -e "\033[31m Process: $PROCESS_MAIN is not exist. \033[0m"
        fi
        echo ""
        ;;
        
    "run")
        # 直接运行（非后台）
        check_port $UVICORN_PORT
        if [ $pid -eq 0 ]; then
            echo "Virtual Environment Activation..."
            source .venv/bin/activate
            echo "Launching $PROCESS_MAIN ..."
            python3 $PROCESS_MAIN ${@:2}
        else
            echo -e "\033[31m Port: $UVICORN_PORT is exist. \033[0m"
        fi
        ;;
        
    *)
        # 默认后台运行
        check_port $UVICORN_PORT
        if [ $pid -eq 0 ]; then
            echo "Virtual Environment Activation..."
            source .venv/bin/activate
            echo "Launching $PROCESS_MAIN ..."
            nohup python3 $PROCESS_MAIN $@ > log-main.log 2>&1 &
        else
            echo -e "\033[31m Port: $UVICORN_PORT is exist. \033[0m"
        fi
        ;;
esac