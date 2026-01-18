#!/bin/bash

# 脚本配置
DEFAULT_DB='appdb.sqlite'
DEFAULT_PORT=9080
DB_DIR="."
IMAGE_NAME="coleifer/sqlite-web:latest"
SCRIPT_NAME=$(basename "$0")

# 日志函数
log_info() {
    echo "[INFO] $1" >&1
}

log_error() {
    echo "[ERROR] $1" >&2
}

log_warn() {
    echo "[WARN] $1" >&2
}

# 显示帮助信息
show_help() {
    cat << EOF
Usage: $SCRIPT_NAME {init|run|backup|stop} [instance_number]

Commands:
  init                    Initialize the database directory and pull Docker image
  run [instance]          Run SQLite Web UI (default instance: 0)
  backup [instance]       Backup database (default instance: 0)
  stop [instance]         Stop running container (default instance: 0)

Options:
  -h, --help              Show this help message

Examples:
  $SCRIPT_NAME init                 Initialize environment
  $SCRIPT_NAME run                  Run on port 8080 with default DB
  $SCRIPT_NAME run 1                Run on port 8081 with $DEFAULT_DB.1
  $SCRIPT_NAME backup 2             Backup $DEFAULT_DB.2 database
  $SCRIPT_NAME stop 1               Stop instance 1 container
EOF
}

# 获取数据库文件名
get_dbfile_name() {
    local instance=${1:-0}
    local suffix=""
    
    if [ "$instance" -ne 0 ]; then
        suffix=".$instance"
    fi
    
    echo "$DEFAULT_DB$suffix"
}

# 获取容器名称
get_container_name() {
    local instance=${1:-0}
    local suffix=""
    
    if [ "$instance" -ne 0 ]; then
        suffix="_$instance"
    fi
    
    echo "${DEFAULT_DB%%.*}$suffix"
}

# 获取端口号
get_port() {
    local instance=${1:-0}
    local port=$((DEFAULT_PORT + instance))
    echo $port
}

# 检查端口是否被占用
check_port() {
    local port=$1
    
    if command -v lsof >/dev/null 2>&1; then
        lsof -Pi :$port -sTCP:LISTEN -t >/dev/null
        return $?
    elif command -v netstat >/dev/null 2>&1; then
        netstat -tuln | grep -q ":$port.*LISTEN"
        return $?
    else
        # 如果没有检查工具，假设端口可用
        return 1
    fi
}

# 检查 Docker 是否安装
check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
}

# 检查 SQLite3 是否安装
check_sqlite3() {
    if ! command -v sqlite3 >/dev/null 2>&1; then
        log_error "sqlite3 is not installed or not in PATH"
        exit 1
    fi
}

# 初始化环境
start_initialize() {
    log_info "Initializing environment..."
    
    # 检查 Docker
    check_docker
    
    # 创建数据库目录
    if [ ! -d "$DB_DIR" ]; then
        log_info "Creating database directory: $DB_DIR"
        if ! mkdir -p "$DB_DIR"; then
            log_error "Failed to create database directory: $DB_DIR"
            exit 1
        fi
    fi
    
    # 拉取 Docker 镜像
    log_info "Pulling Docker image: $IMAGE_NAME"
    if ! docker pull "$IMAGE_NAME"; then
        log_error "Failed to pull Docker image: $IMAGE_NAME"
        exit 1
    fi

    log_info "Initialization completed!"
}

# 备份数据库
start_backup_db() {
    local instance=${1:-0}
    local db_file=$(get_dbfile_name $instance)
    local full_db_path="$DB_DIR/$db_file"
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local backup_dir="backups"
    local backup_file="${backup_dir}/${db_file}_${timestamp}.sql"
    
    # 验证实例号是否有效
    if [[ ! "$instance" =~ ^[0-9]+$ ]]; then
        log_error "Instance number must be a non-negative integer"
        exit 1
    fi    
    # # 验证数据库文件名是否安全
    # if [[ "$db_file" != *.sqlite ]] || [[ "$db_file" == *"/"* ]]; then
    #     log_error "Invalid database filename: $db_file"
    #     exit 1
    # fi
    
    log_info "Backing up database instance $instance"
    log_info "Source: $full_db_path"
    log_info "Destination: $backup_file"
    
    # 检查 SQLite3
    check_sqlite3
    
    # 检查数据库文件是否存在
    if [ ! -f "$full_db_path" ]; then
        log_error "Database $full_db_path does not exist!"
        exit 1
    fi
    
    # 创建备份目录
    if [ ! -d "$backup_dir" ]; then
        if ! mkdir -p "$backup_dir"; then
            log_error "Failed to create backup directory: $backup_dir"
            exit 1
        fi
    fi
    
    # 执行备份
    if ! echo ".dump" | sqlite3 "$full_db_path" > "$backup_file"; then
        log_error "Failed to backup database: $full_db_path"
        exit 1
    fi
    
    log_info "Backup successfully created: $backup_file"
    
    # 显示备份文件大小
    if backup_size=$(du -h "$backup_file" 2>/dev/null | cut -f1); then
        log_info "Backup size: $backup_size"
    else
        log_warn "Could not determine backup file size"
    fi
}

# 运行 SQLite Web UI
start_sqlite_webui() {
    local instance=${1:-0}
    local db_file=$(get_dbfile_name $instance)
    local port=$(get_port $instance)
    local container_name=$(get_container_name $instance)
    local full_db_path="$DB_DIR/$db_file"
    
    # 验证实例号是否有效
    if [[ ! "$instance" =~ ^[0-9]+$ ]]; then
        log_error "Instance number must be a non-negative integer"
        exit 1
    fi
    
    # # 验证数据库文件名是否安全
    # if [[ "$db_file" != *.sqlite ]] || [[ "$db_file" == *"/"* ]]; then
    #     log_error "Invalid database filename: $db_file"
    #     exit 1
    # fi
    # # 验证数据库目录是否存在
    # if [ ! -d "$DB_DIR" ]; then
    #     log_error "Database directory does not exist: $DB_DIR"
    #     exit 1
    # fi
    
    log_info "Starting sqlite-web for instance $instance"
    log_info "Database file: $db_file"
    log_info "Port mapping: $port:8080"
    log_info "Container name: $container_name"
    
    # 验证数据库文件是否存在，如果不存在则创建
    if [ ! -f "$full_db_path" ]; then
        log_warn "Database file $full_db_path does not exist!"
        log_info "Creating empty database..."
        if ! touch "$full_db_path"; then
            log_error "Failed to create database file: $full_db_path"
            exit 1
        fi
    fi
    
    # 检查端口是否被占用
    if check_port $port; then
        log_error "Port $port is already in use!"
        exit 1
    fi
    
    # 停止已存在的容器
    if docker ps -q -f name="$container_name" | grep -q .; then
        log_info "Stopping existing container: $container_name"
        if ! docker stop "$container_name" >/dev/null; then
            log_error "Failed to stop container: $container_name"
            exit 1
        fi
    fi
    
    # 运行新容器
    log_info "Running container: $container_name"
    local current_dir=$(pwd)
    if ! docker run -it --rm --name "$container_name" -p ${port}:8080 \
        -v "$current_dir/$DB_DIR:/data" \
        "$IMAGE_NAME" \
        "/data/$db_file"; then
        
        log_error "Failed to start container: $container_name"
        exit 1
    fi
}

# 停止 SQLite Web UI
stop_container() {
    local instance=${1:-0}
    local container_name=$(get_container_name $instance)
    
    # 验证实例号是否有效
    if [[ ! "$instance" =~ ^[0-9]+$ ]]; then
        log_error "Instance number must be a non-negative integer"
        exit 1
    fi
    
    log_info "Stopping container: $container_name"
    
    if docker ps -q -f name="$container_name" | grep -q .; then
        if docker stop "$container_name" >/dev/null; then
            log_info "Container $container_name stopped successfully"
        else
            log_error "Failed to stop container: $container_name"
            exit 1
        fi
    else
        log_warn "Container $container_name is not running"
    fi
}

# 主逻辑
main() {
    local command=${1:-""}
    local instance=${2:-0}
    
    # 验证命令
    case "$command" in
        "init"|"backup"|"run"|"stop")
            ;;
        "help"|"-h"|"--help")
            show_help
            exit 0
            ;;
        "")
            show_help
            exit 1
            ;;
        *)
            log_error "Invalid command '$command'"
            show_help
            exit 1
            ;;
    esac
    
    # 验证实例号为数字（仅对需要实例号的命令）
    if [[ "$command" == "backup" || "$command" == "run" || "$command" == "stop" ]]; then
        if [ ! -z "$2" ] && [[ ! "$2" =~ ^[0-9]+$ ]]; then
            log_error "Instance number must be a non-negative integer"
            show_help
            exit 1
        fi
    fi
    
    # 执行命令
    case "$command" in
        "init")
            start_initialize
            ;;
        "backup")
            start_backup_db $instance
            ;;
        "run")
            start_sqlite_webui $instance
            ;;
        "stop")
            stop_container $instance
            ;;
    esac
}

# 启动主程序
main "$@"
