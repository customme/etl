# 数据库操作公共类


# 根据数据库id获取数据库连接信息
function set_db()
{
    local db_id=$1
    if [[ -z "$db_id" ]]; then
        error "Empty database id"
        return 1
    fi

    db_conn=($(get_db $db_id))
    if [[ -z "${db_conn[@]}" ]]; then
        error "Can not find database by id: $db_id"
        return 1
    fi
    info "Got database: ${db_conn[@]}"
}

# 执行sql语句
function exec_sql()
{
    local sql="$1"
    local extras="$2"
    if [[ -z "$sql" ]]; then
        sql=`cat`
    fi

    local sql_log_file=${log_path:-.}/etl_sql.log
    local db_url=$(make_mysql_url "${db_conn[1]}" "${db_conn[3]}" "${db_conn[4]}" "${db_conn[5]}" "${db_conn[2]}")
    local db_charset=${db_conn[6]}

    mysql_executor "SET NAMES $db_charset;$sql" "$db_url $extras"
}