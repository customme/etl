#!/bin/bash
#
# Date: 2018-12-11
# Author: superz
# Description: 从hdfs上获取访问日志(json格式),分析得到新增用户并写入mysql表
# 任务扩展属性:
# hdfs_dir        访问日志hdfs根目录
# product_code    产品编码
# ad_db_id        数据库id
# tbl_new         新增用户表名(默认为fact_new_$product_code)
# 任务运行时参数:
# start_date      开始日期(默认为run_time的前一天)
# end_date        结束日期(默认为start_date)


source $ETL_HOME/common/db_util.sh


# 获取数据
function get_data()
{
    # 从hdfs下载访问日志(json格式)
    > $file_visit
    range_date ${start_date//-/} ${end_date//-/} | while read the_date; do
        the_date=`date +%F -d "$the_date"`
        if [[ ! -d $data_path/$the_date ]]; then
            info "hdfs dfs -get $hdfs_dir/$product_code/$the_date $data_path"
            hdfs dfs -get $hdfs_dir/$product_code/$the_date $data_path
        fi

        # json格式转列表格式
        ls $data_path/$the_date | while read file; do
            debug "Format file: $data_path/$the_date/$file"
            sed 's/\":\"\|\",\"/\t/g;s/{\"\|\"}//g' $data_path/$the_date/$file |
            awk -F '\t' 'BEGIN{OFS=FS}{print $2,$4,$6,$8,$10}' >> $file_visit
        done
    done

    # 从数据库获取设备信息
    if [[ ! -s $file_new ]]; then
        # 设置数据库
        set_db $ad_db_id

        echo "SELECT aid, channel_code, init_area, area, init_ip, ip, create_time, update_time FROM $tbl_new;" | exec_sql >> $file_new
    fi

    # 合并数据
    cat $file_visit $file_new > $file_merge
}

# 解析数据
function parse_data()
{
    export LC_ALL=C

    # 排序
    sort -t $'\t' -k 1,1 -k 5,5 $file_merge | awk -F '\t' 'BEGIN{
        OFS=FS
    }{
        if($1 == aid){
            area=$3
            ip=$4
            update_time=$5
        }else{
            if(aid != "") print aid,channel_code,init_area,area,init_ip,ip,create_time,update_time
            aid=$1
            channel_code=$2
            init_area=$3
            area=$3
            init_ip=$4
            ip=$4
            create_time=$5
            update_time=$5
        }
    }END{
        print aid,channel_code,init_area,area,init_ip,ip,create_time,update_time
    }' > $file_result
}

# 导入数据库
function load_data()
{
    # 设置数据库
    set_db $ad_db_id

    echo "LOCK TABLES $tbl_new WRITE;
    DROP TABLE IF EXISTS ${tbl_new}_$prev_day;
    RENAME TABLE $tbl_new TO ${tbl_new}_$prev_day;
    CREATE TABLE $tbl_new LIKE ${tbl_new}_$prev_day;
    LOAD DATA LOCAL INFILE '$file_result' INTO TABLE $tbl_new (aid, channel_code, init_area, area, init_ip, ip, create_time, update_time);
    UNLOCK TABLES;
    " | exec_sql
}

function execute()
{
    # 开始/结束日期
    start_date=`awk -F '=' '$1 == "start_date" {print $2}' $log_path/run_params`
    start_date=${start_date:-$prev_day1}
    end_date=`awk -F '=' '$1 == "end_date" {print $2}' $log_path/run_params`
    end_date=${end_date:-$start_date}

    # 新增用户表
    tbl_new=${tbl_new:-fact_new_$product_code}

    # 从hdfs获取并格式化合并后的文件
    file_visit=$data_path/visit.hdfs
    # 从数据库获取的新增用户文件
    file_new=$data_path/new.table
    # 合并hdfs和数据库后的文件
    file_merge=$data_path/visit.merge
    # 解析结果存储文件
    file_result=$data_path/new.result

    # 获取数据
    log_task $LOG_LEVEL_INFO "Get data"
    get_data

    # 解析数据
    log_task $LOG_LEVEL_INFO "Parse data"
    parse_data

    # 导入数据库
    log_task $LOG_LEVEL_INFO "Load data"
    load_data
}
execute "$@"