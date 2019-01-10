#!/bin/bash
#
# Date: 2018-12-11
# Author: superz
# Description: 从hdfs上获取访问日志(json格式),分析得到活跃用户并写入mysql表
# 调度系统变量
#   log_path     任务日志目录
#   prev_day     run_time前一天(yyyyMMdd)
#   prev_day1    run_time前一天(yyyy-MM-dd)
# 任务扩展属性:
# hdfs_dir        访问日志hdfs根目录
# product_code    产品编码
# ad_db_id        广告数据库id
# tbl_new         新增用户表名(默认为fact_new_$product_code)
# tbl_active      活跃用户表名(默认为fact_active_$product_code)
# 任务运行时参数:
# start_date      开始日期(默认为run_time的前一天)
# end_date        结束日期(默认为start_date)


source $ETL_HOME/common/db_util.sh


# 获取数据
function get_data()
{
    # 从hdfs下载访问日志(json格式)
    file_visit=$data_path/visit.hdfs
    > $file_visit
    range_date ${start_date//-/} ${end_date//-/} | while read the_date; do
        the_date=`date +%F -d "$the_date"`
        if [[ ! -d $data_path/$the_date ]]; then
            info "hdfs dfs -get $hdfs_dir/$product_code/$the_date $data_path"
            hdfs dfs -get $hdfs_dir/$product_code/$the_date $data_path
        fi

        # json格式转扁平格式
        ls $data_path/$the_date | while read file; do
            debug "Format file: $data_path/$the_date/$file"
            sed 's/\":\"\|\",\"/\t/g;s/{\"\|\"}//g' $data_path/$the_date/$file |
            awk -F '\t' 'BEGIN{OFS=FS}{print $2,$6,$10}' >> $file_visit
        done
    done

    # 访问日志日期范围
    date_range=(`awk -F '\t' '{
        visit_date=substr($3,0,10);
        gsub("-","",visit_date);
        arr[++i]=visit_date
    }END{
        len=asort(arr,sarr)
        print sarr[1],sarr[len]
    }' $file_visit`)
    min_date=${date_range[0]}
    max_date=${date_range[1]}

    # 从数据库获取活跃用户
    file_active=$data_path/active.table
    if [[ ! -s $file_active ]]; then
        echo "SELECT aid, area, STR_TO_DATE(active_date,'%Y%m%d') FROM $tbl_active WHERE active_date >= $min_date AND active_date <= $max_date;
        " | exec_sql > $file_active
    fi
}

# 解析数据
function parse_data()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 合并访问日志和活跃用户
    # 按访问时间排序后逐条对比更新
    local file_active1=$data_path/active1
    cat $file_visit $file_active | sort -t $'\t' -k 1,1 -k 3,3 | awk -F '\t' 'BEGIN{
        OFS=FS
    }{
        if($1 == aid && substr($3,1,10) == active_date){
            visit_times++
        }else{
            if(aid != "") print aid,area,active_date,visit_times
            aid=$1
            area=$2
            active_date=substr($3,1,10)
            gsub("-","",active_date)
            visit_times=1
        }
    }END{
        print aid,area,active_date,visit_times
    }' > $file_active1

    # 获取新增用户
    file_new=$data_path/new.table
    echo "SELECT aid,channel_code,create_date FROM $tbl_new;" | exec_sql > $file_new

    # 关联新增用户表获取channel_code,create_date
    file_result=$data_path/active.result
    sort $file_new -o $file_new
    sort $file_active1 -o $file_active1
    join -t "$sep" -o 1.1 2.2 1.2 1.3 2.3 1.4 $file_active1 $file_new > $file_result
}

# 创建表
function create_table()
{
    echo "CREATE TABLE $tbl_active (
      aid VARCHAR(64),
      channel_code VARCHAR(32),
      area VARCHAR(16),
      active_date INT,
      create_date INT,
      date_diff INT,
      visit_times INT,
      PRIMARY KEY (aid, active_date),
      KEY idx_channel_code (channel_code),
      KEY idx_area (area),
      KEY idx_active_date (active_date),
      KEY idx_create_date (create_date),
      KEY idx_date_diff (date_diff),
      KEY idx_visit_times (visit_times)
    ) ENGINE=InnoDB COMMENT='活跃用户';
    " | exec_sql
}

# 导入数据库
function load_data()
{
    echo "DELETE FROM $tbl_active WHERE active_date >= $min_date AND active_date <= $max_date;
    ALTER TABLE $tbl_active DISABLE KEYS;
    LOAD DATA LOCAL INFILE '$file_result' INTO TABLE $tbl_active (aid, channel_code, area, active_date, create_date, visit_times)
    SET date_diff = DATEDIFF(active_date, create_date);
    ALTER TABLE $tbl_active ENABLE KEYS;
    " | exec_sql
}

function execute()
{
    # 开始日期
    start_date=`awk -F '=' '$1 == "start_date" {print $2}' $log_path/run_params`
    start_date=${start_date:-$prev_day1}
    # 结束日期
    end_date=`awk -F '=' '$1 == "end_date" {print $2}' $log_path/run_params`
    end_date=${end_date:-$start_date}

    # 新增用户表
    tbl_new=${tbl_new:-fact_new_$product_code}
    # 活跃用户表
    tbl_active=${tbl_active:-fact_active_$product_code}

    # 设置数据库
    set_db $ad_db_id

    # 创建表
    log_task $LOG_LEVEL_INFO "Create table"
    create_table

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