#!/bin/bash
#
# Date: 2015-09-21
# Author: superz
# Description: 日期维度
# 任务扩展属性:
#   tbl_date          日期表名(默认为dim_date)
#   start_date        开始日期(默认为运行日期the_day)
#   end_date          结束日期(默认为开始日期一个月后)
#   interval_month    间隔月数(默认为1)
#   db_id             数据库id


source $ETL_HOME/common/db_util.sh


# 创建表
function create_table()
{
    echo "CREATE TABLE IF NOT EXISTS $tbl_date (
      id INT,
      the_year SMALLINT,
      half_year TINYINT,
      the_quarter TINYINT,
      month_of_year TINYINT,
      week_of_year TINYINT,
      day_of_month TINYINT,
      week_day TINYINT,
      the_date DATE,
      order_no INT,
      quarter_name CHAR(2),
      month_name VARCHAR(9),
      abbr_month_name VARCHAR(3),
      cn_month_name VARCHAR(9),
      week_name VARCHAR(9),
      abbr_week_name VARCHAR(3),
      cn_week_name VARCHAR(9),
      PRIMARY KEY (id)
    ) ENGINE=MyISAM COMMENT='日期维度';
    " | exec_sql
}

# 添加日期
function add_date()
{
    export LC_TIME=en_US.UTF-8

    range_date $start_date $end_date | awk '{
        id=$1
        year=substr(id, 0, 4)
        month=int(substr(id, 5, 2))
        day=int(substr(id, 7, 2))
        the_time=mktime(year" "month" "day" 00 00 00")

        # 季度
        if(month >= 1 && month <= 3){
            quarter=1
        }else if(month >= 4 && month <= 6){
            quarter=2
        }else if(month >= 7 && month <= 9){
            quarter=3
        }else if(month >= 10 && month <= 12){
            quarter=4
        }

        # 半年
        if(quarter == 1 || quarter == 2){
            half_year=1
        }else if(quarter == 3 || quarter == 4){
            half_year=2
        }

        # 一年中的第几个星期(星期一作为一个星期的开始)
        week_of_year=strftime("%V", the_time)
        # 星期几(星期天是0)
        week_day=strftime("%w", the_time)
        if(week_day == 0){
            week_day=6
        }else{
            week_day--
        }

        # 排序用字段
        order_no=20890520 - id

        # 月名的完整写法(October)
        month_name=strftime("%B",the_time)
        # 月名的缩写(Oct)
        abbr_month_name=strftime("%b",the_time)

        # 星期几的完整写法(Sunday)
        week_name=strftime("%A",the_time)
        # 星期几的缩写(Sun)
        abbr_week_name=strftime("%a",the_time)

        # 简体中文月份
        cn_month[1]="一月"
        cn_month[2]="二月"
        cn_month[3]="三月"
        cn_month[4]="四月"
        cn_month[5]="五月"
        cn_month[6]="六月"
        cn_month[7]="七月"
        cn_month[8]="八月"
        cn_month[9]="九月"
        cn_month[10]="十月"
        cn_month[11]="十一月"
        cn_month[12]="十二月"

        # 简体中文周几
        cn_week[0]="周一"
        cn_week[1]="周二"
        cn_week[2]="周三"
        cn_week[3]="周四"
        cn_week[4]="周五"
        cn_week[5]="周六"
        cn_week[6]="周日"

        print "INSERT IGNORE INTO '$tbl_date' (id, the_year, half_year, the_quarter, month_of_year, week_of_year, day_of_month, week_day, the_date, order_no, quarter_name, month_name, abbr_month_name, cn_month_name, week_name, abbr_week_name, cn_week_name) VALUES"
        printf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, \"Q%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");\n",id, year, half_year, quarter,month, week_of_year, day, week_day, id, order_no, quarter, month_name, abbr_month_name, cn_month[month], week_name, abbr_week_name, cn_week[week_day])
    }' | exec_sql
}

function execute()
{
    # 开始日期
    start_date=`awk -F '=' '$1 == "start_date" {print $2}' $log_path/run_params`
    start_date=${start_date:-$the_day}
    # 间隔月数
    interval_month=${interval_month:-1}
    _end_date=$(date +%Y%m%d -d "$start_date $interval_month month 1 day ago")
    # 结束日期
    end_date=`awk -F '=' '$1 == "end_date" {print $2}' $log_path/run_params`
    end_date=${end_date:-$_end_date}

    # 表名
    tbl_date=${tbl_date:-dim_date}

    # 设置数据库
    set_db $db_id

    # 创建表
    log_task $LOG_LEVEL_INFO "Create table"
    create_table

    # 添加日期
    log_task $LOG_LEVEL_INFO "Add date"
    add_date
}
execute "$@"