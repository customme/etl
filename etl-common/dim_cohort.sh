#!/bin/bash
#
# Date: 2015-09-21
# Author: superz
# Description: 群组维度
# 任务扩展属性:
#   tbl_cohort          日期表名(默认为dim_cohort)
#   db_id             数据库id


source $ETL_HOME/common/db_util.sh


# 创建表
function create_table()
{
    echo "CREATE TABLE IF NOT EXISTS $tbl_cohort (
      id SMALLINT(6),
      day_num VARCHAR(4),
      week_num VARCHAR(2),
      month_num VARCHAR(2),
      quarter_num VARCHAR(2),
      year_num VARCHAR(2),
      PRIMARY KEY(id)
    ) ENGINE =MyISAM COMMENT = '群组';
    " | exec_sql
}

# 添加数据
function add_data()
{
    echo ${1:-3600} | awk 'BEGIN{OFS="\t"}{
        for(i=0;i<=$1;i++){
            year=int(i/365)
            month=int(i%365/30)
            if(month>11) month=11
            week=int(i/7)

            print i, i + 1, week + 1, year * 12 + month + 1, year * 4 + int(month / 3) + 1, year + 1
        }
    }' > /tmp/dim_cohort.tmp

    echo "INSERT INTO $tbl_cohort (id, day_num, week_num, month_num, quarter_num, year_num) VALUES (-1, '未知', '未知', '未知', '未知', '未知');
    LOAD DATA LOCAL INFILE 'dim_cohort.tmp' INTO TABLE dim_cohort (id, day_num, week_num, month_num, quarter_num, year_num);
    " | exec_sql

    # 删除临时文件
    rm -f /tmp/dim_cohort.tmp

    # 次数区间
    echo "ALTER TABLE $tbl_cohort ADD COLUMN times SMALLINT(6), ADD COLUMN times_desc VARCHAR(10);
    UPDATE dim_cohort
    SET times = CASE
    WHEN id <= 0 THEN 0
    WHEN id > 0 AND id <= 2 THEN 1
    WHEN id > 2 AND id <= 5 THEN 2
    WHEN id > 5 AND id <= 9 THEN 3
    WHEN id > 9 AND id <= 19 THEN 4
    WHEN id > 19 AND id <= 49 THEN 5
    ELSE 6
    END,
    times_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id > 0 AND id <= 2 THEN '1-2次'
    WHEN id > 2 AND id <= 5 THEN '3-5次'
    WHEN id > 5 AND id <= 9 THEN '6-9次'
    WHEN id > 9 AND id <= 19 THEN '10-19次'
    WHEN id > 19 AND id <= 49 THEN '20-49次'
    ELSE '50次+'
    END;
    " | exec_sql
}

function execute()
{
    # 表名
    tbl_cohort=${tbl_cohort:-dim_cohort}

    # 设置数据库
    set_db $db_id

    # 创建表
    create_table

    # 添加数据
    add_data
}
execute "$@"