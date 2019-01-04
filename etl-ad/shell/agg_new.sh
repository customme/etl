#!/bin/bash
#
# Date: 2018-12-15
# Author: superz
# Description: 生成新增用户聚合表
# 调度系统变量
#   log_path     任务日志目录
#   prev_day     run_time前一天(yyyyMMdd)
# 任务扩展属性:
# product_code    产品编码
# ad_db_id        广告数据库id
# tbl_new         新增用户表名(默认为fact_new_$product_code)


source $ETL_HOME/common/db_util.sh


# 解析聚合规则
function parse_rule()
{
    if [[ -n "$agg_rules" ]]; then
        echo "$agg_rules" | while read rule columns; do
            
        done
    else
        echo "$agg_columns"
    fi
}

# 聚合
function aggregate()
{
    # 设置数据库
    set_db $ad_db_id

    echo "CREATE TABLE IF NOT EXISTS ${tp_agg_new}l_1 (
      create_date INT,
      fact_count INT,
      PRIMARY KEY(create_date)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_new}l_01
    SELECT create_date, COUNT(1)
    FROM $tbl_fact_new
    WHERE create_date >= ${start_date//-/} AND create_date <= ${end_date//-/}
    GROUP BY create_date;

    CREATE TABLE IF NOT EXISTS ${tp_agg_new}l_02 (
      create_date INT,
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(create_date, channel_code)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_new}l_02
    SELECT create_date, channel_code, COUNT(1)
    FROM $tbl_fact_new
    WHERE create_date >= ${start_date//-/} AND create_date <= ${end_date//-/}
    GROUP BY create_date, channel_code;

    CREATE TABLE IF NOT EXISTS ${tp_agg_new}l_03 (
      create_date INT,
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(create_date, area)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_new}l_03
    SELECT create_date, area, COUNT(1)
    FROM $tbl_fact_new
    WHERE create_date >= ${start_date//-/} AND create_date <= ${end_date//-/}
    GROUP BY create_date, area;

    CREATE TABLE IF NOT EXISTS ${tp_agg_new}l_04 (
      create_date INT,
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(create_date, channel_code, area)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_new}l_04
    SELECT create_date, channel_code, area, COUNT(1)
    FROM $tbl_fact_new
    WHERE create_date >= ${start_date//-/} AND create_date <= ${end_date//-/}
    GROUP BY create_date, channel_code, area;
    "
}

function execute()
{
    # 新增用户表
    tbl_new=${tbl_new:-fact_new_$product_code}

    # 开始日期
    start_date=`awk -F '=' '$1 == "start_date" {print $2}' $log_path/run_params`
    start_date=${start_date:-$prev_day1}
    # 结束日期
    end_date=`awk -F '=' '$1 == "end_date" {print $2}' $log_path/run_params`
    end_date=${end_date:-$start_date}

    # 聚合
    aggregate
}
execute "$@"