#!/bin/bash
#
# Date: 2018-12-15
# Author: superz
# Description: 生成活跃用户聚合表
# 调度系统变量
#   log_path     任务日志目录
#   prev_day     run_time前一天(yyyyMMdd)
# 任务扩展属性:
# product_code    产品编码
# ad_db_id        广告数据库id
# tbl_new         新增用户表名(默认为fact_new_$product_code)


source $ETL_HOME/common/db_util.sh


# 聚合
function aggregate()
{
    # 设置数据库
    set_db $ad_db_id

    echo "CREATE TABLE IF NOT EXISTS ${tp_agg_active}l_01 (
      active_date INT,
      create_date INT,
      date_diff INT,
      fact_count INT,
      PRIMARY KEY(active_date, create_date)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_active}l_01
    SELECT active_date, create_date, date_diff, COUNT(1)
    FROM $tbl_fact_active
    WHERE active_date >= ${start_date//-/} AND active_date <= ${end_date//-/}
    GROUP BY active_date, create_date;

    CREATE TABLE IF NOT EXISTS ${tp_agg_active}l_02 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, channel_code)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_active}l_02
    SELECT active_date, create_date, date_diff, channel_code, COUNT(1)
    FROM $tbl_fact_active
    WHERE active_date >= ${start_date//-/} AND active_date <= ${end_date//-/}
    GROUP BY active_date, create_date, channel_code;

    CREATE TABLE IF NOT EXISTS ${tp_agg_active}l_03 (
      active_date INT,
      create_date INT,
      date_diff INT,
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, area)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_active}l_03
    SELECT active_date, create_date, date_diff, area, COUNT(1)
    FROM $tbl_fact_active
    WHERE active_date >= ${start_date//-/} AND active_date <= ${end_date//-/}
    GROUP BY active_date, create_date, area;

    CREATE TABLE IF NOT EXISTS ${tp_agg_active}l_04 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, channel_code, area)
    ) ENGINE=MyISAM;
    REPLACE INTO ${tp_agg_active}l_04
    SELECT active_date, create_date, date_diff, channel_code, area, COUNT(1)
    FROM $tbl_fact_active
    WHERE active_date >= ${start_date//-/} AND active_date <= ${end_date//-/}
    GROUP BY active_date, create_date, channel_code, area;
    "
}

function execute()
{
    # 活跃用户表
    tbl_active=${tbl_active:-fact_active_$product_code}

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