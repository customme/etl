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
# tbl_active      活跃用户表名(默认为fact_active_$product_code)
# agg_prefix      聚合表前缀(默认为agg_active_$product_code)


source $ETL_HOME/common/db_util.sh


# 创建表
function create_table()
{
    echo "CREATE TABLE IF NOT EXISTS ${agg_prefix}_1 (
      active_date INT,
      create_date INT,
      date_diff INT,
      fact_count INT,
      PRIMARY KEY(active_date, create_date)
    ) ENGINE=MyISAM;

    CREATE TABLE IF NOT EXISTS ${agg_prefix}_2 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, channel_code)
    ) ENGINE=MyISAM;

    CREATE TABLE IF NOT EXISTS ${agg_prefix}_3 (
      active_date INT,
      create_date INT,
      date_diff INT,
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, area)
    ) ENGINE=MyISAM;

    CREATE TABLE IF NOT EXISTS ${agg_prefix}_4 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY(active_date, create_date, channel_code, area)
    ) ENGINE=MyISAM;
    " | exec_sql
}

# 聚合
function aggregate()
{
    echo "DELETE FROM ${agg_prefix}_1 WHERE $src_filter;
    INSERT INTO ${agg_prefix}_1
    SELECT active_date, create_date, date_diff, COUNT(1) FROM $tbl_active WHERE $src_filter GROUP BY active_date, create_date;

    DELETE FROM ${agg_prefix}_2 WHERE $src_filter;
    INSERT INTO ${agg_prefix}_2
    SELECT active_date, create_date, date_diff, channel_code, COUNT(1) FROM $tbl_active WHERE $src_filter GROUP BY active_date, create_date, channel_code;

    DELETE FROM ${agg_prefix}_3 WHERE $src_filter;
    INSERT INTO ${agg_prefix}_3
    SELECT active_date, create_date, date_diff, area, COUNT(1) FROM $tbl_active WHERE $src_filter GROUP BY active_date, create_date, area;

    DELETE FROM ${agg_prefix}_4 WHERE $src_filter;
    INSERT INTO ${agg_prefix}_4
    SELECT active_date, create_date, date_diff, channel_code, area, COUNT(1) FROM $tbl_active WHERE $src_filter GROUP BY active_date, create_date, channel_code, area;
    " | exec_sql
}

function execute()
{
    # 活跃用户表
    tbl_active=${tbl_active:-fact_active_$product_code}
    # 聚合表前缀
    agg_prefix=${agg_prefix-agg_active_$product_code}

    if [[ $is_first ]]; then
        src_filter="1 = 1"
    else
        # 开始日期
        start_date=`awk -F '=' '$1 == "start_date" {print $2}' $log_path/run_params`
        start_date=${start_date:-$prev_day}
        # 结束日期
        end_date=`awk -F '=' '$1 == "end_date" {print $2}' $log_path/run_params`
        end_date=${end_date:-$start_date}

        src_filter="create_date >= ${start_date//-/} AND create_date <= ${end_date//-/}"
    fi
    info "$src_filter"

    # 设置数据库
    set_db $ad_db_id

    # 创建表
    create_table

    # 聚合
    aggregate
}
execute "$@"