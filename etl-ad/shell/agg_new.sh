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
# agg_prefix      聚合表前缀(默认为agg_new_${product_code}_)


source $ETL_HOME/common/db_util.sh


# 创建表
function create_table()
{
    echo "CREATE TABLE IF NOT EXISTS ${agg_prefix}l_1 (
      create_date INT,
      fact_count INT,
      PRIMARY KEY (create_date)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_2 (
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (channel_code)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_3 (
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (area)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_4 (
      create_date INT,
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (create_date, channel_code)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_5 (
      create_date INT,
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (create_date, area)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_6 (
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (channel_code, area)
    ) ENGINE=MyISAM;
    CREATE TABLE IF NOT EXISTS ${agg_prefix}l_7 (
      create_date INT,
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (create_date, channel_code, area)
    ) ENGINE=MyISAM;
    " | exec_sql
}

# 聚合
function aggregate()
{
    echo "DELETE FROM ${agg_prefix}l_1 WHERE $src_filter;
    INSERT INTO ${agg_prefix}l_1
    SELECT create_date, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY create_date;

    TRUNCATE TABLE ${agg_prefix}l_2;
    INSERT INTO ${agg_prefix}l_2
    SELECT channel_code, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY channel_code;

    TRUNCATE TABLE ${agg_prefix}l_3;
    INSERT INTO ${agg_prefix}l_3
    SELECT area, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY area;

    DELETE FROM ${agg_prefix}l_4 WHERE $src_filter;
    INSERT INTO ${agg_prefix}l_4
    SELECT create_date, channel_code, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY create_date, channel_code;

    DELETE FROM ${agg_prefix}l_5 WHERE $src_filter;
    INSERT INTO ${agg_prefix}l_5
    SELECT create_date, area, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY create_date, area;

    TRUNCATE TABLE ${agg_prefix}l_6;
    INSERT INTO ${agg_prefix}l_6
    SELECT channel_code, area, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY channel_code, area;

    DELETE FROM ${agg_prefix}l_7 WHERE $src_filter;
    INSERT INTO ${agg_prefix}l_7
    SELECT create_date, channel_code, area, COUNT(1) FROM $tbl_new WHERE $src_filter GROUP BY create_date, channel_code, area;
    " | exec_sql
}

function execute()
{
    # 新增用户表
    tbl_new=${tbl_new:-fact_new_$product_code}
    # 聚合表前缀
    agg_prefix=${agg_prefix-agg_new_${product_code}_}

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