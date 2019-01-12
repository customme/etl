#!/bin/bash
#
# Date: 2015-09-21
# Author: superz
# Description: 运营商维度
# 任务扩展属性:
#   tbl_isp    维度表名(默认为dim_isp)
#   db_id      数据库id


source $ETL_HOME/common/db_util.sh


# 创建表
function create_table()
{
    echo "CREATE TABLE IF NOT EXISTS $tbl_isp (
      id INT(11),
      isp_name VARCHAR(64),
      PRIMARY KEY (id)
    ) ENGINE=MyISAM COMMENT='运营商';
    " | exec_sql
}

# 添加数据
function add_data()
{
    echo "INSERT INTO $tbl_isp (id, isp_name) VALUES
    (0, '未知'),
    (46000, '中国移动'),
    (46001, '中国联通'),
    (46002, '中国移动'),
    (46003, '中国电信'),
    (46007, '中国移动'),
    (46099, '中国电信');
    " | exec_sql
}

function execute()
{
    # 表名
    tbl_isp=${tbl_isp:-dim_isp}

    # 设置数据库
    set_db $db_id

    # 创建表
    create_table

    # 添加数据
    add_data
}
execute "$@"