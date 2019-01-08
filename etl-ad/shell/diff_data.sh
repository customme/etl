#!/bin/bash
#
# spark统计和shell统计两种数据对比


MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWD=mysql
MYSQL_NAME=ad_dw
MYSQL_CHARSET=utf8

TABLES="agg_active_adv_n_l_1 active_date,create_date,date_diff,fact_count
agg_active_adv_n_l_2 active_date,create_date,date_diff,channel_code,fact_count
agg_active_adv_n_l_3 active_date,create_date,date_diff,area,fact_count
agg_active_adv_n_l_4 active_date,create_date,date_diff,channel_code,area,fact_count
agg_new_adv_n_l_1 create_date,fact_count
agg_new_adv_n_l_2 channel_code,fact_count
agg_new_adv_n_l_3 area,fact_count
agg_new_adv_n_l_4 create_date,channel_code,fact_count
agg_new_adv_n_l_5 create_date,area,fact_count
agg_new_adv_n_l_6 channel_code,area,fact_count
agg_new_adv_n_l_7 create_date,channel_code,area,fact_count
fact_active_adv_n aid,channel_code,area,active_date,create_date,date_diff,visit_times
fact_new_adv_n aid,channel_code,init_area,area,init_ip,ip,create_time,update_time,create_date"


function exec_sql()
{
    local sql="${1:-`cat`}"
    local params="${2:--s -N --local-infile}"

    echo "SET NAMES $MYSQL_CHARSET;$sql" | mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWD $MYSQL_NAME $params
}

function main()
{
    echo "$TABLES" | while read table columns; do
        echo "SELECT $columns FROM ad_dw1.$table" | exec_sql | sort > $table.ad_dw1
        echo "SELECT $columns FROM ad_dw2.$table" | exec_sql | sort > $table.ad_dw2
        diff $table.ad_dw1 $table.ad_dw2 > $table.diff
        if [[ -s $table.diff ]]; then
            echo "$table does not match"
        else
            rm -f $table.ad_dw1 $table.ad_dw2 $table.diff
        fi
    done
}
main "$@"