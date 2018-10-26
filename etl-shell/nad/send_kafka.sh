#!/bin/bash
#
# 向kafka写入json数据


# 数据文件目录
DATADIR=/var/data/native

# zookeeper
ZK_LIST=yygz-65.gzserv.com:2181,yygz-66.gzserv.com:2181,yygz-67.gzserv.com:2181

# broker
BROKER_LIST=yygz-65.gzserv.com:9092,yygz-66.gzserv.com:9092,yygz-67.gzserv.com:9092


function main()
{
    find $DATADIR -mindepth 1 -maxdepth 1 -type d | while read the_dir; do
        prod_id=`basename $the_dir`

        # 创建topic
        $KAFKA_HOME/bin/kafka-topics.sh --create --replication-factor 2 --partitions 3 --topic $prod_id --zookeeper $ZK_LIST

        ls $the_dir/visit.* | grep -v json | while read file_visit; do
            # 转json
            awk -F '\t' '{
                printf("{\"aid\":\"%s\",\"cuscode\":\"%s\",\"city\":\"%s\",\"ip\":\"%s\",\"create_time\":\"%s\"}\n",$1,$2,$3,$4,$5)
            }' $file_visit > ${file_visit}.json

            # 发送数据
            $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BROKER_LIST --topic $prod_id < ${file_visit}.json
        done
    done

    # 消费
    #$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $BROKER_LIST --topic $prod_id --from-beginning 100
}
