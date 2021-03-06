package com.jiuzhi.etl.ad

import org.apache.spark.sql.functions._

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.apache.spark.sql.streaming.OutputMode

class RealtimeNewStat(task: Task) extends TaskExecutor(task) with Serializable {

  // 产品编码
  val productCode = task.taskExt.get("product_code").get
  // 广告数据库
  val dbAd = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
  // 新增用户表
  val tableNew = task.taskExt.getOrElse("tbl_new", s"realtime_new_${productCode}")

  // kafka配置
  val brokerList = task.taskExt.get("broker_list").get
  val groupId = task.taskExt.getOrElse("group_id", "realtime_new_stat")
  val topic = task.taskExt.get("topic").getOrElse(productCode)
  val startOffset = task.taskExt.getOrElse("start_offset", "earliest")

  val kafkaProps = Map(
    "kafka.bootstrap.servers" -> brokerList,
    "group.id" -> groupId,
    "subscribe" -> topic,
    "startingOffsets" -> startOffset)

  def execute {
    import spark.implicits._

    val visitlog = spark.readStream.format("kafka").options(kafkaProps).load
      .selectExpr("CAST(value AS STRING)")
      .select(
        get_json_object($"value", "$.aid").as("aid"),
        get_json_object($"value", "$.channel_code").as("channel_code"),
        get_json_object($"value", "$.area").as("area"),
        get_json_object($"value", "$.ip").as("ip"),
        get_json_object($"value", "$.create_time").as("create_time"))
      .withWatermark("create_time", "10 MINUTES")
      .groupBy(window(col("create_time"), "10 MINUTES", "5 MINUTES"), col("channel_code"))
      .count

    visitlog.writeStream.format("console").outputMode(OutputMode.Complete).start()

    spark.streams.awaitAnyTermination()
  }

}