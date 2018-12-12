package com.jiuzhi.etl.fad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 解析访问日志得到运营级别信息fact_runlevel
 */
class FactRunlevel(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val visitLogPath = rootPath + task.prevDate + "/" + topic
  val newVisitLogPath = rootPath + task.theDate + "/" + topic

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  def execute {
    // 读取hdfs json文件
    val visitLog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg AS app_key", "clnt", "runlevel", "CAST(DATE_FORMAT(createTime, 'yyyyMMdd') AS INT) AS create_date")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPath, newVisitLogPath)
        .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > '' AND LENGTH(clnt) <= 50")
        .selectExpr("udid", "apppkg AS app_key", "clnt", "runlevel", "CAST(DATE_FORMAT(createtime, 'yyyyMMdd') AS INT) AS create_date")
    }
    if (log.isDebugEnabled) {
      visitLog.printSchema
      visitLog.show(50, false)
    }

    val result = visitLog.groupBy("app_key", "clnt", "runlevel", "create_date")
      .agg(countDistinct("udid").alias("user_count"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, s"DELETE FROM fact_runlevel WHERE create_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_runlevel", adDb.connProps)
  }
}