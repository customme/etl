package com.jiuzhi.etl.fad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 开放数据
 */
class OpenData(task: Task) extends TaskExecutor(task) with Serializable {

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
  // 客户数据库
  val cusDb = getDbConn(task.taskExt.getOrElse("cus_db_id", 0).toString.toInt)

  // 其他目标数据库
  val tarDbs = task.taskExt.getOrElse("tar_db_ids", "").split(",").map(x => getDbConn(x.toInt).get)

  // 包含渠道
  val includeChannels = task.taskExt.getOrElse("include_channels", "")
  // 排除渠道
  val excludeChannels = task.taskExt.getOrElse("exclude_channels", "")

  // 测试
  val debug = task.taskExt.getOrElse("debug", 0).toString.toInt

  def execute {
    val result = if (task.isFirst) {
      spark.read.jdbc(adDb.jdbcUrl, "fact_client", Array("app_key > '' AND clnt > ''"), adDb.connProps)
        .groupBy("create_date", "clnt")
        .agg(countDistinct("udid").alias("csum"))
        .selectExpr("CONCAT(SUBSTR(create_date, 0, 4), '-', SUBSTR(create_date, 5, 2), '-', SUBSTR(create_date, 7, 2)) AS cdate", "clnt AS chacode", "csum")
        .coalesce(parallelism)
    } else {
      spark.read.jdbc(adDb.jdbcUrl, "fact_client", Array(s"create_date = '${task.statDate}' AND app_key > '' AND clnt > ''"), adDb.connProps)
        .groupBy("clnt")
        .agg(countDistinct("udid").alias("csum"))
        .selectExpr("clnt AS chacode", "csum")
        .withColumn("cdate", lit(s"${task.prevDate}"))
        .coalesce(parallelism)
    }
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    if (debug == 0) {
      if (cusDb.isDefined) {
        val db = cusDb.get
        JdbcUtil.executeUpdate(db, s"DELETE FROM t_channeldaily WHERE cdate = '${task.prevDate}'")
        result.write.mode(SaveMode.Append).jdbc(db.jdbcUrl, "t_channeldaily", db.connProps)
      }
    } else {
      result.show(10000, false)
    }

    // 过滤渠道
    val result1 = if (includeChannels != "") {
      val includes = includeChannels.split(",").mkString("'", "','", "'")
      result.where(s"clnt IN (${includes})")
    } else if (excludeChannels != "") {
      val excludes = excludeChannels.split(",").mkString("'", "','", "'")
      result.where(s"clnt NOT IN (${excludes})")
    } else {
      result
    }

    // 入库
    if (debug == 0) {
      result1.cache
      tarDbs.foreach { db =>
        val filter = if (includeChannels != "") {
          val includes = includeChannels.split(",").mkString("'", "','", "'")
          s"AND chacode IN (${includes})"
        } else if (excludeChannels != "") {
          val excludes = excludeChannels.split(",").mkString("'", "','", "'")
          s"AND chacode NOT IN (${excludes})"
        } else { "" }
        JdbcUtil.executeUpdate(db, s"DELETE FROM t_channeldaily WHERE cdate = '${task.prevDate}' ${filter}")
        result1.write.mode(SaveMode.Append).jdbc(db.jdbcUrl, "t_channeldaily", db.connProps)
      }
    } else {
      result1.show(10000, false)
    }
  }
}