package com.jiuzhi.etl.fad

import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.fad.model.Active2

/**
 * 解析访问日志得到fact_active
 */
class FactActive2(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val visitLogPath = rootPath + task.prevDate + "/" + topic
  val newVisitLogPath = rootPath + task.theDate + "/" + topic

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  def execute {
    // 读取hdfs json文件
    val visitlog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg", "DATE_FORMAT(createtime, 'yyyy-MM-dd') AS active_date", "logtype", "appVersion", "CAST(cityId AS LONG)", "country", "createtime")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPath, newVisitLogPath)
        .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > ''")
        .selectExpr("udid", "apppkg", "DATE_FORMAT(createtime, 'yyyy-MM-dd') AS active_date", "CAST(logtype AS INT)", "appversion", "CAST(cityid AS LONG)", "country", "CAST(createtime AS TIMESTAMP)")
    }
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    import spark.implicits._

    // 分析
    val active = visitlog.map(Active2(_)).rdd
      .groupBy(row => row.udid + row.app_key + row.active_date + row.log_type)
      .map {
        _._2.toSeq.sortBy(_.create_time.getTime)
          .reduceLeft { (acc, curr) => Active2.update(acc, curr) }
      }
      .toDF()
      .drop("create_time")
    if (log.isDebugEnabled) {
      active.printSchema
      active.show(50, false)
    }

    // 读取fact_client
    val client = spark.read.jdbc(adDb.jdbcUrl, "fact_client", adDb.connProps)
      .selectExpr("udid", "app_key", "clnt", "init_version", "create_date", "DATE_FORMAT(create_time, 'yyyy-MM-dd') AS _create_date")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
    }

    // 关联fact_client获取init_version clnt create_date等信息
    val result = active.join(client, Seq("udid", "app_key"))
      .selectExpr("udid", "app_key", "CAST(DATE_FORMAT(active_date, 'yyyyMMdd') AS INT) AS active_date", "log_type", "version",
        "city_id", "country", "visit_times", "clnt", "init_version", "create_date", "DATEDIFF(active_date, _create_date) AS date_diff")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, s"DELETE FROM fact_active WHERE active_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_active", adDb.connProps)
  }
}