package com.jiuzhi.etl.fad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.fad.model.Client

/**
 * 解析访问日志得到fact_client
 */
class FactClient(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val visitLogPath = rootPath + task.prevDate + "/" + topic
  val newVisitLogPath = rootPath + task.theDate + "/" + topic

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(adDb, "TRUNCATE TABLE fact_client")
      JdbcUtil.executeUpdate(adDb, s"INSERT INTO fact_client SELECT * FROM fact_client_${task.statDate}")
    }

    // 读取hdfs json文件
    val visitlog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg", "clnt", "appVersion", "appVersion init_version",
          "sdkver", "sdkver init_sdkver", "CAST(path AS INT)", "createTime", "updateTime", "CAST(DATE_FORMAT(createTime, 'yyyyMMdd') AS INT)")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPath, newVisitLogPath)
        .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > '' AND LENGTH(udid) <= 64 AND LENGTH(appversion) <= 50 AND LENGTH(clnt) <= 50")
        .selectExpr("udid", "apppkg", "clnt", "appversion", "appversion init_version",
          "SUBSTR(sdkver, 0, 50)", "SUBSTR(sdkver, 0, 50) init_sdkver", "CASE WHEN path IN ('1', '2', '3', '4', '5') THEN CAST(path AS INT) ELSE 0 END", "CAST(createtime AS TIMESTAMP) create_time", "CAST(updatetime AS TIMESTAMP) update_time",
          "CAST(DATE_FORMAT(createtime, 'yyyyMMdd') AS INT)")
    }
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取fact_client
    val client = spark.read.jdbc(adDb.jdbcUrl, "fact_client", adDb.connProps)
      .select("udid", "app_key", "clnt", "version", "init_version", "sdkver", "init_sdkver", "app_path", "create_time", "update_time", "create_date")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
      client.write.option("delimiter", "\t").csv(s"/tmp/${task.theDate}/${task.taskId}/fact_client-1")
    }

    import spark.implicits._

    // 合并
    val result = visitlog.union(client)
      .map(Client(_)).rdd
      .groupBy(row => row.udid + row.app_key)
      .map { row => (row._1, row._2.toSeq.sortBy(_.update_time.getTime)) }
      .map(_._2.reduceLeft { (acc, curr) => Client.update(acc, curr) })
      .toDF()
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
      result.write.option("delimiter", "\t").csv(s"/tmp/${task.theDate}/${task.taskId}/fact_client-2")
    }

    // 写入临时表
    val tmpTable = "tmp_fact_client_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(adDb, s"CREATE TABLE ${tmpTable} LIKE fact_client")
    try {
      result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, tmpTable, adDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份fact_client
    try {
      JdbcUtil.executeUpdate(adDb, s"RENAME TABLE fact_client TO fact_client_${task.statDate}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table fact_client_${task.statDate} already exists")
        JdbcUtil.executeUpdate(adDb, "DROP TABLE fact_client")
    }

    // 更新fact_client
    JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${tmpTable} TO fact_client")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-3, task.theTime))
    JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS fact_client_${prevDate}")
  }
}