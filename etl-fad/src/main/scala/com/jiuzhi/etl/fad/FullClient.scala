package com.jiuzhi.etl.fad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.fad.model.Client2

/**
 * 解析访问日志得到full_client
 * 全量处理
 */
class FullClient(task: Task) extends TaskExecutor(task) with Serializable {

  // 日期范围
  val startDate = DateUtil.getDate(task.taskExt.getOrElse("start_date", "2017-04-01"))
  val endDate = DateUtil.getDate(task.taskExt.getOrElse("end_date", task.prevDate))

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val visitLogPaths = DateUtils.genDate(startDate, endDate).map(rootPath + DateUtil.formatDate(_) + "/" + topic)

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  val factTable = "full_client"
  val bakTable = s"full_client_${task.statDate}"

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(adDb, s"TRUNCATE TABLE ${factTable}")
      JdbcUtil.executeUpdate(adDb, s"INSERT INTO ${factTable} SELECT * FROM ${bakTable}")
    }

    // 读取hdfs json文件
    val visitlog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg", "clnt", "appVersion", "appVersion init_version",
          "CAST(path AS INT)", "createTime", "updateTime", "CAST(DATE_FORMAT(createTime, 'yyyyMMdd') AS INT)")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPaths: _*)
        .where("udid > '' AND apppkg > '' AND clnt > '' AND appversion > '' AND LENGTH(clnt) <= 50")
        .selectExpr("udid", "apppkg", "clnt", "appversion", "appversion init_version",
          "CAST(path AS INT)", "CAST(createtime AS TIMESTAMP) create_time", "CAST(updatetime AS TIMESTAMP) update_time", "CAST(DATE_FORMAT(createtime, 'yyyyMMdd') AS INT)")
    }
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取${factTable}
    val client = spark.read.jdbc(adDb.jdbcUrl, factTable, adDb.connProps)
      .select("udid", "app_key", "clnt", "version", "init_version", "app_path", "create_time", "update_time", "create_date")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
    }

    import spark.implicits._

    // 合并
    val result = visitlog.union(client)
      .map(Client2(_)).rdd
      .groupBy(row => row.udid + row.app_key)
      .map {
        _._2.toSeq.sortBy(_.update_time.getTime)
          .reduceLeft { (acc, curr) => Client2.update(acc, curr) }
      }
      .toDF()
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 写入临时表
    val tmpTable = s"tmp_${factTable}_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(adDb, s"CREATE TABLE ${tmpTable} LIKE ${factTable}")
    try {
      result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, tmpTable, adDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份${factTable}
    try {
      JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${factTable} TO ${bakTable}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table ${bakTable} already exists")
        JdbcUtil.executeUpdate(adDb, s"DROP TABLE ${factTable}")
    }

    // 更新${factTable}
    JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${tmpTable} TO ${factTable}")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-2, task.theTime))
    JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS ${factTable}_${prevDate}")
  }
}