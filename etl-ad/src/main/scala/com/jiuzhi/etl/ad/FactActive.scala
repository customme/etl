package com.jiuzhi.etl.ad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.ad.model.New
import com.jiuzhi.etl.ad.model.Active

/**
 * 解析hdfs上的访问日志(json格式),得到活跃用户并写入mysql表
 */
class FactActive(task: Task) extends TaskExecutor(task) with Serializable {

  // 访问日志hdfs根目录
  val hdfsDir = task.taskExt.get("hdfs_dir").get
  // 产品编码
  val productCode = task.taskExt.get("product_code").get
  // 开始日期
  val startDate = task.runParams.getOrElse("start_date", task.prevDate)
  // 结束日期
  val endDate = task.runParams.getOrElse("end_date", startDate)
  // 访问日志目录
  val visitLogDirs = DateUtils.genDate(DateUtil.getDate(startDate), DateUtil.getDate(endDate)).map {
    hdfsDir + productCode + "/" + DateUtil.formatDate(_)
  }

  // 广告数据库
  val dbAd = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  // 新增用户表
  val tableNew = task.taskExt.getOrElse("tbl_new", "fact_new_" + productCode)
  // 活跃用户表
  val tableActive = task.taskExt.getOrElse("tbl_active", "fact_active_" + productCode)

  def execute {
    // 读取 hdfs上的访问日志
    val visitlog = spark.read.json(visitLogDirs: _*)
      .selectExpr("aid", "area", "DATE_FORMAT(create_time, 'yyyy-MM-dd') AS active_date", "CAST(create_time AS TIMESTAMP)")

    // 日期范围
    val range = visitlog.selectExpr("DATE_FORMAT(MIN(active_date), 'yyyyMMdd')", "DATE_FORMAT(MAX(active_date), 'yyyyMMdd')").first
    val minDate = range.getString(0)
    val maxDate = range.getString(1)
    if (log.isDebugEnabled) {
      log.debug("{ minDate -> " + minDate + ", maxDate -> " + maxDate + " }")
    }

    // 读取活跃用户表
    val activeTable = spark.read.jdbc(dbAd.jdbcUrl, tableActive, dbAd.connProps)
      .selectExpr("aid", "area", "active_date", "CAST(UNIX_TIMESTAMP(CAST(active_date AS VARCHAR(10)), 'yyyyMMdd') AS TIMESTAMP)")
      .where(s"active_date >= ${minDate} AND active_date <= ${maxDate}")

    import spark.implicits._

    // 合并
    val active = visitlog.union(activeTable).na.drop(Seq("aid", "create_time"))
      .map(Active(_)).rdd
      .groupBy(row => row.aid + row.active_date)
      .map {
        _._2.toSeq.sortBy(_.create_time.getTime)
          .reduceLeft { (acc, curr) => Active.update(acc, curr) }
      }
      .toDF()
      .drop("create_time")

    // 读取新增用户表
    val newTable = spark.read.jdbc(dbAd.jdbcUrl, tableNew, dbAd.connProps)
      .selectExpr("aid", "channel_code", "create_date", "DATE_FORMAT(create_time, 'yyyy-MM-dd') AS _create_date")

    // 关联新增用户表获取channel_code,create_date
    val result = broadcast(active).join(newTable, Seq("aid"))
      .selectExpr("aid", "channel_code", "area", "CAST(DATE_FORMAT(active_date, 'yyyyMMdd') AS INT) AS active_date", "create_date",
        "DATEDIFF(active_date, _create_date) AS date_diff", "visit_times")
      .coalesce(parallelism)

    // 删除已经存在的数据
    JdbcUtil.executeUpdate(dbAd, s"DELETE FROM ${tableActive} WHERE active_date >= ${minDate} AND active_date <= ${maxDate}")

    // 入库
    result.write.mode(SaveMode.Append).jdbc(dbAd.jdbcUrl, tableActive, dbAd.connProps)
  }

}