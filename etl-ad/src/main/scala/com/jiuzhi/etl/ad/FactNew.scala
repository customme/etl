package com.jiuzhi.etl.ad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.ad.model.New

/**
 * 解析hdfs上的访问日志(json格式),得到新增用户并写入mysql表
 */
class FactNew(task: Task) extends TaskExecutor(task) with Serializable {

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
    hdfsDir + "/" + productCode + "/" + DateUtil.formatDate(_)
  }

  // 广告数据库
  val dbAd = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  // 新增用户表
  val tableNew = task.taskExt.getOrElse("tbl_new", "fact_new_" + productCode)
  // 新增用户表前一天备份表
  val prevTable = tableNew + "_" + task.statDate

  // 新增用户表备份保留个数
  val bakCount = task.taskExt.getOrElse("bak_count", 3).toString.toInt

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(dbAd, s"TRUNCATE TABLE ${tableNew}")
      JdbcUtil.executeUpdate(dbAd, s"CREATE TABLE IF NOT EXISTS ${prevTable} LIKE ${tableNew}")
      JdbcUtil.executeUpdate(dbAd, s"INSERT INTO ${tableNew} SELECT * FROM ${prevTable}")
    }

    // 读取 hdfs上的访问日志
    val visitlog = spark.read.json(visitLogDirs: _*)
      .selectExpr("aid", "channel_code", "area init_area", "area", "ip init_ip", "ip",
        "CAST(create_time AS TIMESTAMP)", "CAST(create_time AS TIMESTAMP) update_time")

    // 读取新增用户表
    val newTable = spark.read.jdbc(dbAd.jdbcUrl, tableNew, dbAd.connProps)
      .select("aid", "channel_code", "init_area", "area", "init_ip", "ip", "create_time", "update_time")

    import spark.implicits._

    // 合并访问日志和新增用户
    // 按更新时间排序后逐条对比更新
    val result = newTable.union(visitlog).na.drop(Seq("aid", "channel_code", "create_time"))
      .map(New(_)).rdd
      .groupBy(_.aid)
      .map { row => (row._1, row._2.toSeq.sortBy(_.update_time.getTime)) }
      .map(_._2.reduceLeft { (acc, curr) => New.update(acc, curr) })
      .toDF()
      .withColumn("create_date", date_format(col("create_time"), "yyyyMMdd").cast(IntegerType))
      .coalesce(parallelism)

    // 写入临时表
    val tmpTable = s"tmp_${tableNew}_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(dbAd, s"CREATE TABLE ${tmpTable} LIKE ${tableNew}")
    try {
      result.write.mode(SaveMode.Append).jdbc(dbAd.jdbcUrl, tmpTable, dbAd.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(dbAd, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份当前新增用户表
    JdbcUtil.executeUpdate(dbAd, s"DROP TABLE IF EXISTS ${prevTable}")
    JdbcUtil.executeUpdate(dbAd, s"RENAME TABLE ${tableNew} TO ${prevTable}")

    // 重命名临时表得到最新新增用户表
    JdbcUtil.executeUpdate(dbAd, s"RENAME TABLE ${tmpTable} TO ${tableNew}")

    // 删除历史备份表
    val hisDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-bakCount, task.theTime))
    JdbcUtil.executeUpdate(dbAd, s"DROP TABLE IF EXISTS ${tableNew}_${hisDate}")
  }

}