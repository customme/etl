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
import org.zc.sched.constant.DBConstant

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
    s"${hdfsDir}/${productCode}/" + DateUtil.formatDate(_)
  }

  // 广告数据库
  val dbAd = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
  // 新增用户表
  val tableNew = task.taskExt.getOrElse("tbl_new", s"fact_new_${productCode}")
  // 活跃用户表
  val tableActive = task.taskExt.getOrElse("tbl_active", s"fact_active_${productCode}")

  // 创建表模式
  val createMode = task.taskExt.getOrElse("create_mode", DBConstant.CREATE_MODE_AUTO)
  // 创建表语句
  val createSql = task.taskExt.getOrElse("create_sql", s"""
    CREATE TABLE IF NOT EXISTS ${tableActive} (
      aid VARCHAR(64),
      channel_code VARCHAR(32),
      area VARCHAR(16),
      active_date INT,
      create_date INT,
      date_diff INT,
      visit_times INT,
      PRIMARY KEY (aid, active_date),
      KEY idx_channel_code (channel_code),
      KEY idx_area (area),
      KEY idx_active_date (active_date),
      KEY idx_create_date (create_date),
      KEY idx_date_diff (date_diff),
      KEY idx_visit_times (visit_times)
    ) ENGINE=InnoDB COMMENT='活跃用户'
    """)

  def execute {
    // 删除表
    if (createMode.equals(DBConstant.CREATE_MODE_DROP)) {
      JdbcUtil.executeUpdate(dbAd, s"DROP TABLE IF EXISTS ${tableActive}")
    }
    // 创建表
    if (Seq(DBConstant.CREATE_MODE_AUTO, DBConstant.CREATE_MODE_DROP).contains(createMode)) {
      JdbcUtil.executeUpdate(dbAd, createSql)
    }

    // 读取 hdfs上的访问日志
    val visitlog = spark.read.json(visitLogDirs: _*)
      .selectExpr("aid", "area", "DATE_FORMAT(create_time, 'yyyy-MM-dd') AS active_date", "CAST(create_time AS TIMESTAMP)")

    // 获取访问日志日期范围
    val range = visitlog.selectExpr("DATE_FORMAT(MIN(active_date), 'yyyyMMdd')", "DATE_FORMAT(MAX(active_date), 'yyyyMMdd')").first
    val minDate = range.getString(0)
    val maxDate = range.getString(1)
    if (log.isDebugEnabled) {
      log.debug(s"{ minDate -> ${minDate}, maxDate -> ${maxDate} }")
    }

    // 读取活跃用户表
    val activeTable = spark.read.jdbc(dbAd.jdbcUrl, tableActive, dbAd.connProps)
      .selectExpr("aid", "area", "active_date", "CAST(UNIX_TIMESTAMP(CAST(active_date AS VARCHAR(10)), 'yyyyMMdd') AS TIMESTAMP)")
      .where(s"active_date >= ${minDate} AND active_date <= ${maxDate}")

    import spark.implicits._

    // 合并访问日志和活跃用户
    // 按访问时间排序后逐条对比更新
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

    // 入库活跃用户
    result.write.mode(SaveMode.Append).jdbc(dbAd.jdbcUrl, tableActive, dbAd.connProps)
  }

}