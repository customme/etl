package com.jiuzhi.etl.nad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.nad.model.New

class FactNew(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log hdfs 目录
  val rootDir = task.taskExt.get("root_dir").get
  val topic = task.taskExt.get("topic").get
  val visitLogPath = rootDir + topic + "/" + task.prevDate

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  // 事实表
  val factTable = task.taskExt.getOrElse("fact_table", "fact_new_" + topic)

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(adDb, s"TRUNCATE TABLE ${factTable}")
      JdbcUtil.executeUpdate(adDb, s"INSERT INTO ${factTable} SELECT * FROM ${factTable}_${task.statDate}")
    }

    // 读取 hdfs json 文件
    val visitlog = spark.read.json(visitLogPath)
      .selectExpr("aid", "cuscode", "city init_city", "city", "ip init_ip",
        "ip", "CAST(create_time AS TIMESTAMP)", "CAST(create_time AS TIMESTAMP) update_time", "CAST(DATE_FORMAT(create_time, 'yyyyMMdd') AS INT)")

    // 读取数据库表
    val factNew = spark.read.jdbc(adDb.jdbcUrl, factTable, adDb.connProps)
      .select("aid", "cuscode", "init_city", "city", "init_ip", "ip", "create_time", "update_time", "create_date")

    import spark.implicits._

    // 合并
    val result = visitlog.union(factNew)
      .map(New(_)).rdd
      .groupBy(_.aid)
      .map { row => (row._1, row._2.toSeq.sortBy(_.update_time.getTime)) }
      .map(_._2.reduceLeft { (acc, curr) => New.update(acc, curr) })
      .toDF()
      .coalesce(parallelism)

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

    // 备份
    try {
      JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${factTable} TO ${factTable}_${task.statDate}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table ${factTable}_${task.statDate} already exists")
        JdbcUtil.executeUpdate(adDb, "DROP TABLE ${factTable}")
    }

    // 更新
    JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${tmpTable} TO ${factTable}")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-3, task.theTime))
    JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS ${factTable}_${prevDate}")
  }

}