package com.jiuzhi.etl.ad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil
import org.zc.sched.constant.DBConstant

/**
 * 生成活跃用户表聚合表
 */
class AggActive(task: Task) extends TaskExecutor(task) with Serializable {

  // 产品编码
  val productCode = task.taskExt.get("product_code").get

  // 广告数据库
  val dbAd = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
  // 活跃用户表
  val tableActive = task.taskExt.getOrElse("tbl_active", s"fact_active_${productCode}")

  // 聚合规则
  // 格式(每行一条规则): 规则编码 分组字段列表
  // 例如: l_001 字段1,字段2,字段n
  val aggRules = task.taskExt.get("agg_rules")
  // 聚合列(没有指定聚合规则时,根据聚合列生成聚合规则)
  val aggColumns = task.taskExt.get("agg_columns")
  // 必须出现的列
  val mustColumns = task.taskExt.get("must_columns")
  // 聚合表前缀
  val aggPrefix = task.taskExt.getOrElse("agg_prefix", s"agg_active_${productCode}_")

  val keyColumn = task.taskExt.getOrElse("key_column", "id")
  val factCount = task.taskExt.getOrElse("fact_count", "fact_count")

  // 创建表模式
  val createMode = task.taskExt.getOrElse("create_mode", DBConstant.CREATE_MODE_AUTO)
  // 创建表语句
  val createSqls = Seq(
    s"""CREATE TABLE IF NOT EXISTS ${aggPrefix}l_1 (
      active_date INT,
      create_date INT,
      date_diff INT,
      fact_count INT,
      PRIMARY KEY (active_date, create_date)
    ) ENGINE=MyISAM""",
    s"""CREATE TABLE IF NOT EXISTS ${aggPrefix}l_2 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (active_date, create_date, channel_code)
    ) ENGINE=MyISAM""",
    s"""CREATE TABLE IF NOT EXISTS ${aggPrefix}l_3 (
      active_date INT,
      create_date INT,
      date_diff INT,
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (active_date, create_date, area)
    ) ENGINE=MyISAM""",
    s"""CREATE TABLE IF NOT EXISTS ${aggPrefix}l_4 (
      active_date INT,
      create_date INT,
      date_diff INT,
      channel_code VARCHAR(50),
      area VARCHAR(50),
      fact_count INT,
      PRIMARY KEY (active_date, create_date, channel_code, area)
    ) ENGINE=MyISAM""")

  def execute {
    // 解析参数得到聚合规则
    // 格式为Array(规则编码,分组字段列表)
    val rules = if (aggRules.isDefined) {
      log.info("parse aggregation rules")
      aggRules.get.split("\r\n").map(x => {
        val arr = x.split(" ")
        (arr(0), arr(1).split(","))
      })
    } else {
      log.info("generate aggregation rules from aggregation columns")
      val columns = aggColumns.get.split(",")
      val must = if (mustColumns.isDefined) mustColumns.get.split(",") else Array[String]()
      val size = columns.size
      val length = size.toString.length
      val combines = for (i <- 2 to size) yield columns.combinations(i).map(_.++:(must))
      // 联合所有规则
      Array(must).union(columns.map(Array(_).++:(must))).union(combines.flatten)
        .dropWhile(_.isEmpty)
        .zip(Stream from 1).map { x =>
          ("l_" + s"%0${length}d".format(x._2), x._1)
        }
    }

    // 逐条规则聚合
    rules.foreach(rule => {
      // 聚合规则编码
      val code = rule._1
      // 分组字段
      val columns = rule._2
      // 聚合表名
      val aggTable = aggPrefix + code
      log.info(s"aggregation rule: { ${code}\t" + columns.mkString("", ",", " }"))
      val activeTable = if (task.isFirst) {
        log.info("aggregate all data for the first time")
        spark.read.jdbc(dbAd.jdbcUrl, tableActive, dbAd.connProps)
      } else {
        log.info("aggregate incremental data")
        spark.read.jdbc(dbAd.jdbcUrl, tableActive, Array(s"active_date = ${task.statDate}"), dbAd.connProps)
      }

      // 聚合
      var result = activeTable.groupBy(columns(0), columns.drop(1): _*)
        .agg(count(keyColumn).alias(factCount))
      log.info(s"table: ${aggTable}, schema: ${result.schema.simpleString}")

      // 删除表
      if (createMode.equals(DBConstant.CREATE_MODE_DROP)) {
        JdbcUtil.executeUpdate(dbAd, s"DROP TABLE IF EXISTS ${aggTable}")
      }
      // 创建表
      if (Seq(DBConstant.CREATE_MODE_AUTO, DBConstant.CREATE_MODE_DROP).contains(createMode)) {
        val createSql = createSqls.find(_.contains(aggTable))
        JdbcUtil.executeUpdate(dbAd, createSql.get)
      }

      // 删除已经存在的数据
      val sql = if (task.isFirst) {
        s"TRUNCATE TABLE ${aggTable}"
      } else {
        s"DELETE FROM ${aggTable} WHERE active_date = ${task.statDate}"
      }
      log.info(s"delete existing data: ${sql}")
      JdbcUtil.executeUpdate(dbAd, sql)

      // 写入数据库
      result.write.mode(SaveMode.Append).jdbc(dbAd.jdbcUrl, aggTable, dbAd.connProps)
    })
  }

}