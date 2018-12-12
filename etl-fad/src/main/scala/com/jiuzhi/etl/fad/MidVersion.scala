package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

/**
 * 解析访问日志得到用户使用App版本信息mid_version
 */
class MidVersion(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val visitLogPath = rootPath + task.prevDate + "/" + topic
  val newVisitLogPath = rootPath + task.theDate + "/" + topic

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取hdfs json文件
    val visitlog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg", "appVersion", "UNIX_TIMESTAMP(createTime)")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPath, newVisitLogPath)
        .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > '' AND LENGTH(appversion) <= 50")
        .selectExpr("udid", "apppkg", "appversion", "UNIX_TIMESTAMP(createtime)")
    }
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取mid_version
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-2, task.theTime))
    spark.sql("USE " + dbName)
    val version = spark.sql(s"SELECT udid, app_key, version, UNIX_TIMESTAMP(create_time) AS ctimestamp FROM mid_version WHERE stat_date = '${prevDate}'")

    // 联合
    val result = version.union(visitlog)
      .groupBy("udid", "app_key", "version")
      .agg(min("ctimestamp").alias("mtimestamp"))
      .selectExpr("udid", "app_key", "version", "FROM_UNIXTIME(mtimestamp)")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    result.createOrReplaceTempView("tmp_mid_version")
    spark.sql(s"ALTER TABLE mid_version DROP IF EXISTS PARTITION(stat_date = '${task.statDate}') PURGE")
    spark.sql(s"INSERT INTO mid_version PARTITION(stat_date = '${task.statDate}') SELECT * FROM tmp_mid_version")

    // 删除历史分区
    val oldDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-4, task.theTime))
    spark.sql(s"ALTER TABLE mid_version DROP IF EXISTS PARTITION(stat_date = '${oldDate}') PURGE")

    // 更新dim_version
    JdbcUtil.executeUpdate(adDb, "TRUNCATE TABLE dim_version")
    result.selectExpr("version AS id")
      .distinct
      .coalesce(1)
      .write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "dim_version", adDb.connProps)
  }
}