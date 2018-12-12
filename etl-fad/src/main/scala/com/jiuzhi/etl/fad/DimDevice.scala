package com.jiuzhi.etl.fad

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode

import org.zc.sched.model.Task
import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.fad.model.Device

/**
 * 解析访问日志得到dim_device
 */
class DimDevice(task: Task) extends TaskExecutor(task) with Serializable {

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
      JdbcUtil.executeUpdate(adDb, "TRUNCATE TABLE dim_device")
      JdbcUtil.executeUpdate(adDb, s"INSERT INTO dim_device SELECT * FROM dim_device_${task.statDate}")
    }

    // 读取hdfs json文件
    val visitlog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_device_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "deviceid", "imsi", "imei", "vender",
          "model", "osVersion", "platform", "androidid", "operator",
          "CASE WHEN network = 'GPRS' THEN '1GPRS' WHEN network = 'Wifi' THEN '0WIFI' WHEN network = 'Unknown' THEN '-Unknown' ELSE network END",
          "src", "mac", "apppkg", "clnt", "CAST(isRoot AS INT)",
          "CAST(gp AS INT)", "gaid", "rom", "lang", "ua",
          "cityId", "country", "createTime", "updateTime")
    } else {
      spark.read.option("allowUnquotedFieldNames", true).json(visitLogPath, newVisitLogPath)
        .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > '' AND LENGTH(udid) <= 64 AND LENGTH(clnt) <= 50")
        .na.fill(Map("gp" -> 2))
        .selectExpr("udid", "deviceid", "imsi", "imei", "vender",
          "model", "osVersion", "platform", "androidid", "operator",
          "CASE WHEN network = 'GPRS' THEN '1GPRS' WHEN network = 'Wifi' THEN '0WIFI' WHEN network = 'Unknown' THEN '-Unknown' WHEN network > '' THEN SUBSTR(network, 0, 8) ELSE '-Unknown' END",
          "src", "mac", "apppkg", "clnt", "CASE WHEN isroot = '1' THEN 1 ELSE 2 END",
          "CASE WHEN gp = '1' THEN 1 ELSE 2 END", "gaid", "CAST(rom AS LONG)", "lang", "SUBSTR(ua, 0, 200)",
          "CAST(cityid AS LONG)", "country", "CAST(createtime AS TIMESTAMP) create_time", "CAST(updatetime AS TIMESTAMP) update_time")
    }
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取dim_device
    val device = spark.read.jdbc(adDb.jdbcUrl, "dim_device", adDb.connProps)
      .selectExpr("udid", "deviceid", "imsi", "imei", "vender",
        "model", "os_version", "platform", "android_id", "operator",
        "CASE WHEN network = 'GPRS' THEN '1GPRS' WHEN network = 'Wifi' THEN '0WIFI' WHEN network = 'Unknown' THEN '-Unknown' ELSE network END",
        "src", "mac", "app_key", "clnt", "is_root",
        "has_gplay", "gaid", "rom", "lang", "ua",
        "city_id", "country", "create_time", "update_time")
    if (log.isDebugEnabled) {
      device.printSchema
      device.show(50, false)
    }

    import spark.implicits._

    // 合并
    val result = visitlog.union(device)
      .map(Device(_)).rdd
      .groupBy(_.udid)
      .map { row => (row._1, row._2.toSeq.sortBy(_.update_time.getTime)) }
      .map(_._2.reduceLeft { (acc, curr) => Device.update(acc, curr) })
      .map(Device.finalize(_))
      .toDF()
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
      result.write.option("delimiter", "\t").csv(s"/tmp/${task.theDate}/${task.taskId}/dim_device")
    }

    // 写入临时表
    val tmpTable = "tmp_dim_device_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(adDb, s"CREATE TABLE ${tmpTable} LIKE dim_device")
    try {
      result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, tmpTable, adDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份dim_device
    try {
      JdbcUtil.executeUpdate(adDb, s"RENAME TABLE dim_device TO dim_device_${task.statDate}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table dim_device_${task.statDate} already exists")
        JdbcUtil.executeUpdate(adDb, "DROP TABLE dim_device")
    }

    // 更新dim_device
    JdbcUtil.executeUpdate(adDb, s"RENAME TABLE ${tmpTable} TO dim_device")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-3, task.theTime))
    JdbcUtil.executeUpdate(adDb, s"DROP TABLE IF EXISTS dim_device_${prevDate}")
  }
}