package com.jiuzhi.etl.recommend

import java.util.Properties
import java.math.RoundingMode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import org.apache.spark.sql.SparkSession

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.recommend.util.HBaseUtil
import com.jiuzhi.etl.recommend.util.SimiUtil
import org.zc.sched.constant.TaskConstant

/**
 * 用户兴趣模型计算
 */
class UserProfile(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // MyCat limit
  val mycatLimit = task.taskExt.getOrElse("mycat_limit", 100000000).toString().toInt

  // 用户标签个数
  val userTagNum = task.taskExt.getOrElse("user_tag_num", 50).toString().toInt
  // 用户权重标签个数
  val userWtagNum = task.taskExt.getOrElse("user_wtag_num", 30).toString().toInt

  // 标签相关度精度
  val relevancyScale = task.taskExt.getOrElse("relevancy_scale", 5).toString().toInt
  // 标签权重精度
  val weightScale = task.taskExt.getOrElse("weight_scale", 2).toString().toInt

  // 单个用户点击某个资讯最多算几次
  val maxUserClicks = task.taskExt.getOrElse("max_user_clicks", 3).toString().toInt

  // 事件权重（格式：recreate_click: 1.0, comments: 0.5, share: 0.8）
  val eventWeight = task.taskExt.get("event_weight").get.split(",").map { tag =>
    val arr = tag.split(":")
    (arr(0).trim(), arr(1).trim().toDouble)
  }.toMap

  // 计算多少天的用户行为数据
  val intervalDay = task.taskExt.getOrElse("interval_day", 100).toString().toInt

  // 资讯发布时间开始日期
  val infoStartDate = DateUtil.getDate(task.taskExt.getOrElse("info_start_date", "2015-05-01").toString())
  // 资讯数据按多少天分一个区
  val partitionDay = task.taskExt.getOrElse("partition_day", 30).toString().toInt

  // 资讯数据库
  val infoDB = getDbConn(task.taskExt.get("info_db_id").get.toInt).get

  // 资讯表
  val infoTable = task.taskExt.getOrElse("info_table", "t_information")

  // HBase Zookeeper Quorum
  val hBaseZKURL = task.taskExt.get("hbase_zk_url").get

  def execute {
    import spark.implicits._

    //  获取用户行为，计算用户跟资讯相关度
    val startDate = DateUtil.formatDate("yyyy-MM-dd", DateUtil.nextDate(-intervalDay, task.theTime))
    val endDate = if (TaskConstant.TASK_CYCLE_HOUR.equalsIgnoreCase(task.taskCycle)) task.theDate else task.prevDate
    var sql = s"SELECT b.device_id, b.event_name, b.info_id, DATEDIFF('${task.theDate}', b.click_date) date_diff, LEAST(b.click_count, ${maxUserClicks}), a.user_count" +
      s" FROM (SELECT * FROM d_info_click WHERE click_date >= '${startDate}' AND click_date <= '${endDate}') a" +
      s" JOIN (SELECT * FROM d_user_click WHERE click_date >= '${startDate}' AND click_date <= '${endDate}') b" +
      " ON a.click_date = b.click_date" +
      " AND a.info_id = b.info_id"
    log.debug(sql)
    spark.sql("USE recommender")
    spark.sql(sql).map { row =>
      val params = Map[String, Double](
        "event_name" -> eventWeight.get(row.getString(1)).get,
        "date_diff" -> Math.max(row.getInt(3), 0.5),
        "user_count" -> row.getInt(4),
        "total_count" -> row.getInt(5))
      (row.getString(0) + "-" + row.getLong(2), SimiUtil.rating(params))
    }.rdd.reduceByKey(_ + _).map { obj =>
      val arr = obj._1.split("-")
      (arr(0), arr(1).toLong, obj._2)
    }.toDF("device_id", "info_id", "score").createOrReplaceTempView("user_info_simi")

    // 获取资讯标签
    log.info("Get information tags")
    val predicates = DateUtils.rangeDate(partitionDay, infoStartDate).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      s"publish_time >= '${startTime}' AND publish_time < '${endTime}' AND idwords > '' LIMIT ${mycatLimit}"
    }
    spark.read.jdbc(infoDB.jdbcUrl, infoTable, predicates, infoDB.connProps)
      .select("id", "idwords")
      .flatMap { row =>
        row.getString(1).split(",").map { tag =>
          val arr = tag.split(":")
          (row.getLong(0), arr(0).toLong, arr(1).toDouble)
        }
      }
      .toDF("id", "tag_id", "weight").createOrReplaceTempView("info_profile")

    // 用资讯标签来表示用户兴趣
    sql = "SELECT a.device_id, b.tag_id, CAST(SUM(a.score * b.weight) AS DECIMAL(12, 9)) score" +
      " FROM user_info_simi a JOIN info_profile b" +
      " ON a.info_id = b.id" +
      " GROUP BY a.device_id, b.tag_id" +
      " HAVING score > 0"
    log.debug(sql)
    val userProfile = spark.sql(sql).map { row =>
      (row.getString(0), Array(row.getLong(1) -> row.getDecimal(2)))
    }.rdd.reduceByKey(_ ++ _).map { user =>
      val stags = user._2.sortWith((bigger, smaller) => bigger._2.compareTo(smaller._2) > 0)
      val tags = stags.map(tag => (tag._1, tag._2.setScale(relevancyScale, RoundingMode.UP))).filter(_._2.doubleValue() > 0).take(userTagNum)
      val sum = stags.take(userWtagNum).map(_._2).reduce((acc, current) => acc.add(current))
      val wtags = stags.take(userWtagNum).map(tag => (tag._1, tag._2.divide(sum, weightScale, RoundingMode.HALF_UP))).filter(_._2.doubleValue() > 0)
      (user._1, tags.map(tag => (tag._1 + ":" + tag._2)).mkString(","), wtags.map(tag => (tag._1 + ":" + tag._2)).mkString(","))
    }.map { row =>
      val put = new Put(Bytes.toBytes(row._1))
      put.addColumn(Bytes.toBytes("tag"), Bytes.toBytes("tags"), Bytes.toBytes(row._2))
      put.addColumn(Bytes.toBytes("tag"), Bytes.toBytes("wtags"), Bytes.toBytes(row._3))
      put
    }.coalesce(parallelism)

    // 写入HBase
    log.info("Write user profile to hbase table: user_profile")
    val config = new Configuration
    config.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    config.set(TableOutputFormat.OUTPUT_TABLE, "user_profile")
    config.addResource(spark.sparkContext.hadoopConfiguration)
    val job = Job.getInstance(config)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    HBaseUtil.writeHBase(job.getConfiguration, userProfile)
  }
}