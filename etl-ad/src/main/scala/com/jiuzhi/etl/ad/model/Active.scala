package com.jiuzhi.etl.ad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

/**
 * 活跃用户
 */
case class Active(aid: String, area: String, active_date: String, create_time: Timestamp, var visit_times: Int = 1)

object Active {

  def apply(row: Row): Active = {
    Active(row.getString(0), row.getString(1), row.getString(2), row.getTimestamp(3))
  }

  def update(acc: Active, curr: Active): Active = {
    acc.visit_times = acc.visit_times + curr.visit_times

    acc
  }

}