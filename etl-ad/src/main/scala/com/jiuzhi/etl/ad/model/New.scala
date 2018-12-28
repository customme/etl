package com.jiuzhi.etl.ad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

/**
 * 新增用户
 */
case class New(aid: String, channel_code: String, init_area: String, var area: String, init_ip: String,
  var ip: String, create_time: Timestamp, var update_time: Timestamp, create_date: Int)

object New {

  def apply(row: Row): New = {
    New(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.getString(5), row.getTimestamp(6), row.getTimestamp(7), row.getInt(8))
  }

  def update(acc: New, curr: New): New = {
    acc.area = curr.area
    acc.ip = curr.ip
    acc.update_time = curr.update_time

    acc
  }

}