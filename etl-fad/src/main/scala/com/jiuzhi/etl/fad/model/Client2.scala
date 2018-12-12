package com.jiuzhi.etl.fad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Client2(udid: String, app_key: String, clnt: String, var version: String, init_version: String,
  app_path: Int, create_time: Timestamp, var update_time: Timestamp, create_date: Int)

object Client2 {

  def apply(row: Row): Client2 = {
    Client2(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.getInt(5), row.getTimestamp(6), row.getTimestamp(7), row.getInt(8))
  }

  def update(acc: Client2, curr: Client2) = {
    acc.version = curr.version
    acc.update_time = curr.update_time

    acc
  }
}