package com.jiuzhi.etl.fad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Client(udid: String, app_key: String, clnt: String, var version: String, init_version: String,
  var sdkver: String, init_sdkver: String, app_path: Int, create_time: Timestamp, var update_time: Timestamp,
  create_date: Int)

object Client {

  def apply(row: Row): Client = {
    Client(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getString(3),
      row.getString(4),
      row.getString(5),
      row.getString(6),
      row.getInt(7),
      row.getTimestamp(8),
      row.getTimestamp(9),
      row.getInt(10))
  }

  def update(acc: Client, curr: Client) = {
    acc.version = curr.version
    acc.sdkver = curr.sdkver
    acc.update_time = curr.update_time

    acc
  }
}