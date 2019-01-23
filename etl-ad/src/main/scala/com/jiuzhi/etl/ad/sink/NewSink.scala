package com.jiuzhi.etl.ad.sink

import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

import org.zc.sched.model.DBConn

class NewSink(dbConn: DBConn, tableNew: String) extends ForeachWriter[Row] {

  var conn: Connection = _
  var stmt: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(dbConn.jdbcDriver)
    conn = DriverManager.getConnection(dbConn.jdbcUrl, dbConn.username, dbConn.password)
    stmt = conn.createStatement
    true
  }

  def process(row: Row) {
    val aid = row.getString(0)
    val channelCode = row.getString(1)
    val area = row.getString(2)
    val ip = row.getString(3)
    val create_time = row.getString(4)
    val sql = s"""INSERT INTO ${tableNew} (aid, channel_code, init_area, area, init_ip, ip, create_time, update_time) 
          VALUES ('${aid}', '${channelCode}', '${area}', '${area}', '${ip}', '${ip}', '${create_time}', '${create_time}')
          ON DUPLICATE KEY UPDATE area = '${area}', ip = '${ip}', update_time = '${create_time}'"""
    stmt.executeUpdate(sql)
  }

  def close(errorOrNull: Throwable) {
    if (stmt != null) stmt.close
    if (conn != null) conn.close
  }

}