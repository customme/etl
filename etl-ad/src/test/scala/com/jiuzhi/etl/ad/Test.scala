package com.jiuzhi.etl.ad

object Test {

  def array {
    val aaa = Array(1, 2, 3)
    val bbb = Array(4, 5, 6)
    println(aaa.++:(bbb).mkString(","))
    println(aaa.++(bbb).mkString(","))
  }

  def union {
    val aaa = Array(Array())
    val bbb = Array(Array(1, 2, 3))
    val ccc = Array(Array(4, 5, 6))
    println(aaa.union(bbb).union(ccc).size)
    println(aaa.union(bbb).union(ccc).dropWhile(_.isEmpty).size)
  }

  def combine {
    val columns = Array("id", "name", "gender")
    val combines = for (i <- 2 to columns.size) yield columns.combinations(i)
    val alls = columns.map(Array(_)).union(combines.flatten)
    alls.foreach(x => println(x.mkString(",")))
  }

  def zip {
    val columns = Array("id", "name", "gender")
    columns.zipWithIndex.foreach(x => println(x._1 + "\t" + x._2))
  }

  def dtype {
    val dtypes = Array(("id", "int"), ("name", "varchar(10)"), ("gender", "char(1)"))
    val ddl = dtypes.map(x => x._1 + " " + x._2).mkString("CREATE TABLE IF NOT EXISTS test (", ",", ")")
    println(ddl)
  }

  def propertyToMap {
    import scala.collection.JavaConversions.propertiesAsScalaMap

    println(System.getProperties.toMap.mkString("{\n", "\n", "\n}"))
  }

  def parse {
    //val aggRules = "l_01 id\r\nl_02 name\r\nl_03 id,name"
    val aggRules = ""
    val aggColumns = "id,name,gender"

    val rules = if (aggRules != "") {
      println("aggRules")
      aggRules.split("\r\n").map(x => {
        val arr = x.split(" ")
        (arr(0), arr(1).split(","))
      })
    } else {
      println("aggColumns")
      val columns = aggColumns.split(",")
      val combines = for (i <- 2 to columns.size) yield columns.combinations(i)
      columns.map(Array(_)).union(combines.flatten).zipWithIndex.map(x => ("l_" + x._2, x._1))
    }

    rules.foreach(x => {
      println(x._1 + " " + x._2.mkString(","))
    })
  }

  def main(args: Array[String]): Unit = {
    // combine
    // parse
    //zip
    //dtype
    //propertyToMap
    //array
    union
  }

}