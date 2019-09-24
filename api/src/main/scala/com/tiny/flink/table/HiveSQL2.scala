package com.tiny.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

/**
 * cmd: flink run -m yarn-cluster -yn 1 -ynm sql-test -c com.tiny.flink.table.HiveSQL2 -d -yq example.jar
 *
 * use stream mode to read data from hive.
 */
object HiveSQL2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/apache/hive/latest/conf", "3.1.1"))
    val table = tableEnv.sqlQuery("select id, name from hive.analytics.tmp_sp_serde")
    table.toAppendStream[Row].map(row => {
      val str = row.toString
      logger.info("print item: " + str)
      str
    }).print()

    println(env.getExecutionPlan)
    env.execute()
  }
}
