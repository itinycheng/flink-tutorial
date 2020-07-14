package com.tiny.flink.table

import com.tiny.flink.table.udf.LongToStrDate
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

/**
 * TODO
 * join kafka & hive
 */
object KafkaHiveSQL1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/apache/hive/latest/conf", "3.1.1"))
    tableEnv.registerFunction("str_date", new LongToStrDate)
    tableEnv.sqlUpdate(activityTableSchema)
    val table = tableEnv.sqlQuery("select deviceid, productdeviceoffset, str_date(starttime) from activity_log where productid = 3281358")

    table.toAppendStream[Row].print()
    println(env.getExecutionPlan)
    env.execute()


  }

}
