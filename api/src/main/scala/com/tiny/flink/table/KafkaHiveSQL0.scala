package com.tiny.flink.table

import com.tiny.flink.table.udf.LongToStrDate
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * unsupported for Flink-1.9
 **/
object KafkaHiveSQL0 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/apache/hive/latest/conf", "3.1.1"))
    tableEnv.registerFunction("str_date", new LongToStrDate)
    tableEnv.sqlUpdate(activityTableSchema)
    tableEnv.sqlUpdate("insert into hive.analytics.tmp_activity_log " +
      "select productid, platformid, sessionid, productdeviceoffset, str_date(starttime) from activity_log")
  }

}
