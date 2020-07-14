package com.tiny.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
/**
 * insert into partition table
 * sourceTable data format: textFile
 * sinkTable data format: json, parquet
 */
object HiveSQL1 {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/apache/hive/latest/conf", "3.1.1"))
    tableEnv.sqlUpdate("insert into hive.analytics.tmp_json_serde select id, name, desc, birthday"
      + " from hive.analytics.tmp_sp_serde")

    tableEnv.sqlUpdate("insert into hive.analytics.tmp_sp_serde_parquet select id, name, desc, birthday"
      + " from hive.analytics.tmp_sp_serde")

    println(env.getExecutionPlan)
    env.execute()
  }
}
