package com.tiny.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 *
 * write complex type data to hive
 */
object HiveSQL5 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/hive/conf", "3.1.1"))
    tableEnv.useCatalog("hive")
    tableEnv.useDatabase("analytics")
    // TODO
    tableEnv.sqlUpdate("insert into tmp_newuser select ....")
    println(env.getExecutionPlan)
    env.execute()
  }
}
