package com.tiny.flink.table

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * must be executed on server side
 */
object HiveCatalogSQL {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", null, "2.3.4"))
    val result = tableEnv.sqlQuery("select * from hive.analytics.tmp_sp_serde")
    tableEnv.toDataSet[(Long, String, String, String, String)](result).print()
  }
}
