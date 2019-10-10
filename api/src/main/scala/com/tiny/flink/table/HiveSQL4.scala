package com.tiny.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

/**
 * complex data types
 *
 * DataSet
 *
 * hive table:
 * CREATE TABLE `tmp_newuser`(
 * `user` map<string,string> COMMENT 'user',
 * `device` map<string,string> COMMENT 'device',
 * `app` map<string,string> COMMENT 'app',
 * `event` struct<eventtype:string,attribute:map<string,string>,eventdatas:array<struct<key:string,value:string,type:string>>> COMMENT 'event'
 * )
 * PARTITIONED BY (`job_time` bigint)
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
 * STORED AS TEXTFILE
 */
object HiveSQL4 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    tableEnv.registerCatalog("hive", new HiveCatalog("hive",
      "analytics", "/home/hadoop/hive/conf", "3.1.1"))
    tableEnv.useCatalog("hive")
    tableEnv.useDatabase("analytics")
    val table = tableEnv.sqlQuery("select count(distinct event.attribute['userID']) from tmp_newuser")
    table.toDataSet[Row].map(row => {
      val str = row.toString
      logger.info("print item: " + str)
      str
    }).print()

    println(env.getExecutionPlan)
    env.execute()
  }
}
