package com.tiny.flink.table

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.hcatalog.scala.HCatInputFormat
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.hadoop.hive.conf.HiveConf

object HiveHCatalogSQL {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.createInput(new HCatInputFormat[(Int, Int, String, String)]("analytics",
      "tmp_person", new HiveConf))
    tableEnv.registerDataSet("tmp_person", dataSet)
    tableEnv.sqlUpdate("insert into tmp_person values(1, 20, 'tiny', '20190413')")

    val table = tableEnv.sqlQuery("select id from tmp_person")
    table.toDataSet[Int].print()
  }
}
