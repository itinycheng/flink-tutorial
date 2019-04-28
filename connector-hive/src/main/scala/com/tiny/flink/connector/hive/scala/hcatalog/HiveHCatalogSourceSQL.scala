package com.tiny.flink.connector.hive.scala.hcatalog

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{TableEnvironment, Types}

object HiveHCatalogSourceSQL {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = HiveHCatalogTableSource.builder()
      .database("analytics")
      .metastoreUris("thrift://ip:10000")
      .table("tmp_student")
      .field("id", Types.INT)
      .field("age", Types.INT)
      .field("name", Types.STRING)
      .field("cdate", Types.STRING)
      .build()
      .getDataSet(env.getJavaEnv)
    tableEnv.registerDataSet("tmp_student", new DataSet(dataSet))

  }
}
