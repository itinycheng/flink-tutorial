package com.tiny.flink.table

import com.tiny.flink.table.udf.LongToStrDate
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

/**
 * read from Kafka format with CSV
 * sink to kafka & std-out format with JSON
 *
 * UDF support
 **/
object KafkaSQL2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.registerFunction("str_date", new LongToStrDate)
    tableEnv.sqlUpdate(activityTableSchema)
    tableEnv.sqlUpdate(deviceTableSchema)
    tableEnv.sqlUpdate("insert into device_log select deviceid, productdeviceoffset, str_date(starttime) from activity_log where productid = 3281358")
    val table = tableEnv.sqlQuery("select deviceid, productdeviceoffset, str_date(starttime) from activity_log where productid = 3281358")
    table.toAppendStream[Row].print()

    println(env.getExecutionPlan)
    env.execute()
  }

}
