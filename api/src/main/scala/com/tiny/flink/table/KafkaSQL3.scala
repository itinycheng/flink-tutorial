package com.tiny.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
/**
 * read and query complex type data
 *
 */
object KafkaSQL3 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate(userTableSchema)
    val table = tableEnv.sqlQuery("select event.attribute['userID'] from user_log")
    table.toAppendStream[Row].print()

    println(env.getExecutionPlan)
    env.execute()
  }

}
