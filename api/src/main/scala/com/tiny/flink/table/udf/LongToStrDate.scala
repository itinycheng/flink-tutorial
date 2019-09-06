package com.tiny.flink.table.udf

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}

class LongToStrDate extends ScalarFunction {

  private val pattern: String = "yyyy-MM-dd HH:mm:ss"

  def main(args: Array[String]): Unit = {
    println(eval(10000000))
  }

  override def open(context: FunctionContext): Unit = {
    println("open.")
  }

  def eval(timestamp: Long): String = DateTimeFormatter.ofPattern(pattern)
    .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()))


  override def close(): Unit = {
    println("close.")
  }

}
