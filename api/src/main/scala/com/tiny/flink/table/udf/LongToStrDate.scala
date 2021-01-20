package com.tiny.flink.table.udf

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}

class LongToStrDate extends ScalarFunction {

  private val pattern: String = "yyyy-MM-dd"

  override def open(context: FunctionContext): Unit = {
    println("open.")
  }

  def eval(timestamp: Long): String = DateTimeFormatter.ofPattern(pattern)
    .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()))

  def eval(timestamp: String): String = eval(timestamp.toLong)

  override def close(): Unit = {
    println("close.")
  }

}
