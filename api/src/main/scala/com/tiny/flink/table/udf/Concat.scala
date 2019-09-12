package com.tiny.flink.table.udf

import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}

/**
 * UDF: concat Objects to String
 **/
class Concat extends ScalarFunction {

  override def open(context: FunctionContext): Unit = {
    println("open.")
  }

  def eval(delimiter: String, items: Any*): String = {
    val buf = new StringBuilder
    for (item <- items) buf ++= item + delimiter
    buf.substring(0, buf.length - delimiter.length)
  }

  override def close(): Unit = {
    println("close.")
  }
}
