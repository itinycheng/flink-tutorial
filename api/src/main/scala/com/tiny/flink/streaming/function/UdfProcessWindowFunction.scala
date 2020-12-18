package com.tiny.flink.streaming.function

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UdfProcessWindowFunction extends ProcessWindowFunction[(Int, String, Int), String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(Int, String, Int)], out: Collector[String]): Unit = {
    out.collect(key)
    println(elements)
    println(context.window)
  }
}
