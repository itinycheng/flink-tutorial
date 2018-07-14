package com.tiny.flink.window

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object TimeWindow0 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, (math.random * 10).toInt))
      .keyBy(0)
      .timeWindow(Time.of(5, TimeUnit.SECONDS))
      .sum(1)
    counts.print
    env.execute("""Scala WordCount from SocketTextStream Example""")
  }

}
