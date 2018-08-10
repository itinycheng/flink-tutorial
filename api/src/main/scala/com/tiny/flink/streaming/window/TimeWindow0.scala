package com.tiny.flink.streaming.window

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
      // NOTE - TINY: print Collection[(key, value)] of the last 5 seconds
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print
    env.execute("""Time Window Example""")
  }

}
