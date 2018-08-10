package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  *
  * @author tiny.wang
  */
object SlidingWindow0 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((System.currentTimeMillis, _, 1))
      .assignAscendingTimestamps(_._1)
      .keyBy(1)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5),
        Time.seconds(5)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
