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
    env.disableOperatorChaining()


    val text = env.socketTextStream("localhost", 12345)
    val counts = text.map(_.split(","))
      .filter(arr => arr.nonEmpty && arr.length == 2)
      .map(arr => (arr(0).toInt, arr(1), 1))
      .assignAscendingTimestamps(_._1)
      .keyBy(1)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(8)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
