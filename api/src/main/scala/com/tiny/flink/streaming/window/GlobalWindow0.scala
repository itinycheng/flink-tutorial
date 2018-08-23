package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger

/**
  * NOTE - TINY:
  * 1. only one window.
  * 2. ContinuousEventTimeTrigger.of(time interval): emit all element at a fixed interval.
  *
  * @author tiny.wang
  */
object GlobalWindow0 {

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
      .window(GlobalWindows.create())
      // NOTE - TINY: CountTrigger, DeltaTrigger, PurgingTrigger, etc.
      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(2)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
