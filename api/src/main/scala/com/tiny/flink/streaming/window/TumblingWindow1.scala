package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  * 1. a window is created as soon as the first element that should belong to this window arrives
  * 2. TumblingEventTimeWindows.of(time, offset), create a new window at every (n min + 30s)
  * 3. TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)), balance time zone.
  * 4. output prev window's content when a new window is created.
  *
  * @author tiny.wang
  */
object TumblingWindow1 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // NOTE - TINY: default: TimeCharacteristic.ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((System.currentTimeMillis, _, 1))
      // NOTE - TINY: assignAscendingTimestamps default: AscendingTimestampExtractor
      .assignAscendingTimestamps(_._1)
      .keyBy(1)
      //TumblingEventTimeWindows, TumblingProcessingTimeWindows
      .window(TumblingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
