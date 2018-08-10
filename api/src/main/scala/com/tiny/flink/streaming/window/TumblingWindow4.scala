package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  * code work well but not suggested.
  * use System.currentMills as timestamp under IngestionTime window
  *
  * @author tiny.wang
  */
object TumblingWindow4 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((System.currentTimeMillis, _, 1))
      // NOTE - TINY: butter use System.currentMills as timestamp, [IngestionTimeExtractor]
      .assignAscendingTimestamps(_._1)
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
