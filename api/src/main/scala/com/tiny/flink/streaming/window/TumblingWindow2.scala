package com.tiny.flink.streaming.window

import com.tiny.flink.streaming.function.AssignerWithPeriodicFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  * periodic watermark
  *
  * @author tiny.wang
  */
object TumblingWindow2 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // NOTE - TINY: default watermark interval is 200ms
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((System.currentTimeMillis, _, 1))
      .assignTimestampsAndWatermarks(AssignerWithPeriodicFunction())
      .keyBy(1)
      //TumblingEventTimeWindows, TumblingProcessingTimeWindows
      .window(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
