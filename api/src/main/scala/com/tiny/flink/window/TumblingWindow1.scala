package com.tiny.flink.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  * auto print window content out when next window time arrived.
  *
  * @author tiny.wang
  */
object TumblingWindow1 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      //TumblingEventTimeWindows, TumblingProcessingTimeWindows
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(1)

    counts.print
    env.execute("""Time Window Example""")
  }

}
