package com.tiny.flink.streaming.window

import com.tiny.flink.streaming.source.SocketWkTsFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  *
  * @author tiny.wang
  */
object TumblingWindow6 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.addSource(SocketWkTsFunction("localhost", 12345))
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(1)

    counts.print
    env.execute("""Time Window Example""")
  }

}
