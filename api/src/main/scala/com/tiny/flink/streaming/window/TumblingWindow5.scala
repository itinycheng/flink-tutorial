package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * NOTE - TINY:
  * Compared to event time, ingestion time programs cannot handle any out-of-order events or late data,
  * but the programs don’t have to specify how to generate watermarks.
  *
  * @author tiny.wang
  */
object TumblingWindow5 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((System.currentTimeMillis, _, 1))
      .assignTimestampsAndWatermarks(new IngestionTimeExtractor[(Long, String, Int)])
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .sum(2)

    counts.print
    env.execute("""Time Window Example""")
  }

}
