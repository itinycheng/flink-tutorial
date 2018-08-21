package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * input (ts, word)
  *
  * @author tiny.wang
  */
object TumblingWindow8 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.map(_.toUpperCase.split("\\W+"))
      .filter(_.length >= 2)
      .filter(arr => arr(0).forall(Character.isDigit))
      .map(arr => (arr(0).toLong, arr(1), 1))
      .assignTimestampsAndWatermarks(assigner)
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
      // NOTE - TINY: description for allowedLateness(3mills);
      // 1. the last window is triggered computation when current window's watermark 3mills bigger than the last.
      // 2. take in element as long as the window which current element belongs exist.
      .allowedLateness(Time.milliseconds(3))
      .sideOutputLateData(OutputTag[(Long, String, Int)]("output"))
      .sum(2)

    // side output immediately
    val side = counts.getSideOutput(OutputTag[(Long, String, Int)]("output"))
      .map(in => ("output", in._1, in._2, in._3))

    counts.print
    side.print
    env.execute("""Time Window Example""")
  }

  def assigner: AssignerWithPeriodicWatermarks[(Long, String, Int)] = {
    new AssignerWithPeriodicWatermarks[(Long, String, Int)] {

      var currentMaxTimestamp: Long = _

      /**
        * NOTE - TINY:
        * 1. have the same effect as allowedLateness(3mills)
        * 2. the effect of maxOutOfOrderness can stack with allowedLateness(3mills);
        * means: allowedLateness(3mills) + maxOutOfOrderness(3mills) = 6mills.
        */
      var maxOutOfOrderness: Long = 3

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      override def extractTimestamp(element: (Long, String, Int), previousElementTimestamp: Long): Long = {
        currentMaxTimestamp = math.max(element._1, currentMaxTimestamp)
        element._1
      }
    }
  }

}


