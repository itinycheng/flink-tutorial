package com.tiny.flink.streaming.window

import com.tiny.flink.streaming.function.UdfProcessWindowFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * NOTE - TINY: get side output from union stream
 *
 * @author tiny.wang
 */
object SlidingWindow2 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)
    env.disableOperatorChaining()


    val text = env.socketTextStream("localhost", 12345)
    val stream = text.map(_.split(","))
      .filter(arr => arr.nonEmpty && arr.length == 2)
      .map(arr => (arr(0).toInt, arr(1), 1))
      .assignAscendingTimestamps(_._1)

    val outputTag = OutputTag[(Int, String, Int)]("lateness")

    val window1 = stream.keyBy(_._2)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2)))
      .sideOutputLateData(outputTag)
      .process(new UdfProcessWindowFunction)

    val window2 = stream.keyBy(_._2)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(5)))
      .sideOutputLateData(outputTag)
      .process(new UdfProcessWindowFunction)

    // work well
    window1.getSideOutput(outputTag)
      .union(window2.getSideOutput(outputTag))

    // not work
    val union = window1.union(window2)
    union.print()
    union.getSideOutput(outputTag).print()

    env.execute("""Time Window Example""")
  }

}
