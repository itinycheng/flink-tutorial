package com.tiny.flink.streaming.window

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * reduce function
  *
  * @author tiny.wang
  */
object TumblingWindow9 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // .reduce((origin, input) => (origin._1, origin._2 + input._2))
      .reduce((origin: (String, Int), input: (String, Int)) => (origin._1, origin._2 + input._2), processWindowFunction())

    counts.print
    env.execute("""Time Window Example""")
  }


  def processWindowFunction(): ProcessWindowFunction[(String, Int), String, Tuple, TimeWindow] = {
    new ProcessWindowFunction[(String, Int), String, Tuple, TimeWindow] {
      override def process(key: Tuple, context: Context, input: Iterable[(String, Int)], out: Collector[String]): Unit = {
        val k = key.asInstanceOf[Tuple1[String]].f0
        val count = input.foldLeft(0)(_ + _._2)
        out.collect(s"window: ${context.window}, key:$k count: $count")
      }
    }
  }
}
