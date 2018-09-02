package com.tiny.flink.streaming.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * aggr function
  *
  * @author tiny.wang
  */
object TumblingWindow10 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, (math.random * 10).toInt))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(aggregateFunction(), processWindowFunction())

    counts.print
    env.execute("""Time Window Example""")
  }

  def aggregateFunction(): AggregateFunction[(String, Int), (Long, Long), Double] = {
    new AggregateFunction[(String, Int), (Long, Long), Double] {
      override def createAccumulator(): (Long, Long) = (0L, 0L)

      override def add(value: (String, Int), accumulator: (Long, Long)): (Long, Long) = {
        (accumulator._1 + value._2, accumulator._2 + 1L)
      }

      override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

      override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)
    }
  }

  def processWindowFunction(): ProcessWindowFunction[Double, String, Tuple, TimeWindow] = {
    new ProcessWindowFunction[Double, String, Tuple, TimeWindow] {
      override def process(key: Tuple, context: Context, elements: Iterable[Double], out: Collector[String]): Unit = {
        val k = key.asInstanceOf[Tuple1[String]].f0
        val result = elements.foldLeft(0d)(_ + _)
        out.collect(s"key: $k, aggregate result: $result ")
      }
    }
  }
}
