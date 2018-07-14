package com.tiny.flink.streaming

import com.tiny.flink.streaming.function.MapStateFunction
import org.apache.flink.streaming.api.scala._

/**
  * maintain a state of word count in [[MapStateFunction]]
  */
object StatefulExp1 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .map(MapStateFunction())

    counts.print
    env.execute("""Scala Stateful Computation Example""")
  }

}
