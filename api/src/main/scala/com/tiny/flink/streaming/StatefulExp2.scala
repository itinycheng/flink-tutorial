package com.tiny.flink.streaming

import com.tiny.flink.streaming.function.{MapStateFunction, QueryableStateFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author tiny.wang
 */
object StatefulExp2 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .map(QueryableStateFunction())

    counts.print
    env.execute("""Scala Stateful Computation Example""")
  }

}
