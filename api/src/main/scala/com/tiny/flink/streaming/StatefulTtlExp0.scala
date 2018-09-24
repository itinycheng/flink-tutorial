package com.tiny.flink.streaming

import com.tiny.flink.streaming.function.ValueStateTtlFunction
import org.apache.flink.streaming.api.scala._

/**
  * state time-to-live
  * @author tiny.wang
  */
object StatefulTtlExp0 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, (math.random * 100).toInt))
      .keyBy(0)
      .map(ValueStateTtlFunction())

    counts.print
    env.execute("""Scala Stateful Computation Example""")
  }

}
