package com.tiny.flink.streaming

import org.apache.flink.streaming.api.scala._

/**
  * @author tiny.wang
  */
object StatefulExp0 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // NOTE - TINY: output tuple(key, originVal), update key state by sum(originVale + tuple._2)
      .mapWithState((in: (String, Int), count: Option[Int]) => {
      count match {
        case Some(c) => ((in._1, c), Some(c + in._2))
        case _ => ((in._1, 0), Some(in._2))
      }
    })
    counts.print
    env.execute("""Scala Stateful Computation Example""")
  }

}
