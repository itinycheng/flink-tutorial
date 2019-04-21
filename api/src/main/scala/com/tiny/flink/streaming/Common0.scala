package com.tiny.flink.streaming

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Common0 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(2)
    env.getConfig.setMaxParallelism(10)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(5000L)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)

    val text1 = env.socketTextStream("localhost", 12345)
    val text2 = env.socketTextStream("localhost", 23456)
    text1.flatMap(_.toUpperCase.split("\\W+"))
      .uid("uid")
      .filter(_.nonEmpty)
      .map(s => s + s.toInt)
      .rebalance // round-robin
      .global
      .shuffle
      .forward

    // .connect(text2)
    // .join(text2)
    // .broadcast
    // .coGroup(text2)


    text2.broadcast

  }
}
