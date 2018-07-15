package com.tiny.flink.streaming.function

import com.tiny.flink.streaming.model.AverageAccumulator
import org.apache.flink.api.common.functions.RichAggregateFunction

class AggregateFunction extends RichAggregateFunction[(String, Int), AverageAccumulator, Double] {
  override def createAccumulator(): AverageAccumulator = new AverageAccumulator

  override def add(value: (String, Int), accumulator: AverageAccumulator): AverageAccumulator = {
    accumulator.count += 1
    accumulator.sum += value._2
    accumulator
  }

  override def getResult(accumulator: AverageAccumulator): Double = {
    accumulator.sum.toDouble / accumulator.count
  }

  /**
    * TODO merge invoke by what
    */
  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    a.sum += b.sum
    a.count += b.count
    a
  }
}


object AggregateFunction {
  def apply(): AggregateFunction = new AggregateFunction()
}
