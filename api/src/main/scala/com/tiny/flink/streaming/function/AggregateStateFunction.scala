package com.tiny.flink.streaming.function

import com.tiny.flink.streaming.model.AverageAccumulator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration

class AggregateStateFunction extends RichMapFunction[(String, Int), Double] {

  private[this] var state: AggregatingState[(String, Int), Double] = _

  override def map(value: (String, Int)): Double = {
    val tmp = state
    tmp.add(value)
    tmp.get()
  }

  override def open(parameters: Configuration): Unit = {
    val descriptor = new AggregatingStateDescriptor("aggregate",
      AggregateFunction(),
      TypeInformation.of(new TypeHint[AverageAccumulator] {}))
    state = getRuntimeContext.getAggregatingState(descriptor)
  }

  override def close(): Unit = {
    println("close")
  }

}

object AggregateStateFunction {
  def apply(): AggregateStateFunction = new AggregateStateFunction()
}


