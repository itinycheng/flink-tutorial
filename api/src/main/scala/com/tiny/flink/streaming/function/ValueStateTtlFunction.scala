package com.tiny.flink.streaming.function

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration

class ValueStateTtlFunction extends RichMapFunction[(String, Int), (String, Int)] {

  private[this] var state: ValueState[(String, Int)] = _

  override def map(input: (String, Int)): (String, Int) = {
    val tmp = state
    val counter = if (tmp.value == null) 0 else tmp.value._2
    val merged = (input._1, counter + input._2)
    state.update(merged)
    merged
  }

  override def open(parameters: Configuration): Unit = {
    val ttl = StateTtlConfig.newBuilder(Time.seconds(5))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()
    val descriptor = new ValueStateDescriptor[(String, Int)]("valueState",
      TypeInformation.of(new TypeHint[(String, Int)] {}))
    descriptor.enableTimeToLive(ttl)
    state = getRuntimeContext.getState(descriptor)
  }

  override def close(): Unit = {
    println("close")
  }

}

object ValueStateTtlFunction {
  def apply(): ValueStateTtlFunction = new ValueStateTtlFunction()
}



