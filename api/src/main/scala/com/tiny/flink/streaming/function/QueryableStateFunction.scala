package com.tiny.flink.streaming.function

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration

class QueryableStateFunction extends RichMapFunction[(String, Int), (String, Int)] {

  /**
    * NOTE - TINY:
    * client must keep Type[[java.lang.Integer]] consistent with server
    * that's why user Integer instead of scala.Int
    */
  private[this] var state: ValueState[Integer] = _

  override def map(value: (String, Int)): (String, Int) = {
    val tmp = state
    val count = if (null == tmp.value()) 0 else tmp.value() + 1
    state.update(count)
    (value._1, count)
  }

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[Integer]("sum",
      TypeInformation.of(new TypeHint[Integer] {}))
    descriptor.setQueryable("word_count")
    state = getRuntimeContext.getState(descriptor)
  }

  override def close(): Unit = {
    println("close")
  }
}

object QueryableStateFunction {
  def apply(): QueryableStateFunction = new QueryableStateFunction()
}


