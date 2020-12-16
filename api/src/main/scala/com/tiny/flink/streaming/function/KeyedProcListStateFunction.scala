package com.tiny.flink.streaming.function

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class KeyedProcListStateFunction extends KeyedProcessFunction[String, (Int, String), String] {

  private[this] var state: ListState[String] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ListStateDescriptor[String]("state_contents",
      TypeInformation.of(new TypeHint[String] {}))
    state = getRuntimeContext.getListState(descriptor)
  }

  override def processElement(value: (Int, String), ctx: KeyedProcessFunction[String, (Int, String), String]#Context, out: Collector[String]): Unit = {
    println(s"current key : ${ctx.getCurrentKey}")
    println("------original--------")
    state.get().asScala.foreach(println)
    state.add(value.toString())
    println("------added current element--------")
    state.get().asScala.foreach(println)
    ctx.timerService().registerEventTimeTimer(math.max(2, value._1 - (value._1 % 2)))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (Int, String), String]#OnTimerContext, out: Collector[String]): Unit = {
    println(s"time domain : ${ctx.timeDomain}")
    println(s"timestamp: ${ctx.timestamp}")
    println(s"watermark: ${ctx.timerService.currentWatermark}")
    println(s"state before update: ${state.get()}")
    state.update(state.get().asScala.filter(!_.contains("DEL")).toList.asJava)
    println(s"state updated: ${state.get()}")
  }
}
