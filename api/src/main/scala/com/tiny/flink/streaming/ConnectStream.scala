package com.tiny.flink.streaming

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.HeapBroadcastState
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

class ConnectStream {

  var broadcastState = new MapStateDescriptor[String, String](
    "broadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(20000)
    env.setMaxParallelism(4096)
    env.setParallelism(1)

    val stream1 = env.socketTextStream("localhost", 12345)
      .broadcast(broadcastState)

    val stream2 = env.socketTextStream("localhost", 12346)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(arr => (arr(0), arr(1)))


    stream2.connect(stream1).process(processFunction)
  }

  def processFunction: BroadcastProcessFunction[(String, String), String, String] = new BroadcastProcessFunction[(String, String), String, String]() {
    override def processElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String), String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
      val config = ctx.getBroadcastState(broadcastState)
      val iterator = config.get(value._1)
      out.collect(iterator)
    }

    override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[(String, String), String, String]#Context, out: Collector[String]): Unit = {
      val state = ctx.getBroadcastState(broadcastState)
      ctx.getBroadcastState(broadcastState).put(value, "1")
    }
  }
}
