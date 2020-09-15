package com.tiny.flink.streaming.usecase


import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  *
  * input data format: `timestamp,orderId,productId,price`
  *
  * @author tiny.wang
  */
object GmvCalc {

  val LOGGER: Logger = LoggerFactory.getLogger("")

  val orderSet: mutable.HashSet[String] = mutable.HashSet[String]()

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("USAGE:\nGmvCalc <hostname> <port>")
      return
    }
    val host = args(0)
    val port = args(1).toInt
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    setEnvConf(env)

    val sum = env.socketTextStream(host, port)
      .map(toOrder _)
      .filter(order => isNewcomer(order))
      .assignTimestampsAndWatermarks(assignWatermark)
      .keyBy(_.orderId.hashCode % 2)
      .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      .trigger(customTrigger)
      .fold(0d)((sum, order) => sum + order.price)

    sum.print
    env.execute("""Calc Gmv Example""")
  }

  def customTrigger: Trigger[Order, TimeWindow] = {
    new Trigger[Order, TimeWindow] {

      val maxInterval = 2

      val stateDesc = new ValueStateDescriptor[Long]("timestamp", TypeInformation.of(new TypeHint[Long] {}))

      override def onElement(element: Order, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        val state = ctx.getPartitionedState(stateDesc)
        // TODO why state.value == null
        val timestamp = state.value()
        if (timestamp + maxInterval < element.createTime) {
          state.update(element.createTime)
          TriggerResult.FIRE
        } else {
          TriggerResult.CONTINUE
        }
      }

      override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

      override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        if (time == window.maxTimestamp) TriggerResult.FIRE
        else TriggerResult.CONTINUE
      }

      override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        ctx.getPartitionedState(stateDesc).clear()
      }
    }
  }

  def isNewcomer(order: Order): Boolean = {
    Option(order) match {
      case Some(x) =>
        val b = orderSet.contains(x.orderId)
        orderSet += x.orderId
        !b
      case None => false
    }
  }

  def toOrder(str: String): Order = {
    try {
      val arr = str.split(",")
      if (arr.length >= 3) Order(arr(0), arr(1).toDouble, arr(2).toLong) else null
    } catch {
      case ex: Throwable => LOGGER.error("parse order info failed. ", ex)
        null
    }
  }

  case class Order(orderId: String, price: Double, createTime: Long)

  def assignWatermark: AssignerWithPeriodicWatermarks[Order] = {
    new BoundedOutOfOrdernessTimestampExtractor[Order](Time.hours(1)) {
      var currentMaxTimestamp: Long = _

      override def extractTimestamp(element: Order): Long = {
        currentMaxTimestamp = math.max(element.createTime, currentMaxTimestamp)
        element.createTime
      }
    }
  }

  def setEnvConf(env: StreamExecutionEnvironment): Unit = {
    env.setParallelism(1)
    env.setMaxParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new MemoryStateBackend(100 * 1024 * 1024, false).asInstanceOf[StateBackend])
    env.getConfig.enableForceKryo()
    env.getConfig.disableForceAvro()
    env.getConfig.setAutoWatermarkInterval(3000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(10000)
    env.getCheckpointConfig.setCheckpointTimeout(20000)
  }

}
