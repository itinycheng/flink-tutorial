package com.tiny.flink.streaming.join

import java.lang

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

/**
 * stream1.coGroup(stream2)
 * watermark will move on next when left and right stream's watermark reach the same one
 */
object CoGroupStreamInWindow {

  case class Order(reserveTime: Long, orderId: String, items: String)

  case class Payment(payTime: Long, orderId: String, price: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setMaxParallelism(4096)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(20000)

    val stateBackend = new RocksDBStateBackend("file:///Users/tiny/rocksdb", false)
    // NOTE - TINY: write state to a random dir
    stateBackend.setDbStoragePaths("/Users/tiny/rocksdb/db1", "/Users/tiny/rocksdb/db2")
    env.setStateBackend(stateBackend.asInstanceOf[StateBackend])
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(10000)
    env.getCheckpointConfig.setCheckpointTimeout(15000)

    val stream1 = env.socketTextStream("localhost", 12345)
      .map(_.split(","))
      .filter(_.length == 3)
      .map(arr => Order(arr(0).toLong, arr(1), arr(2)))
      .assignTimestampsAndWatermarks(orderWatermarkStrategy)

    val stream2 = env.socketTextStream("localhost", 12346)
      .map(_.split(","))
      .filter(_.length == 3)
      .map(arr => Payment(arr(0).toLong, arr(1), arr(2).toDouble))
      .assignTimestampsAndWatermarks(paymentWatermarkStrategy)

    stream1.coGroup(stream2)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2))) // window[start:include, end:exclude)
      .trigger(CountTrigger.of(2)) // CountTrigger: trigger out when counting maxCount[left+right] element in a key's window
      .evictor(CountEvictor.of(10)) // CountEvictor: remove the oldest element
      .allowedLateness(Time.milliseconds(2)) // how much late after watermark allowed (lateness + timestamp <= currentWatermark)
      .apply(coGroupFunction)
      .print()

    env.execute("coGroupStreamInWindow")
  }

  def orderWatermarkStrategy: WatermarkStrategy[Order] = {
    new WatermarkStrategy[Order] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Order] = {
        new WatermarkGenerator[Order] {

          private var maxTimestamp = 0L

          private val delay = 0

          override def onEvent(event: Order, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            maxTimestamp = math.max(maxTimestamp, eventTimestamp)
            if ("emit".eq(event.orderId))
              output.emitWatermark(new Watermark(maxTimestamp - delay))
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(maxTimestamp - delay))
          }
        }
      }

      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Order] = {
        new TimestampAssigner[Order] {
          override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
            math.max(element.reserveTime, recordTimestamp)
          }
        }
      }
    }
  }

  def paymentWatermarkStrategy: WatermarkStrategy[Payment] = {
    new WatermarkStrategy[Payment] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Payment] = {
        new WatermarkGenerator[Payment] {

          private var maxTimestamp = 0L

          private val delay = 0

          override def onEvent(event: Payment, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            maxTimestamp = math.max(maxTimestamp, event.payTime)
            if ("emit".eq(event.orderId))
              output.emitWatermark(new Watermark(maxTimestamp - delay))
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(maxTimestamp - delay))
          }
        }
      }

      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Payment] = {
        new TimestampAssigner[Payment] {
          override def extractTimestamp(element: Payment, recordTimestamp: Long): Long = {
            math.max(element.payTime, recordTimestamp)
          }
        }
      }
    }
  }

  def customTrigger: Trigger[TaggedUnion[Order, Payment], TimeWindow] = {
    new Trigger[TaggedUnion[Order, Payment], TimeWindow] {

      val maxInterval = 2

      val timeStateDesc = new ValueStateDescriptor[Long]("time", TypeInformation.of(new TypeHint[Long] {}))

      override def onElement(element: TaggedUnion[Order, Payment], timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        val occurTime = if (element.getOne != null) element.getOne.reserveTime else element.getTwo.payTime
        val state = ctx.getPartitionedState(timeStateDesc)
        val timestamp = state.value()
        if (timestamp + maxInterval < occurTime) {
          state.update(occurTime)
          TriggerResult.FIRE
        } else {
          TriggerResult.CONTINUE
        }
      }

      override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

      override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        if (window.maxTimestamp() == time) TriggerResult.FIRE
        else TriggerResult.CONTINUE
      }

      override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        ctx.getPartitionedState(timeStateDesc).clear()
      }
    }
  }

  def coGroupFunction: CoGroupFunction[Order, Payment, String] = {
    new CoGroupFunction[Order, Payment, String] {
      override def coGroup(iterable: lang.Iterable[Order], iterable1: lang.Iterable[Payment], collector: Collector[String]): Unit = {
        collector.collect(iterable.toString + iterable1.toString)
      }
    }
  }


}
