package com.tiny.flink.streaming

import com.tiny.flink.streaming.function.KeyedProcListStateFunction
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

/**
 * TODO internalState VS UserFacingState
 *
 * NOTE - TINY:
 * watermark not only used in window but also in stateful function.
 * think more about io use in diff scenario with diff state(ListState, MapState)
 * ListState contains update
 */
object StatefulExp4 {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointInterval(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.map(_.toUpperCase.split(","))
      .filter(_.length == 2)
      .map(arr => (arr(0).toInt, arr(1)))
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(_._2)
      .process(new KeyedProcListStateFunction())

    counts.print
    env.execute("""Scala Stateful Computation Example""")
  }

  def watermarkStrategy: WatermarkStrategy[(Int, String)] = {
    new WatermarkStrategy[(Int, String)] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(Int, String)] = {
        new WatermarkGenerator[(Int, String)] {

          private var maxTimestamp = 0L

          private val delay = 0

          override def onEvent(event: (Int, String), eventTimestamp: Long, output: WatermarkOutput): Unit = {
            maxTimestamp = math.max(maxTimestamp, event._1)
            if ("emit" == event._2)
              output.emitWatermark(new Watermark(maxTimestamp - delay))
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(maxTimestamp - delay))
          }
        }
      }

      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(Int, String)] = {
        new TimestampAssigner[(Int, String)] {
          override def extractTimestamp(element: (Int, String), recordTimestamp: Long): Long = {
            math.max(element._1, recordTimestamp)
          }
        }
      }
    }
  }

}
