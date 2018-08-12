package com.tiny.flink.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * data skew by key, combine key before keyBy function
  * optimize state local cache, prevent freq access external state.
  *
  * @author tiny.wang
  */
object TumblingWindow7 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // NOTE - TINY: default watermark interval is 200ms
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getConfig.setParallelism(1)

    val text = env.socketTextStream("localhost", 12345)
    val counts = text.flatMap(_.toUpperCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(Cell(System.currentTimeMillis - (math.random * 10000).toLong, _, 1))
      .assignTimestampsAndWatermarks(new Assigner)
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //.trigger(EventTimeTrigger.create())
      // CountEvictor.of(2); each window at most have tow elements
      // TimeEvictor.of(Time.seconds(1)); each window only keep msg at first 1s
      // TODO DeltaEvictor how to use
      .evictor(TimeEvictor.of(Time.seconds(2)))
      .allowedLateness(Time.seconds(1))
      .sideOutputLateData(OutputTag[Cell]("output"))
      .sum(2)

    // side output immediately
    val side = counts.getSideOutput(OutputTag[Cell]("output"))
      .map(("output", _))

    counts.print
    side.print
    env.execute("""Time Window Example""")

  }
}

class Assigner extends AssignerWithPeriodicWatermarks[Cell] {

  var currentMaxTimestamp: Long = _

  override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp)

  override def extractTimestamp(element: Cell, previousElementTimestamp: Long): Long = {
    currentMaxTimestamp = math.max(element.ts, currentMaxTimestamp)
    element.ts
  }
}

class Delta extends DeltaFunction[Cell] {
  override def getDelta(oldDataPoint: Cell, newDataPoint: Cell): Double = newDataPoint.calc
}

case class Cell(ts: Long, item: String, var calc: Int)