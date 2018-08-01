package com.tiny.flink.streaming.function

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * NOTE - TINY:
  * 1. like as [[org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor]]
  * 2. several abstract class can use which implement AssignerWithPeriodicWatermarks
  *
  * @author tiny.wang
  */
class AssignerWithPeriodicFunction extends AssignerWithPeriodicWatermarks[(Long, String, Int)] {

  val maxOutOfOrderness: Long = 3000L

  var currentMaxTimestamp: Long = _

  /**
    * NOTE - TINY:
    * 1. generate a watermark with fixed delay(maxOutOfOrderness).
    * 2. watermark will send to the downstream operator.
    */
  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  /**
    * NOTE - TINY:
    * 1. called when input an element
    * 2. extract timestamp as current eventTime
    */
  override def extractTimestamp(element: (Long, String, Int), previousElementTimestamp: Long): Long = {
    currentMaxTimestamp = math.max(element._1, currentMaxTimestamp)
    element._1
  }
}

object AssignerWithPeriodicFunction {

  def apply(): AssignerWithPeriodicFunction = new AssignerWithPeriodicFunction()

}
