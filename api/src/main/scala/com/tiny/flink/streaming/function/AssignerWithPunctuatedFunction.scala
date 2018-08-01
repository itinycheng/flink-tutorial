package com.tiny.flink.streaming.function

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * NOTE - TINY:
  * if(? == WK) new watermark
  *
  * @author tiny.wang
  */
class AssignerWithPunctuatedFunction extends AssignerWithPunctuatedWatermarks[(Long, String, Int)] {

  private val watermark = "WK"

  override def checkAndGetNextWatermark(lastElement: (Long, String, Int), extractedTimestamp: Long): Watermark = {
    if (watermark == lastElement._2) new Watermark(extractedTimestamp) else null
  }

  override def extractTimestamp(element: (Long, String, Int), previousElementTimestamp: Long): Long = element._1
}

object AssignerWithPunctuatedFunction {
  def apply(): AssignerWithPunctuatedFunction = new AssignerWithPunctuatedFunction()
}


