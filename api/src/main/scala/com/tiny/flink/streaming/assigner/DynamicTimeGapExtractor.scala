package com.tiny.flink.streaming.assigner

import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor

class DynamicTimeGapExtractor extends SessionWindowTimeGapExtractor[(Long, String, Int)] {
  override def extract(element: (Long, String, Int)): Long = (math.random * 20000).toLong
}

object DynamicTimeGapExtractor {
  def apply(): DynamicTimeGapExtractor = new DynamicTimeGapExtractor()
}