package com.tiny.flink.streaming.model

class AverageAccumulator extends Serializable {
  var sum: Long = _
  var count: Long = _
}
