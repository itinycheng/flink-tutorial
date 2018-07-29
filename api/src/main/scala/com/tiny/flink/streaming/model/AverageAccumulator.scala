package com.tiny.flink.streaming.model

class AverageAccumulator(var sum: Long, var count: Long) extends Serializable
