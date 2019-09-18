package com.tiny.flink

import org.slf4j.{Logger, LoggerFactory}

package object table {

  lazy val logger: Logger = LoggerFactory.getLogger("")

  val kafkaServers = "172.0.0.1:9092,172.0.0.1:9092,172.0.0.1:9092"

  val kafkaZookeepers = "172.0.0.1:2181,172.0.0.1:2181,172.0.0.1:2181/kafka8"

  val activityTopic = "analytics_activity_log"

  val deviceTopic = "analytics_device_log"

  val consumerKafkaSQL_83 = "KafkaSQL-83"
}
