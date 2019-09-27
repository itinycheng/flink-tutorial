package com.tiny.flink

import org.slf4j.{Logger, LoggerFactory}

package object table {

  lazy val logger: Logger = LoggerFactory.getLogger("")

  val kafkaServers = "172.0.0.1:9092,172.0.0.1:9092,172.0.0.1:9092"

  val kafkaZookeepers = "172.0.0.1:2181,172.0.0.1:2181,172.0.0.1:2181/kafka8"

  val activityTopic = "analytics_activity_log"

  val deviceTopic = "analytics_device_log"

  val consumerKafkaSQL_83 = "KafkaSQL-83"

  val activityTableSchema: String =
    s"""create table activity_log(
         `trackid` varchar(200),
         `deviceid` varchar,
         `productid` bigint,
         `sessionid` varchar,
         `starttime` BIGINT,
         `pageduration` int,
         `versioncode` varchar,
         `refpagename` varchar,
         `pagename` varchar,
         `platformid` int,
         `partnerid` bigint,
         `developerid` bigint,
         `deviceoffset` bigint,
         `productdeviceoffset` bigint
       ) with (
          'connector.type' = 'kafka',
          'connector.version' = '0.8',
          'connector.topic' = '$activityTopic',
          'connector.startup-mode' = 'latest-offset',
          'connector.sink-partitioner' = 'fixed',
          'connector.properties.0.key' = 'fetch.message.max.bytes',
          'connector.properties.0.value' = '33554432',
          'connector.properties.1.key' = 'zookeeper.connect',
          'connector.properties.1.value' = '$kafkaZookeepers',
          'connector.properties.2.key' = 'socket.receive.buffer.bytes',
          'connector.properties.2.value' = '1048576',
          'connector.properties.3.key' = 'group.id',
          'connector.properties.3.value' = '$consumerKafkaSQL_83',
          'connector.properties.4.key' = 'bootstrap.servers',
          'connector.properties.4.value' = '$kafkaServers',
          'connector.properties.5.key' = 'auto.commit.interval.ms',
          'connector.properties.5.value' = '10000',
          'update-mode' = 'append',
          'format.field-delimiter' = ',',
          'format.property-version' = '1',
          'format.type' = 'csv',
          'format.schema' = 'ROW<trackid VARCHAR, deviceid VARCHAR, productid BIGINT, sessionid VARCHAR, starttime BIGINT, pageduration INT, versioncode VARCHAR, refpagename VARCHAR, pagename VARCHAR, platformid INT, partnerid BIGINT, developerid BIGINT, deviceoffset BIGINT, productdeviceoffset BIGINT>'
         )"""

  val deviceTableSchema: String =
    s"""create table device_log(
       |         `deviceid` varchar,
       |         `productdeviceoffset` bigint,
       |         `starttime` varchar
       |       ) with (
       |          'connector.type' = 'kafka',
       |          'connector.version' = '0.8',
       |          'connector.topic' = '$deviceTopic',
       |          'connector.startup-mode' = 'earliest-offset',
       |          'connector.sink-partitioner' = 'fixed',
       |          'connector.properties.0.key' = 'fetch.message.max.bytes',
       |          'connector.properties.0.value' = '33554432',
       |          'connector.properties.1.key' = 'zookeeper.connect',
       |          'connector.properties.1.value' = '$kafkaZookeepers',
       |          'connector.properties.2.key' = 'socket.receive.buffer.bytes',
       |          'connector.properties.2.value' = '1048576',
       |          'connector.properties.3.key' = 'group.id',
       |          'connector.properties.3.value' = '$consumerKafkaSQL_83',
       |          'connector.properties.4.key' = 'bootstrap.servers',
       |          'connector.properties.4.value' = '$kafkaServers',
       |          'connector.properties.5.key' = 'auto.commit.interval.ms',
       |          'connector.properties.5.value' = '10000',
       |          'update-mode' = 'append',
       |          'format.type' = 'json',
       |          'format.schema' = 'ROW<deviceid VARCHAR, productdeviceoffset BIGINT, starttime STRING>'
       |       )
       |""".stripMargin
}
