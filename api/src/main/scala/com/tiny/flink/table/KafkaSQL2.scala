package com.tiny.flink.table

import com.tiny.flink.table.udf.LongToStrDate
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

/**
 * read from Kafka format with CSV
 * sink to kafka & std-out format with JSON
 *
 * UDF support
 **/
object KafkaSQL2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.registerFunction("str_date", new LongToStrDate)
    tableEnv.sqlUpdate(
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
          'connector.startup-mode' = 'earliest-offset',
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
         )""")

    tableEnv.sqlUpdate(
      s"""
         | create table device_log(
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
         |""".stripMargin)

    tableEnv.sqlUpdate("insert into device_log select deviceid, productdeviceoffset, str_date(starttime) from activity_log where productid = 3281358")
    val table = tableEnv.sqlQuery("select deviceid, productdeviceoffset, str_date(starttime) from activity_log where productid = 3281358")
    table.toAppendStream[Row].print()

    println(env.getExecutionPlan)
    env.execute()
  }

}
