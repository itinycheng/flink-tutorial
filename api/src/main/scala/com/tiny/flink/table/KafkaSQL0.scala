package com.tiny.flink.table

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.types.Row

/**
 * TODO
 *
 * read CVS data from Kafka
 *
 * */
object KafkaSQL0 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    val kafka08 = new KafkaTableSource(schema, activityTopic, kafkaProps, new JsonSer)
    // tableEnv.registerTableSource("activity_log", kafka08)
    val table = tableEnv.sqlQuery("select * from activity_log where productid = 3281358")

    table.toAppendStream[Row].print()
    println(env.getExecutionPlan)
    env.execute()
  }

  def schema: TableSchema = new TableSchema.Builder()
    .field("trackid", DataTypes.STRING())
    .field("deviceid", DataTypes.STRING())
    .field("productid", DataTypes.BIGINT())
    .field("sessionid", DataTypes.STRING())
    .field("starttime", DataTypes.BIGINT())
    .field("pageduration", DataTypes.INT())
    .field("versioncode", DataTypes.STRING())
    .field("refpagename", DataTypes.STRING())
    .field("pagename", DataTypes.STRING())
    .field("platformid", DataTypes.INT())
    .field("partnerid", DataTypes.BIGINT())
    .field("developerid", DataTypes.BIGINT())
    .field("deviceoffset", DataTypes.BIGINT())
    .field("productdeviceoffset", DataTypes.BIGINT())
    .build()

  def kafkaProps: Properties = {
    val props = new Properties()
    props.put("fetch.message.max.bytes", "33554432")
    props.put("socket.receive.buffer.bytes", "1048576")
    props.put("auto.commit.interval.ms", "10000")
    props.put("bootstrap.servers", kafkaServers)
    props.put("zookeeper.connect", kafkaZookeepers)
    props.put("group.id", consumerKafkaSQL_83)
    props
  }

  class JsonSer extends DeserializationSchema[Row] {

    override def deserialize(message: Array[Byte]): Row = {
      val arr = new String(message, StandardCharsets.UTF_8).split(",")
      if (arr.length >= 14) {
        Row.of(arr(0), arr(1), arr(2).toLong.asInstanceOf[Object], arr(3)
          , arr(4).toLong.asInstanceOf[Object], arr(5).toInt.asInstanceOf[Object]
          , arr(6), arr(7), arr(8), arr(9).toInt.asInstanceOf[Object]
          , arr(10).toLong.asInstanceOf[Object], arr(11).toLong.asInstanceOf[Object]
          , arr(12).toLong.asInstanceOf[Object], arr(13).toLong.asInstanceOf[Object])
      } else null
    }

    override def isEndOfStream(nextElement: Row): Boolean = false

    override def getProducedType: TypeInformation[Row] = new RowTypeInfo(schema.getFieldTypes, schema.getFieldNames)

  }

}
