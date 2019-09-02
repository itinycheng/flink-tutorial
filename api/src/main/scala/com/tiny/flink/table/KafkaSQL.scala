package com.tiny.flink.table

import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka08TableSource
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.types.Row

class KafkaSQL {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    val schema = new TableSchema.Builder()
        .field("trackid", DataTypes.STRING())
        .field("deviceid", DataTypes.STRING())
        .field("productid", DataTypes.BIGINT())
        .field("sessionid", DataTypes.STRING())
        .field("starttime",DataTypes.BIGINT())
        .field("pageduration",DataTypes.INT())
        .field("versioncode",DataTypes.STRING())
        .field("refpagename", DataTypes.STRING())
        .field("pagename", DataTypes.STRING())
        .field("platformid", DataTypes.INT())
        .field("partnerid", DataTypes.BIGINT())
        .field("developerid", DataTypes.BIGINT())
        .field("deviceoffset", DataTypes.BIGINT())
        .field("productdeviceoffset", DataTypes.BIGINT())
      .build()

    val  props = new Properties()
    props.put("fetch.message.max.bytes", "33554432")
    props.put("socket.receive.buffer.bytes", "1048576")
    props.put("auto.commit.interval.ms", "10000")
    props.put("bootstrap.servers", args(1))
    props.put("zookeeper.connect", args(2))
    props.put("group.id", args(3))

    val kafka08 = new Kafka08TableSource(schema, args(0), props, new JsonSer)
    tableEnv.registerTableSource("kafka", kafka08)
  }
}

class JsonSer extends DeserializationSchema[Row]{

  private val mapper = new ObjectMapper()

  override def deserialize(message: Array[Byte]): Row = {
      mapper.readValue(message, classOf[Row])
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = TypeExtractor.getForClass(classOf[Row])
}