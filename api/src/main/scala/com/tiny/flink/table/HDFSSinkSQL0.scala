package com.tiny.flink.table


import java.time.ZoneId

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

/**
 * 1. read from kafka and format string to Row<..>
 * 2. sink specific-columns to HDFS
 */
object HDFSSinkSQL0 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate(activityTableSchema)
    val result = tableEnv.sqlQuery("select deviceid, productdeviceoffset, starttime from activity_log")
    val input = result.toAppendStream[Row]

    input.map(row => Device(row.getField(0).asInstanceOf[String],
      row.getField(1).asInstanceOf[Long],
      row.getField(2).asInstanceOf[Long]))
      .addSink(StreamingFileSink.forBulkFormat(new Path("/user/hadoop/analytics/kafka-data"),
        ParquetAvroWriters.forReflectRecord(classOf[Device]))
          .withBucketAssigner(new DateTimeBucketAssigner[Device]("yyyy-MM-dd", ZoneId.of("+8")))
          .withBucketCheckInterval(1000)
        .build)

    println(env.getExecutionPlan)
    env.execute()
  }

}

case class Device(deviceId: String, offset: Long, startTime: Long)
