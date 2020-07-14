package com.tiny.flink.table

import java.time.ZoneId

import com.tiny.flink.table.entity.Device
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
/**
 * read HadoopRecoverableFsDataOutputStream to learn detail implementation of the OutputStream in `Encoder.encode(IN, OutputStream)`
 * HDFS data write cache config: io.file.buffer.size, default: 4096, current: 131072
 */
object HDFSSinkSQL1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60 * 1000)
    env.getCheckpointConfig.setCheckpointTimeout(20000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.sqlUpdate(activityTableSchema)
    val result = tableEnv.sqlQuery("select deviceid, productdeviceoffset, starttime from activity_log")
    val input = result.toAppendStream[Row]

    input.map(row => Device(row.getField(0).asInstanceOf[String],
      row.getField(1).asInstanceOf[Long],
      row.getField(2).asInstanceOf[Long]).toString)
      .filter(_.nonEmpty)
      .addSink(StreamingFileSink.forRowFormat(new Path("hdfs:///user/hadoop/analytics/kafka-data"),
        new SimpleStringEncoder[String]())
        .withNewBucketAssignerAndPolicy(new DateTimeBucketAssigner[String]("yyyy-MM-dd", ZoneId.of("+8")),
          DefaultRollingPolicy.create()
            .withRolloverInterval(60 * 60 * 1000)
            .withInactivityInterval(10 * 60 * 1000)
            .withMaxPartSize(256 * 1024 * 1024)
            .build()
        ).withBucketCheckInterval(2000)
        .build
      )

    println(env.getExecutionPlan)
    env.execute()
  }

}


