package com.tiny.flink.table

import java.time.ZoneId

import com.tiny.flink.table.entity.Device
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory
import org.apache.flink.runtime.util.HadoopUtils
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.hadoop.io.{NullWritable, Text}

/**
 * format data to SequenceFile
 */
object HDFSSinkSQL2 {

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
      .map(str => Tuple2.of(NullWritable.get(), new Text(str)))
      .addSink(StreamingFileSink.forBulkFormat(new Path("hdfs:///user/hadoop/analytics/kafka-data"),
        new SequenceFileWriterFactory(HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration()),
          classOf[NullWritable], classOf[Text], "bzip2"))
        .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd", ZoneId.of("+8")))
        .withBucketCheckInterval(2000)
        .build)

    println(env.getExecutionPlan)
    env.execute()
  }

}


