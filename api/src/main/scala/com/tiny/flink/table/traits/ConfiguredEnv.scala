package com.tiny.flink.table.traits

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.{Time => CommonTime}
import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * provide a method for create and config flink env,
 * overwrite the config if it doesn't meet your needs
 *
 * @author tiny.wang
 */
trait ConfiguredEnv {

  def newAndConfStreamEnv(): StreamExecutionEnvironment = {

    // ----------------------- default env config -----------------------
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(4096)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()
    env.getConfig.setAutoWatermarkInterval(20 * 1000)
    env.setRestartStrategy(RestartStrategies.failureRateRestart(10,
      CommonTime.of(60, TimeUnit.SECONDS), CommonTime.of(10, TimeUnit.SECONDS)))

    val stateBackend = new RocksDBStateBackend("hdfs:///flink/rocksdb", true)
    // NOTE - TINY: write state to a random dir
    // stateBackend.setDbStoragePaths("~/rocksdb/")
    env.setStateBackend(stateBackend.asInstanceOf[StateBackend])
    env.enableCheckpointing(5 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(4 * 60 * 1000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5 * 1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    env
  }

  def newTestStreamEnv(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()
    env.getConfig.setAutoWatermarkInterval(20 * 1000)
    env.setStateBackend(new MemoryStateBackend())
    env.enableCheckpointing(5 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env
  }

  def newBatchTableEnv(): TableEnvironment = {
    val setting = EnvironmentSettings.newInstance.useBlinkPlanner.inBatchMode.build
    val tEnv = TableEnvironment.create(setting)
    // set config
    val envConfig = tEnv.getConfig.getConfiguration
    envConfig.setBoolean(PipelineOptions.OBJECT_REUSE, true)
    tEnv
  }

  def newStreamTableEnv(env: StreamExecutionEnvironment): StreamTableEnvironment = {
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, bsSettings)
    tEnv
  }

  /**
   * add hive catalog
   */
  def addHiveCatalog(tEnv: TableEnvironment): Unit = {
    val catalog = new HiveCatalog("hive", "signature", "/opt/cloudera/parcels/CDH/lib/hive/conf")
    tEnv.registerCatalog("hive", catalog)
    tEnv.useCatalog("hive")
  }

}
