package com.tiny.flink.table.task

import com.tiny.flink.table.traits.ConfiguredEnv
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.flink.table.planner.calcite.{CalciteConfig, FlinkRelFactories}
import org.apache.flink.table.planner.hint.FlinkHintStrategies

/**
 * Allow order by statement in subquery by
 * overwrite the calcite's SqlToRelConverter configuration with new configuration.
 *
 * @author tiny.wang
 */
object CustomizedCalciteConfig extends ConfiguredEnv {
  def main(args: Array[String]): Unit = {
    if (args.length != 2 || args(0).toInt > args(1).toInt) {
      println(
        """
          |Param:
          | first parameter range: [0,109]
          | second parameter range: [0, 110]
          | first parameter should be equal or less than the second""".stripMargin)
      sys.exit(-1)
    }
    val leftBound = args(0)
    val rightBound = args(1)

    val tEnv = newBatchTableEnv()
    addHiveCatalog(tEnv)

    val sqlToRelConfig = SqlToRelConverter.config()
      .withTrimUnusedFields(false)
      .withHintStrategyTable(FlinkHintStrategies.createHintStrategyTable())
      .withInSubQueryThreshold(Integer.MAX_VALUE)
      .withExpand(false)
      .withRelBuilderFactory(FlinkRelFactories.FLINK_REL_BUILDER)
      .withRemoveSortInSubQuery(false) // true by default
    val plannerConfig = CalciteConfig.createBuilder().replaceSqlToRelConverterConfig(sqlToRelConfig)

    val tableConfig = tEnv.getConfig
    tableConfig.setPlannerConfig(plannerConfig.build())

    val envConfig = tableConfig.getConfiguration
    envConfig.setBoolean("pipeline.object-reuse", true)

    val startTime = System.currentTimeMillis
    tEnv.executeSql(
      s"""
         | insert OVERWRITE `t_hive_user_group_result` PARTITION(id = 'score_$leftBound-$rightBound', ts = $startTime)
         | SELECT MAP['uuid', t.uuid, 'score', t.score] AS uuid_map
         | FROM (
         |     SELECT uuid, f.score FROM `t_hbase_user_score_result`
         |     WHERE f.score between $leftBound and $rightBound and f.user_type <> '3'
         |     order by f.score desc
         | ) t
         |""".stripMargin)
  }
}
