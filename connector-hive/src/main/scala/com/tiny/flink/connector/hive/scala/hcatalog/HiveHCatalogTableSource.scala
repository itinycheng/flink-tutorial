package com.tiny.flink.connector.hive.scala.hcatalog

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.hcatalog.scala.HCatInputFormat
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import scala.collection.mutable

/**
  * @author tiny.wang
  */
class HiveHCatalogTableSource(
                               dbName: String,
                               tableName: String,
                               fieldNames: Array[String],
                               fieldTypes: Array[TypeInformation[_]],
                               selectedFields: Array[Int],
                               configuration: Configuration)
  extends BatchTableSource[Row]
    with StreamTableSource[Row] {

  def this(dbName: String,
           tableName: String,
           fieldNames: Array[String],
           fieldTypes: Array[TypeInformation[_]],
           configuration: Configuration) {
    this(dbName,
      tableName,
      fieldNames,
      fieldTypes,
      fieldNames.indices.toArray,
      configuration)
  }

  private val selectedFieldNames = selectedFields.map(fieldNames(_))

  private val selectedFieldTypes = selectedFields.map(fieldTypes(_))

  private val returnType = new RowTypeInfo(selectedFieldTypes, selectedFieldNames)

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.createInput(createHCatInput(), returnType).name(explainSource())
  }

  override def getReturnType: RowTypeInfo = returnType

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)

  override def explainSource(): String = {
    s"HiveHCatalogTableSource(read fields: $getReturnType)"
  }

  override def getDataStream(streamExecEnv: StreamExecutionEnvironment): DataStream[Row] = {
    streamExecEnv.createInput(createHCatInput(), returnType).name(explainSource())
  }

  def createHCatInput(): HCatInputFormat[Row] = {
    new HCatInputFormat[Row](dbName, tableName, configuration)
  }
}

object HiveHCatalogTableSource {

  class Builder {

    private val schema: mutable.LinkedHashMap[String, TypeInformation[_]] =
      mutable.LinkedHashMap[String, TypeInformation[_]]()
    private var database: String = _
    private var table: String = _
    private var metastoreUris: String = _

    def database(databaseName: String): Builder = {
      this.database = databaseName
      this
    }

    def table(tableName: String): Builder = {
      this.table = tableName
      this
    }

    def metastoreUris(metastoreUris: String): Builder = {
      this.metastoreUris = metastoreUris
      this
    }

    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName -> fieldType)
      this
    }

    def build(): HiveHCatalogTableSource = {
      if (table == null) {
        throw new IllegalArgumentException("table must be defined.")
      }
      if (database == null) {
        database = "default"
      }
      val hiveConf = new HiveConf
      if (metastoreUris != null && metastoreUris.nonEmpty) {
        hiveConf.set("hive.metastore.uris", metastoreUris)
      }

      new HiveHCatalogTableSource(
        database,
        table,
        schema.keys.toArray,
        schema.values.toArray,
        hiveConf)
    }
  }

  def builder(): Builder = new Builder
}
