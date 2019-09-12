package com.tiny.flink.table.udf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.functions.{FunctionContext, TableFunction}

/**
 * UDTF
 **/
class Split extends TableFunction[(String, Int)] {

  override def close(): Unit = println("close.")

  def eval(separator: String, str: String): Unit = {
    str.split(separator).foreach(x => collect((x, x.length)))
  }

  // TODO how to use signature
  override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.foreach(println)
    Array(Types.of[String], Types.of[Long])
  }


  override def getResultType: TypeInformation[(String, Int)] = Types.TUPLE[(String, Int)]


  override def open(context: FunctionContext): Unit = println("open.")
}
