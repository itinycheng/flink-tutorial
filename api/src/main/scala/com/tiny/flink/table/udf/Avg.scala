package com.tiny.flink.table.udf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext}

/**
 * UDAF
 **/
class Avg extends AggregateFunction[Long, AvgAccumulator] {

  override def open(context: FunctionContext): Unit = println("open.")

  override def close(): Unit = println("close.")

  override def getValue(accumulator: AvgAccumulator): Long =
    if (accumulator.count == 0) 0 else accumulator.sum / accumulator.count

  override def createAccumulator(): AvgAccumulator = new AvgAccumulator

  override def getResultType: TypeInformation[Long] = Types.of[Long]

  override def getAccumulatorType: TypeInformation[AvgAccumulator] =
    Types.POJO(classOf[AvgAccumulator], Map(("sum", Types.of[Long]), ("count", Types.of[Int])))

  /**
   * adder
   **/
  def accumulate(cached: AvgAccumulator, value: Long): Unit = {
    cached.sum += value
    cached.count += 1
  }

  /**
   * retract
   */
  def retract(cached: AvgAccumulator, value: Long): Unit = {
    cached.sum -= value
    cached.count -= 1
  }

  /**
   * reset
   */
  def resetAccumulator(cached: AvgAccumulator): Unit = new AvgAccumulator

}

class AvgAccumulator(var sum: Long = 0L, var count: Int = 0) extends Serializable