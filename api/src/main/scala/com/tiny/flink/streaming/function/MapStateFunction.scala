package com.tiny.flink.streaming.function

import java.lang.Thread.sleep
import java.time.{LocalDate, ZoneId}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

class MapStateFunction extends RichMapFunction[(String, Int), (String, Int)] {

  private[this] var state: MapState[String, Int] = _

  override def map(value: (String, Int)): (String, Int) = {
    val tmp = state
    val count = tmp.get(value._1) + 1
    tmp.put(value._1, count)
    val now = LocalDate.now(ZoneId.of("+8"));
    tmp.put("year", now.getYear)
    tmp.put("month", now.getMonthValue)
    tmp.put("day", now.getDayOfMonth)
    (value._1, count)
  }

  override def open(parameters: Configuration): Unit = {
    val descriptor = new MapStateDescriptor[String, Int]("container",
      TypeInformation.of(new TypeHint[String] {}),
      TypeInformation.of(new TypeHint[Int] {}))
    state = getRuntimeContext.getMapState(descriptor)
    // print action
    new Thread() {
      override def run(): Unit = {
        println(state)
        while (true) {
          sleep(6000)
          println(null == state.entries)
          // TODO , only current kv pair can be seen
          state.entries.foreach(println)
        }
      }
    }.start()
  }

  override def close(): Unit = {
    state.clear()
  }
}

object MapStateFunction {
  def apply(): MapStateFunction = new MapStateFunction()
}
