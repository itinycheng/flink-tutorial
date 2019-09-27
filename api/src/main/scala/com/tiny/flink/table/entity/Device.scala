package com.tiny.flink.table.entity

import com.tiny.flink.util.JsonUtil

import scala.beans.BeanProperty


case class Device(@BeanProperty deviceId: String, @BeanProperty offset: Long, @BeanProperty startTime: Long) {

  override def toString: String = try JsonUtil.writeValueAsString(this) catch { case _: Throwable => ""}

}