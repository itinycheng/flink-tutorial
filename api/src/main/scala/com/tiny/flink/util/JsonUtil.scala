package com.tiny.flink.util

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

object JsonUtil {

  private val MAPPER = new ObjectMapper()
  MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS)

  @throws[Exception]
  def writeValueAsString(obj: Any): String = MAPPER.writeValueAsString(obj)
}
