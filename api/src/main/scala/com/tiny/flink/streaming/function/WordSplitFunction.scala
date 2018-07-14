package com.tiny.flink.streaming.function

import org.apache.flink.api.common.functions.{AbstractRichFunction, FlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class WordSplitFunction extends AbstractRichFunction with FlatMapFunction[String, String] {

  override def open(parameters: Configuration): Unit = println("open")

  override def close(): Unit = println("close")

  override def flatMap(value: String, out: Collector[String]): Unit = {
    Option(value) match {
      case Some(s) => s.toUpperCase.split("\\W+").foreach(out.collect)
      case _ =>
    }
  }

}

object WordSplitFunction {

  def apply(): WordSplitFunction = new WordSplitFunction

}

