package com.tiny.flink.streaming

import com.tiny.flink.streaming.function.WordSplitFunction
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
  * This example shows an implementation of WordCount with data from a text socket.
  * To run the example make sure that the service providing the text data is already up and running.
  *
  * To start an example socket text stream on your local machine run netcat from a command line,
  * where the parameter specifies the port number:
  *
  * {{{
  *   nc -lk 9999
  * }}}
  *
  * Usage:
  * {{{
  *   SocketTextStreamWordCount <hostname> <port>
  * }}}
  *
  * This example shows how to:
  *
  *   - use StreamExecutionEnvironment.socketTextStream
  *   - write a simple Flink Streaming program in scala
  *   - write and use user-defined functions
  */
object WordCount0 {


  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("USAGE:\nWordCount0 <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt
    // TODO: max maxBy
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    setEnvConf(env)

    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap(WordSplitFunction())
      .filter(_.nonEmpty)
      .map((_, (math.random * 10).toInt))
      .keyBy(0)
      .max(1)

    counts.print
    env.execute("""Scala WordCount0 from SocketTextStream Example""")
  }

  def setEnvConf(env: StreamExecutionEnvironment): Unit = {
    env.setParallelism(1)
    env.setMaxParallelism(10)
    env.getConfig.disableForceKryo()
    env.getConfig.enableForceAvro()
    env.getConfig.setAutoWatermarkInterval(3000)
    env.setStateBackend(new MemoryStateBackend(100*1024*1024,false))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(10000)
    env.getCheckpointConfig.setCheckpointTimeout(8000)
  }
}
