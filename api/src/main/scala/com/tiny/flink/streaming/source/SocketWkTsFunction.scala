package com.tiny.flink.streaming.source

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, Socket}

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @author tiny.wang
  */
class SocketWkTsFunction(hostname: String, port: Int, delimiter: String, maxNumRetries: Long) extends RichSourceFunction[String] {

  private val isRunning = true

  private var currentSocket: Socket = _

  def this(hostname: String, port: Int) = this(hostname, port, "\r", 3)

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val socket = new Socket
    currentSocket = socket
    socket.connect(new InetSocketAddress(hostname, port), 0)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    while (isRunning) {
      val ts = System.currentTimeMillis
      val in = reader.readLine
      ctx.collectWithTimestamp(in, ts)
      if ((math.random * 2).toInt == 0)
        ctx.emitWatermark(new Watermark(ts))
    }
  }

  override def cancel(): Unit = {
    currentSocket.close()
  }
}

object SocketWkTsFunction {
  def apply(hostname: String, port: Int): SocketWkTsFunction = new SocketWkTsFunction(hostname, port)
}
