package org.shikshalokam.user.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateUserSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateUserSink.values.add(value)
    }
  }
}

object GenerateUserSink {
  val values: util.List[String] = new util.ArrayList()
}