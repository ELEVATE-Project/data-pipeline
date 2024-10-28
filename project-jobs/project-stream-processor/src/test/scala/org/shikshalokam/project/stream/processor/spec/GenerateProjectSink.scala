package org.shikshalokam.project.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateProjectSinkSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateProjectSinkSink.values.add(value)
    }
  }
}

object GenerateProjectSinkSink {
  val values: util.List[String] = new util.ArrayList()
}