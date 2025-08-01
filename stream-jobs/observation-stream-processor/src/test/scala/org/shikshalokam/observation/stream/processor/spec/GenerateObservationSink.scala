package org.shikshalokam.observation.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateObservationSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateProjectSink.values.add(value)
    }
  }
}

object GenerateProjectSink {
  val values: util.List[String] = new util.ArrayList()
}