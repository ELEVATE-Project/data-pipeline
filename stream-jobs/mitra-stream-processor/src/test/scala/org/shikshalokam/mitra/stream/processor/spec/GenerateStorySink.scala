package org.shikshalokam.mitra.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateStorySink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateStorySink.values.add(value)
    }
  }
}

object GenerateStorySink {
  val values: util.List[String] = new util.ArrayList()
}