package org.shikshalokam.mitra.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateDiscussionSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateDiscussionSink.values.add(value)
    }
  }
}

object GenerateDiscussionSink {
  val values: util.List[String] = new util.ArrayList()
}