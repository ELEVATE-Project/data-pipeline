package org.shikshalokam.mentoring.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util
import java.util.Collections

class GenerateMentoringSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    println(value)
    GenerateMentoringSink.values.add(value)
  }
}

object GenerateMentoringSink {
  val values: util.List[String] = Collections.synchronizedList(new util.ArrayList[String]())
}