package org.shikshalokam.survey.stream.processor.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateSurveySink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized{
      println(value)
      GenerateSurveySink.values.add(value)
    }
  }
}

object GenerateSurveySink {
  val values: util.List[String] = new util.ArrayList()
}