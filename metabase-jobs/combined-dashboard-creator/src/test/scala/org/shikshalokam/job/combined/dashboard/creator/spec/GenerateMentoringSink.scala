package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util

class GenerateMentoringSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      println(value)
      GenerateMentoringSink.values.add(value)
    }
  }
}

object GenerateMentoringSink {
  val values: util.List[String] = new util.ArrayList()
}
