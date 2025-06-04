package org.shikshalokam.user.service.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.util

class GenerateUserServiceSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      println(value)
      GenerateUserServiceSink.values.add(value)
    }
  }
}

object GenerateUserServiceSink {
  val values: util.List[String] = new util.ArrayList()
}
