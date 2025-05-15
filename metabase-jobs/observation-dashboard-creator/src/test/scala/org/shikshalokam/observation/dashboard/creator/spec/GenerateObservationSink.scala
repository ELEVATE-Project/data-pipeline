package org.shikshalokam.observation.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.util


class GenerateMetabaseDashboardSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      println(value)
      GenerateMetabaseDashboardSink.values.add(value)
    }
  }
}

object GenerateMetabaseDashboardSink {
  val values: util.List[String] = new util.ArrayList()
}
