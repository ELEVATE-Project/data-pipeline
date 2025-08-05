package org.shikshalokam.user.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.user.dashboard.creator.fixture.EventsMock
import org.shikshalokam.job.user.dashboard.creator.domain.Event
import org.shikshalokam.job.util.JSONUtil

class UserMetabaseEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAMPLE_EVENT1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAMPLE_EVENT), 0, 0))
  }

  override def cancel(): Unit = {}


}
