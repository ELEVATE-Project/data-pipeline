package org.shikshalokam.project.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.project.creator.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.project.dashboard.creator.fixture.EventsMock


class ProjectEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.PROJECT_EVENT_1), 0, 0))
  }

  override def cancel(): Unit = {}


}
