package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.fixture.ProgramEventsMock
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent
import org.shikshalokam.job.util.JSONUtil

class ProgramServiceEventSource extends SourceFunction[UserMappingEvent] {

  override def run(ctx: SourceContext[UserMappingEvent]): Unit = {
    ctx.collect(new UserMappingEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProgramEventsMock.PROGRAM_CREATE_EVENT_WITH_USERNAME), 0, 0))
    ctx.collect(new UserMappingEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProgramEventsMock.PROGRAM_DELETE_EVENT_WITH_USERNAME), 0, 0))
  }

  override def cancel(): Unit = {}

}
