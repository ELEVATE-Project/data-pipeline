package org.shikshalokam.user.service.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.service.fixture.ProgramEventsMock

class ProgramServiceEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    println("INSIDE Program Service Event Source")
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](ProgramEventsMock.PROGRAM_CREATE_EVENT_WITH_EMAIL), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](ProgramEventsMock.PROGRAM_CREATE_EVENT_WITHOUT_EMAIL), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](ProgramEventsMock.PROGRAM_DELETE_EVENT_WITH_EMAIL), 0, 0))
  }

  override def cancel(): Unit = {}

}
