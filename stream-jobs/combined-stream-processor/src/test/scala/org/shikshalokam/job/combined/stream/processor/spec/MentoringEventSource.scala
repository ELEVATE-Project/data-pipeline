package org.shikshalokam.job.combined.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.stream.processor.domain.MentoringEvent
import org.shikshalokam.job.combined.stream.processor.fixture.MentoringEventMock
import org.shikshalokam.job.util.JSONUtil

class MentoringEventSource extends SourceFunction[MentoringEvent] {

  override def run(ctx: SourceContext[MentoringEvent]): Unit = {
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.SESSION_CREATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.SESSION_UPDATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.SESSION_DELETE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.SESSION_ATTENDANCE_CREATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.SESSION_ATTENDANCE_UPDATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.ORG_MENTOR_RATING_CREATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.ORG_MENTOR_RATING_UPDATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.CONNECTIONS_CREATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.CONNECTIONS_UPDATE), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventMock.CONNECTIONS_DELETE), 0, 0))
  }

  override def cancel(): Unit = {}

}