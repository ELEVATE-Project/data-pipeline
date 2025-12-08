package org.shikshalokam.job.combined.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.stream.processor.domain.UserEvent
import org.shikshalokam.job.combined.stream.processor.fixture.UserEventMock
import org.shikshalokam.job.util.JSONUtil

class UserEventSource extends SourceFunction[UserEvent] {

  override def run(ctx: SourceContext[UserEvent]): Unit = {
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.CREATE), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.BULK_CREATE), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.CREATE_2), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.UPDATE), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.BULK_UPDATE), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventMock.DELETE), 0, 0))
}

  override def cancel(): Unit = {}

}