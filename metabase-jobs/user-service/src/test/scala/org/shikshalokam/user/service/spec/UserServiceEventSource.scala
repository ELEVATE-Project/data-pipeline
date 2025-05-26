package org.shikshalokam.user.service.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.service.fixture.EventsMock

class UserServiceEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.USER_CREATE_EVENT), 0, 0))
//    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.USER_DELETE_EVENT), 0, 0))
  }

  override def cancel(): Unit = {}

}
