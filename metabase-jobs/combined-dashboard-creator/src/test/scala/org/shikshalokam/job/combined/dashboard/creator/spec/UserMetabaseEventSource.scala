package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.domain.UserEvent
import org.shikshalokam.job.combined.dashboard.creator.fixture.UserEventsMock
import org.shikshalokam.job.util.JSONUtil

class UserMetabaseEventSource extends SourceFunction[UserEvent] {

  override def run(ctx: SourceContext[UserEvent]): Unit = {
//    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.TENANT1), 0, 0))
    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.TENANT2), 0, 0))
//    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.SYNC_FILTER_1), 0, 0))
//    ctx.collect(new UserEvent(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.SYNC_FILTER_2), 0, 0))
  }

  override def cancel(): Unit = {}
}
