package org.shikshalokam.user.service.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.service.fixture.EventsMock

class UserServiceEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CREATE_EVENT_WITH_EMAIL_AND_PHONE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.BULK_CREATE_EVENT_WITH_EMAIL_AND_PHONE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CREATE_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.BULK_CREATE_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.BULK_UPDATE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_EVENT_REPORT_ADMIN_ROLE_IN_OLD_VALUE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.DELETE_USER_WITH_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.DELETE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.KAFKA_NOTIFICATION_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.API_NOTIFICATION_EVENT_WITHOUT_EMAIL), 0, 0))
  }

  override def cancel(): Unit = {}

}
