package org.shikshalokam.user.service.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.service.fixture.UserEventsMock

class UserServiceEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    println("INSIDE User Service Event Source")
    //          ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_AND_PHONE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.BULK_CREATE_EVENT_WITH_EMAIL_AND_PHONE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.BULK_CREATE_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.UPDATE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.BULK_UPDATE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.UPDATE_EVENT_REPORT_ADMIN_ROLE_IN_OLD_VALUE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.DELETE_USER_WITH_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.DELETE_USER_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.KAFKA_NOTIFICATION_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.API_NOTIFICATION_EVENT_WITHOUT_EMAIL), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_FOR_STATE_MANAGER), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_FOR_DISTRICT_MANAGER), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_FOR_PROGRAM_MANAGER), 0, 0))
    //        ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_AND_PHONE_TENANT_ADMIN), 0, 0))
    //        ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.UPDATE_EVENT_TENANT_ADMIN_ROLE_IN_OLD_VALUE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.DELETE_USER_WITH_EMAIL_TENANT_ADMIN), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.CREATE_EVENT_WITH_EMAIL_AND_PHONE_ORG_ADMIN), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.UPDATE_EVENT_ORG_ADMIN_ROLE_IN_OLD_VALUE), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](UserEventsMock.DELETE_USER_WITH_EMAIL_ORG_ADMIN), 0, 0))
  }

  override def cancel(): Unit = {}

}
