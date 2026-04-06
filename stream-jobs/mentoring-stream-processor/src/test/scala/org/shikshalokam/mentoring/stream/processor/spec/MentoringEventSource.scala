package org.shikshalokam.mentoring.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.mentoring.stream.processor.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.mentoring.stream.processor.fixture.EventsMock

class MentoringEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {

    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SESSION_CREATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SESSION_UPDATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SESSION_DELETE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SESSION_ATTENDANCE_CREATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SESSION_ATTENDANCE_UPDATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.ORG_MENTOR_RATING_CREATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.ORG_MENTOR_RATING_UPDATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CONNECTIONS_CREATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CONNECTIONS_UPDATE), 0, 0))
    //    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CONNECTIONS_DELETE), 0, 0))
  }

  override def cancel(): Unit = {}

}