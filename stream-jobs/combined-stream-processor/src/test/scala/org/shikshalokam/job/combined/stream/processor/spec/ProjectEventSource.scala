package org.shikshalokam.job.combined.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.stream.processor.domain.ProjectEvent
import org.shikshalokam.job.combined.stream.processor.fixture.ProjectEventMock
import org.shikshalokam.job.util.JSONUtil

class ProjectEventSource extends SourceFunction[ProjectEvent] {

  override def run(ctx: SourceContext[ProjectEvent]): Unit = {
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.PROJECT_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.EVENT_FROM_BE), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.DEV_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.DEV_EVENT_2), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.QA_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.QA_EVENT_2), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.MULTISOLUTION_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.TEST_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventMock.TEST_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}
}