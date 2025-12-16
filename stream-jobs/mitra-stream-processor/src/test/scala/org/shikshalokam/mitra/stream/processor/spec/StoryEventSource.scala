package org.shikshalokam.mitra.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.mitra.stream.processor.domain.StoryEvent
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.mitra.stream.processor.fixture.StoryEventMock

class StoryEventSource extends SourceFunction[StoryEvent] {

  override def run(ctx: SourceContext[StoryEvent]): Unit = {
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SURVEY_EVENT_STARTED), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SURVEY_EVENT_INPROGRESS), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SURVEY_EVENT_COMPLETED), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SAAS_QA_DATA_EVENT_1), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SAAS_QA_DATA_EVENT_2), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.SAAS_QA_DATA_EVENT_3), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.MULTISOLUTION_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}