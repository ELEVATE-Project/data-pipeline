package org.shikshalokam.mitra.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.mitra.stream.processor.domain.StoryEvent
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.mitra.stream.processor.fixture.StoryEventMock

class StoryEventSource extends SourceFunction[StoryEvent] {

  override def run(ctx: SourceContext[StoryEvent]): Unit = {
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_1), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_2), 0, 0))
    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_3), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_4), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_5), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_6), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_7), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_8), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_9), 0, 0))
//    ctx.collect(new StoryEvent(JSONUtil.deserialize[java.util.Map[String, Any]](StoryEventMock.EVENT_10), 0, 0))
  }

  override def cancel(): Unit = {}

}