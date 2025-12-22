package org.shikshalokam.mitra.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.mitra.stream.processor.domain.DiscussionEvent
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.mitra.stream.processor.fixture.DiscussionEventMock

class DiscussionEventSource extends SourceFunction[DiscussionEvent] {

  override def run(ctx: SourceContext[DiscussionEvent]): Unit = {
    ctx.collect(new DiscussionEvent(JSONUtil.deserialize[java.util.Map[String, Any]](DiscussionEventMock.EVENT_1), 0, 0))
    ctx.collect(new DiscussionEvent(JSONUtil.deserialize[java.util.Map[String, Any]](DiscussionEventMock.EVENT_2), 0, 0))
    ctx.collect(new DiscussionEvent(JSONUtil.deserialize[java.util.Map[String, Any]](DiscussionEventMock.EVENT_3), 0, 0))
    ctx.collect(new DiscussionEvent(JSONUtil.deserialize[java.util.Map[String, Any]](DiscussionEventMock.EVENT_4), 0, 0))
  }

  override def cancel(): Unit = {}

}