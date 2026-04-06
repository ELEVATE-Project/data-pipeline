package org.shikshalokam.observation.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.observation.stream.processor.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.observation.stream.processor.fixture.EventsMock

class ObservationEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.EVENT_FROM_QA_ENV_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.EVENT_FROM_QA_ENV_2), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CUSTOMIZED_FILTER_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}