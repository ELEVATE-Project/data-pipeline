package org.shikshalokam.job.combined.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.combined.stream.processor.fixture.ObservationEventMock
import org.shikshalokam.job.util.JSONUtil

class ObservationEventSource extends SourceFunction[ObservationEvent] {

  override def run(ctx: SourceContext[ObservationEvent]): Unit = {
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventMock.EVENT_FROM_QA_ENV_1), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventMock.EVENT_FROM_QA_ENV_2), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventMock.CUSTOMIZED_FILTER_EVENT), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventMock.MULTISOLUTION_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}