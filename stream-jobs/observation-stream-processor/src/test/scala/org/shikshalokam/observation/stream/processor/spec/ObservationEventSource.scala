package org.shikshalokam.obseravtion.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.observation.stream.processor.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.observation.stream.processor.fixture.EventsMock

class ObservationEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.OBS_ENTITY_INFO_CHANGE_EVENT_SUB_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.OBS_ENTITY_INFO_CHANGE_EVENT_SUB_2), 0, 0))
  }

  override def cancel(): Unit = {}

}