package org.shikshalokam.user.mapping.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.mapping.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.mapping.stream.processor.fixture.ObservationEventsMock

class ObservationEventSource extends SourceFunction[ObservationEvent] {

  override def run(ctx: SourceContext[ObservationEvent]): Unit = {
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.OBSERVATION_SUBMITTED), 0, 0))
  }

  override def cancel(): Unit = {}

}
