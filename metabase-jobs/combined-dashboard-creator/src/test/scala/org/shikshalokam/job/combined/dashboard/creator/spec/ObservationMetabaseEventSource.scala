package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.domain.ObservationEvent
import org.shikshalokam.job.combined.dashboard.creator.fixture.ObservationEventsMock
import org.shikshalokam.job.util.JSONUtil


class ObservationMetabaseEventSource extends SourceFunction[ObservationEvent] {

  override def run(ctx: SourceContext[ObservationEvent]): Unit = {
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.CUSTOMIZED_FILTER_EVENT), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.METABASE_DASHBOARD_EVENT_3), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.MULTISOLUTION_EVENT_2), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.UPDATE_FILTER_EVENT), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.UPDATE_FILTER_EVENT_2), 0, 0))
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ObservationEventsMock.UPDATE_FILTER_EVENT_3), 0, 0))
  }

  override def cancel(): Unit = {}


}
