package org.shikshalokam.observation.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.observation.dashboard.creator.fixture.EventsMock
import org.shikshalokam.job.observation.dashboard.creator.domain.Event
import org.shikshalokam.job.util.JSONUtil


class ObservationMetabaseEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.CUSTOMIZED_FILTER_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.METABASE_DASHBOARD_EVENT_3), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT_2), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_FILTER_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_FILTER_EVENT_2), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_FILTER_EVENT_3), 0, 0))
  }

  override def cancel(): Unit = {}


}
