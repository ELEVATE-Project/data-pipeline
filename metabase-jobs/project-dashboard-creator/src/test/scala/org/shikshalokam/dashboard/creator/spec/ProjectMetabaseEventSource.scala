package org.shikshalokam.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.dashboard.creator.fixture.EventsMock
import org.shikshalokam.job.dashboard.creator.domain.Event
import org.shikshalokam.job.util.JSONUtil


class ProjectMetabaseEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.METABASE_DASHBOARD_EVENT_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DASHBOARD_EVENT_S1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DASHBOARD_EVENT_S2), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DASHBOARD_EVENT_S3), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DASHBOARD_EVENT_S4), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DASHBOARD_EVENT_S5), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.TEST_EVENT_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.UPDATE_FILTER_DATA_EVENT), 0, 0))
  }

  override def cancel(): Unit = {}


}
