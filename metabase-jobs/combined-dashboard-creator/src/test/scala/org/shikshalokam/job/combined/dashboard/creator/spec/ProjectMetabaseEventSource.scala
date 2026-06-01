package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.domain.ProjectEvent
import org.shikshalokam.job.combined.dashboard.creator.fixture.ProjectEventsMock
import org.shikshalokam.job.util.JSONUtil


class ProjectMetabaseEventSource extends SourceFunction[ProjectEvent] {

  override def run(ctx: SourceContext[ProjectEvent]): Unit = {
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.METABASE_DASHBOARD_EVENT_1), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.SAAS_QA_DASHBOARD_EVENT_S1), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.SAAS_QA_DASHBOARD_EVENT_S2), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.SAAS_QA_DASHBOARD_EVENT_S3), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.SAAS_QA_DASHBOARD_EVENT_S4), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.SAAS_QA_DASHBOARD_EVENT_S5), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.MULTISOLUTION_EVENT_1), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.TEST_EVENT_1), 0, 0))
//    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.UPDATE_FILTER_DATA_EVENT), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.TEST_STATE_EVENT_1), 0, 0))
    ctx.collect(new ProjectEvent(JSONUtil.deserialize[java.util.Map[String, Any]](ProjectEventsMock.TEST_DISTRICT_EVENT_1), 0, 0))
  }

  override def cancel(): Unit = {}


}
