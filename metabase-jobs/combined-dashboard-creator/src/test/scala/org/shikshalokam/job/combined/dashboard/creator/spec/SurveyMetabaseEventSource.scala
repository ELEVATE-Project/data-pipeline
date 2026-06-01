package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.domain.SurveyEvent
import org.shikshalokam.job.combined.dashboard.creator.fixture.SurveyEventsMock
import org.shikshalokam.job.util.JSONUtil

class SurveyMetabaseEventSource extends SourceFunction[SurveyEvent] {

  override def run(ctx: SourceContext[SurveyEvent]): Unit = {
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.METABASE_DASHBOARD_EVENT), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.METABASE_DASHBOARD_EVENT_WITHOUT_ADMIN), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.METABASE_DASHBOARD_EVENT_1_WITHOUT_PROGRAM), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.UPDATE_FILTER_DATA_EVENT), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventsMock.UPDATE_FILTER_DATA_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}
