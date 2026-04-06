package org.shikshalokam.job.combined.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.stream.processor.domain.SurveyEvent
import org.shikshalokam.job.combined.stream.processor.fixture.SurveyEventMock
import org.shikshalokam.job.util.JSONUtil

class SurveyEventSource extends SourceFunction[SurveyEvent] {

  override def run(ctx: SourceContext[SurveyEvent]): Unit = {
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SURVEY_EVENT_STARTED), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SURVEY_EVENT_INPROGRESS), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SURVEY_EVENT_COMPLETED), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SAAS_QA_DATA_EVENT_1), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SAAS_QA_DATA_EVENT_2), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.SAAS_QA_DATA_EVENT_3), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new SurveyEvent(JSONUtil.deserialize[java.util.Map[String, Any]](SurveyEventMock.MULTISOLUTION_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}