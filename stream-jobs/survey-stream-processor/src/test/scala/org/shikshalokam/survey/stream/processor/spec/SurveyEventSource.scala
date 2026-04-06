package org.shikshalokam.survey.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.survey.stream.processor.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.survey.stream.processor.fixture.EventsMock

class SurveyEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SURVEY_EVENT_STARTED), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SURVEY_EVENT_INPROGRESS), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SURVEY_EVENT_COMPLETED), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DATA_EVENT_1), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DATA_EVENT_2), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.SAAS_QA_DATA_EVENT_3), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.MULTISOLUTION_EVENT_2), 0, 0))
  }

  override def cancel(): Unit = {}

}