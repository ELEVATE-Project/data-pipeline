package org.shikshalokam.job.combined.dashboard.creator.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.combined.dashboard.creator.domain.MentoringEvent
import org.shikshalokam.job.combined.dashboard.creator.fixture.MentoringEventsMock
import org.shikshalokam.job.util.JSONUtil

class MentoringMetabaseEventSource extends SourceFunction[MentoringEvent] {

  override def run(ctx: SourceContext[MentoringEvent]): Unit = {
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.EVENT1), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.EVENT2), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.EVENT3), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.SYNC_FILTER_1), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.SYNC_FILTER_2), 0, 0))
    ctx.collect(new MentoringEvent(JSONUtil.deserialize[java.util.Map[String, Any]](MentoringEventsMock.SYNC_FILTER_3), 0, 0))
  }

  override def cancel(): Unit = {}
}
