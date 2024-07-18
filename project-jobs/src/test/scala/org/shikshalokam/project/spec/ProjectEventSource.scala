package org.shikshalokam.project.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.project.fixture.EventsMock

class ProjectEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
//    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.TEST_EVENT), 0, 0))
    ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventsMock.PROJECT_EVENT), 0, 0))
}

  override def cancel(): Unit = {}

}