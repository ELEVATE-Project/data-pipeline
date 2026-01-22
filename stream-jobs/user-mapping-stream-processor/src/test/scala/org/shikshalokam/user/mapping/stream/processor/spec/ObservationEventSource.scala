package org.shikshalokam.user.mapping.stream.processor.spec

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.shikshalokam.job.user.mapping.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.util.JSONUtil
import org.shikshalokam.user.mapping.stream.processor.fixture.ObservationEventsMock

class ObservationEventSource extends SourceFunction[ObservationEvent] {

  override def run(ctx: SourceContext[ObservationEvent]): Unit = {
    val filePath = "/home/ttpl-rt-221/elevate/data-pipeline/stream-jobs/user-mapping-stream-processor/obs_kafka_response.json"
    val source = scala.io.Source.fromFile(filePath)
    val jsonContent = try source.mkString finally source.close()
    
    ctx.collect(new ObservationEvent(JSONUtil.deserialize[java.util.Map[String, Any]](jsonContent), 0, 0))
  }

  override def cancel(): Unit = {}

}
