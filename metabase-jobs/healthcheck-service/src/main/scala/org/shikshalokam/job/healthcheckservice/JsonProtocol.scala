package org.shikshalokam.job.healthcheckservice

import spray.json._

object JsonProtocol extends DefaultJsonProtocol {
  implicit val flinkJobStatusFormat  = jsonFormat2(FlinkJobStatus)
  implicit val flinkClusterHealthFmt = jsonFormat4(FlinkClusterHealth)
  implicit val flinkHealthFmt        = jsonFormat2(FlinkHealth)
  implicit val kafkaHealthFmt        = jsonFormat2(KafkaHealth)
  implicit val metabaseHealthFmt     = jsonFormat2(MetabaseHealth)
  implicit val fullResponseFmt       = jsonFormat4(FullHealthResponse)
}
