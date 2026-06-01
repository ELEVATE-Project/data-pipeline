package org.shikshalokam.job.akkaservice.models

import spray.json._


// --- Health Models ---
case class FlinkJobStatus(name: String, status: String)

case class FlinkClusterHealth(status: String, taskmanagers: Int, slotsTotal: Int, slotsAvailable: Int, jobsRunning: Int, jobsFinished: Int, jobsCancelled: Int, jobsFailed: Int, flinkVersion: String)

case class FlinkHealth(cluster: FlinkClusterHealth, jobs: List[FlinkJobStatus])

case class KafkaHealth(status: String, broker: String)

case class MetabaseHealth(status: String, url: String)

case class FullHealthResponse(flink: FlinkHealth, kafka: KafkaHealth, metabase: MetabaseHealth, timestamp: String)

// --- Json Protocol ---
object JsonProtocol extends DefaultJsonProtocol {
  implicit val flinkJobStatusFormat: RootJsonFormat[FlinkJobStatus] = jsonFormat2(FlinkJobStatus)
  implicit val flinkClusterHealthFmt: RootJsonFormat[FlinkClusterHealth] = jsonFormat9(FlinkClusterHealth)
  implicit val flinkHealthFmt: RootJsonFormat[FlinkHealth] = jsonFormat2(FlinkHealth)
  implicit val kafkaHealthFmt: RootJsonFormat[KafkaHealth] = jsonFormat2(KafkaHealth)
  implicit val metabaseHealthFmt: RootJsonFormat[MetabaseHealth] = jsonFormat2(MetabaseHealth)
  implicit val fullResponseFmt: RootJsonFormat[FullHealthResponse] = jsonFormat4(FullHealthResponse)
}
