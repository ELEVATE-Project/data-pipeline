package org.shikshalokam.job.healthcheck.service.models

case class FlinkJobStatus(name: String, status: String)
case class FlinkClusterHealth(status: String, taskmanagers: Int, slotsTotal: Int, slotsAvailable: Int)
case class FlinkHealth(cluster: FlinkClusterHealth, jobs: List[FlinkJobStatus])

case class KafkaHealth(status: String, broker: String)
case class MetabaseHealth(status: String, url: String)

case class FullHealthResponse(flink: FlinkHealth, kafka: KafkaHealth, metabase: MetabaseHealth, timestamp: String)
