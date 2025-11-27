package org.shikshalokam.job.healthcheck.service.models

case class FlinkConfig(restApiUrl: String, jobs: List[String])
case class KafkaConfig(broker: String)
case class MetabaseConfig(url: String)

case class AppConfig(flink: FlinkConfig, kafka: KafkaConfig, metabase: MetabaseConfig)
