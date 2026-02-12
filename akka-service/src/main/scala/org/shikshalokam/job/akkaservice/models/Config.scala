package org.shikshalokam.job.akkaservice.models

case class FlinkConfig(restApiUrl: String, jobs: List[String])
case class KafkaConfig(broker: String)
case class MetabaseConfig(url: String)
case class SecurityConfig(apiToken: String)

case class AppConfig(flink: FlinkConfig, kafka: KafkaConfig, metabase: MetabaseConfig, security: SecurityConfig)
