package org.shikshalokam.job.healthcheckservice

import com.typesafe.config.{ConfigFactory, Config => TConfig}

case class FlinkConfig(restApiUrl: String, jobs: List[String])
case class KafkaConfig(broker: String)
case class MetabaseConfig(url: String)

case class AppConfig(
                      flink: FlinkConfig,
                      kafka: KafkaConfig,
                      metabase: MetabaseConfig
                    )

object AppConfigLoader {

  def load(): AppConfig = {
    val conf: TConfig = ConfigFactory.load()

    val flink = FlinkConfig(
      restApiUrl = conf.getString("services.flink.rest-api-url"),
      jobs = conf.getStringList("services.flink.jobs").toArray.map(_.toString).toList
    )

    val kafka = KafkaConfig(
      broker = conf.getString("services.kafka.broker-list")
    )

    val metabase = MetabaseConfig(
      url = conf.getString("services.metabase.url")
    )

    AppConfig(flink, kafka, metabase)
  }
}
