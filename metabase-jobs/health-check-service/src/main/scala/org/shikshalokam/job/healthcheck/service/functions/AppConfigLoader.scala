package org.shikshalokam.job.healthcheck.service.functions

import com.typesafe.config.{ConfigFactory, Config => TConfig}
import org.shikshalokam.job.healthcheck.service.models.{AppConfig, FlinkConfig, KafkaConfig, MetabaseConfig}

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
