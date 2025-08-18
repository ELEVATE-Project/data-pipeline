package org.shikshalokam.job.user.dashboard.creator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.domain.reader.Event


class UserMetabaseDashboardConfig(override val config: Config) extends BaseJobConfig(config, "MetabaseUserDashboardJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // Parallelism
  val mlMetabaseParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val metabaseDashboardProducer: String = "metabase-dashboard-consumer"

  // Functions
  val metabaseDashboardFunction: String = "UserMetabaseDashboardFunction"

  // Metabase Dashboard submissions job metrics
  val metabaseDashboardCleanupHit: String = "metabase-dashboard-cleanup-hit"
  val skipCount: String = "skipped-message-count"
  val successCount: String = "success-message-count"
  val totalEventsCount: String = "total-metabase-dashboard-events-count"

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val dashboardMetadata: String = config.getString("postgres.tables.dashboardMetadataTable")
  val userMetrics: String = config.getString("postgres.tables.userMetricsTable")
  val reportConfig: String = config.getString("postgres.tables.reportConfigTable")
  val metabasePgDatabase: String = config.getString("postgres.metabaseDb")

  // Metabase connection config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  val metabaseDatabase: String = config.getString("metabase.database")
  val metabaseApiKey: String = config.getString("metabase.metabaseApiKey")
}
