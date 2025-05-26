package org.shikshalokam.job.user.service.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.user.service.domain.Event

class UserServiceConfig(override val config: Config) extends BaseJobConfig(config, "UserManagement") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // Parallelism
  val mlUserServiceParallelism: Int = config.getInt("task.sl.user.service.parallelism")

  // Consumers
  val userServiceProducer: String = "user-service-consumer"

  // Functions
  val userServiceFunction: String = "UserServiceFunction"

  // Metabase Dashboard submissions job metrics
  val userServiceCleanupHit = "user-service-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-metabase-dashboard-events-count"

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val solutions: String = config.getString("postgres.tables.solutionsTable")
  val dashboard_metadata: String = config.getString("postgres.tables.dashboardMetadataTable")
  val report_config: String = config.getString("postgres.tables.reportConfigTable")

  // Metabase connection config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  val metabaseDatabase: String = config.getString("metabase.database")

  // Domain static name & password
  val domainName: String = config.getString("domain.name")
  val domainPassword: String = config.getString("domain.password")

}
