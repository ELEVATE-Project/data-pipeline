package org.shikshalokam.job.project.creator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.shikshalokam.job.project.creator.domain.Event
import org.shikshalokam.job.BaseJobConfig


class ProjectDashboardConfig(override val config: Config) extends BaseJobConfig(config, "ProjectsDashboardJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // Parallelism
  val mlProjectsParallelism: Int = config.getInt("task.sl.projects.dashboard.parallelism")

  // Consumers
  val metabaseDashboardProducer: String = "project-dashboard-consumer"

  // Functions
  val projectsDashboardFunction: String = "ProjectDashboardFunction"

  // Project submissions job metrics
  val projectsCleanupHit = "project-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-project-events-count"

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")

  // Metabase connection config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  //val metabaseDatabase: Int = config.getInt("metabase.database")

}
