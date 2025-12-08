package org.shikshalokam.job.combined.dashboard.creator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.combined.dashboard.creator.domain._
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent

class CombinedDashboardCreatorConfig(override val config: Config) extends BaseJobConfig(config, "DashboardCreatorJob") {

  implicit val mentoringEventInfo: TypeInformation[MentoringEvent] = TypeExtractor.getForClass(classOf[MentoringEvent])
  implicit val observationEventInfo: TypeInformation[ObservationEvent] = TypeExtractor.getForClass(classOf[ObservationEvent])
  implicit val projectEventInfo: TypeInformation[ProjectEvent] = TypeExtractor.getForClass(classOf[ProjectEvent])
  implicit val surveyEventInfo: TypeInformation[SurveyEvent] = TypeExtractor.getForClass(classOf[SurveyEvent])
  implicit val userEventInfo: TypeInformation[UserEvent] = TypeExtractor.getForClass(classOf[UserEvent])
  implicit val userServiceEventInfo: TypeInformation[UserMappingEvent] = TypeExtractor.getForClass(classOf[UserMappingEvent])

  // Kafka Topics Configuration
  val mentoringInputTopic: String = config.getString("kafka.input.mentoring.topic")
  val observationInputTopic: String = config.getString("kafka.input.observation.topic")
  val projectInputTopic: String = config.getString("kafka.input.project.topic")
  val surveyInputTopic: String = config.getString("kafka.input.survey.topic")
  val userInputTopic: String = config.getString("kafka.input.user.topic")
  val userServiceInputTopic: String = config.getString("kafka.input.userservice.topic")
  val programServiceInputTopic: String = config.getString("kafka.input.programservice.topic")
  val notificationOutputTopic: String = config.getString("kafka.output.notification.topic")

  val userServiceOutputTag: OutputTag[String] = OutputTag[String]("user-service-output-event")

  // Parallelism
  override val parallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Metrics
  val metabaseDashboardCleanupHit: String = "metabase-dashboard-cleanup-hit"
  val skipCount: String = "skipped-message-count"
  val successCount: String = "success-message-count"
  val totalEventsCount: String = "total-metabase-dashboard-events-count"

  // Metabase Dashboard submissions job metrics
  val userServiceCleanupHit = "user-service-cleanup-hit"
  val programServiceCleanupHit = "program-service-cleanup-hit"

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val metabasePgDatabase: String = config.getString("postgres.metabaseDb")

  // Postgres Tables
  val solutions: String = config.getString("postgres.tables.solutionsTable")
  val projects: String = config.getString("postgres.tables.projectsTable")
  val tasks: String = config.getString("postgres.tables.tasksTable")
  val dashboard_metadata: String = config.getString("postgres.tables.dashboardMetadataTable")
  val dashboardMetadata: String = dashboard_metadata
  val report_config: String = config.getString("postgres.tables.reportConfigTable")
  val reportConfig: String = report_config
  val userMetrics: String = config.getString("postgres.tables.userMetricsTable")

  // Metabase Config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  val metabaseDatabase: String = config.getString("metabase.database")
  val metabaseDomainName: String = config.getString("metabase.domainName")
  val evidenceBaseUrl: String = config.getString("metabase.evidenceBaseUrl")
  val metabaseApiKey: String = config.getString("metabase.metabaseApiKey")
  val metabaseKey: String = metabaseApiKey

  // User Service specific configurations
  val domainName: String = config.getString("domain.name")
  
  // Notification config
  val notificationType: String = config.getString("notify.type")
  val notificationApiUrl: String = config.getString("notify.api.url")
  val notificationEmailTemplate: String = config.getString("notify.email.template")
  val notificationSmsTemplate: String = config.getString("notify.sms.template")
}
