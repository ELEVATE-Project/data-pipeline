package org.shikshalokam.job.combined.dashboard.creator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.combined.dashboard.creator.domain._
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent

class CombinedDashboardCreatorConfig(override val config: Config) extends BaseJobConfig(config, "CombinedDashboardCreatorJob") {

  implicit val mentoringEventInfo: TypeInformation[MentoringEvent] = TypeExtractor.getForClass(classOf[MentoringEvent])
  implicit val observationEventInfo: TypeInformation[ObservationEvent] = TypeExtractor.getForClass(classOf[ObservationEvent])
  implicit val projectEventInfo: TypeInformation[ProjectEvent] = TypeExtractor.getForClass(classOf[ProjectEvent])
  implicit val surveyEventInfo: TypeInformation[SurveyEvent] = TypeExtractor.getForClass(classOf[SurveyEvent])
  implicit val userEventInfo: TypeInformation[UserEvent] = TypeExtractor.getForClass(classOf[UserEvent])
  implicit val userServiceEventInfo: TypeInformation[UserMappingEvent] = TypeExtractor.getForClass(classOf[UserMappingEvent])

  // Kafka Topics Configuration
  val projectInputTopic: String = config.getString("kafka.project.dashboard.input.topic")
  val surveyInputTopic: String = config.getString("kafka.survey.dashboard.input.topic")
  val observationInputTopic: String = config.getString("kafka.observation.dashboard.input.topic")
  val userInputTopic: String = config.getString("kafka.user.dashboard.input.topic")
  val mentoringInputTopic: String = config.getString("kafka.mentoring.dashboard.input.topic")
  val userServiceInputTopic: String = config.getString("kafka.user.map.input.topic.one")
  val programServiceInputTopic: String = config.getString("kafka.user.map.input.topic.two")
  val notificationOutputTopic: String = config.getString("kafka.user.map.output.topic")

  val userServiceOutputTag: OutputTag[String] = OutputTag[String]("user-service-output-event")

  // Parallelism
  override val parallelism: Int = 1

  // Mentoring
  val mentoringConsumerParallelism: Int = config.getInt("task.mentoring.dashboard.parallelism")
  val mentoringProcessParallelism: Int = config.getInt("task.mentoring.metabase.dashboard.parallelism")
  private val mentoringConsumerGroup: String = config.getString("kafka.mentoring.dashboard.groupId")
  lazy val mentoringKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(mentoringConsumerGroup)

  // Observation
  val observationConsumerParallelism: Int = config.getInt("task.observation.dashboard.parallelism")
  val observationProcessParallelism: Int = config.getInt("task.observation.metabase.dashboard.parallelism")
  private val observationConsumerGroup: String = config.getString("kafka.observation.dashboard.groupId")
  lazy val observationKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(observationConsumerGroup)

  // Project
  val projectConsumerParallelism: Int = config.getInt("task.project.dashboard.parallelism")
  val projectProcessParallelism: Int = config.getInt("task.project.metabase.dashboard.parallelism")
  private val projectConsumerGroup: String = config.getString("kafka.project.dashboard.groupId")
  lazy val projectKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(projectConsumerGroup)

  // Survey
  val surveyConsumerParallelism: Int = config.getInt("task.survey.dashboard.parallelism")
  val surveyProcessParallelism: Int = config.getInt("task.survey.metabase.dashboard.parallelism")
  private val surveyConsumerGroup: String = config.getString("kafka.survey.dashboard.groupId")
  lazy val surveyKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(surveyConsumerGroup)

  // User
  val userConsumerParallelism: Int = config.getInt("task.user.dashboard.parallelism")
  val userProcessParallelism: Int = config.getInt("task.user.metabase.dashboard.parallelism")
  private val userConsumerGroup: String = config.getString("kafka.user.dashboard.groupId")
  lazy val userKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(userConsumerGroup)

  // User Service
  val userServiceConsumerParallelism: Int = config.getInt("task.user.map.user.service.parallelism")
  val userServiceProcessParallelism: Int = config.getInt("task.user.map.user.service.process.parallelism")
  private val userServiceConsumerGroup: String = config.getString("kafka.user.map.groupId")
  lazy val userServiceKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(userServiceConsumerGroup)

  // Program Service
  val programServiceConsumerParallelism: Int = config.getInt("task.user.map.program.service.parallelism")
  val programServiceProcessParallelism: Int = config.getInt("task.user.map.program.service.process.parallelism")
  private val programServiceConsumerGroup: String = config.getString("kafka.user.map.groupId")
  lazy val programServiceKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(programServiceConsumerGroup)

  // Notification
  val notificationProducerParallelism: Int = config.getInt("task.user.map.notification.parallelism")

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
  val userMetrics: String = config.getString("postgres.tables.userMetrics")

  // Metabase Config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  val metabaseDatabase: String = config.getString("metabase.database")
  val metabaseDomainName: String = config.getString("metabase.domainName")
  val evidenceBaseUrl: String = config.getString("metabase.evidenceBaseUrl")
  val metabaseApiKey: String = config.getString("metabase.api.key")
  val metabaseKey: String = metabaseApiKey

  // User Service specific configurations
  val domainName: String = config.getString("user.map.domain.name")
  
  // Notification config
  val notificationType: String = config.getString("user.map.notify.type")
  val notificationApiUrl: String = config.getString("user.map.notify.api.url")
  val notificationEmailTemplate: String = config.getString("user.map.notify.email.template")
  val notificationSmsTemplate: String = config.getString("user.map.notify.sms.template")
}
