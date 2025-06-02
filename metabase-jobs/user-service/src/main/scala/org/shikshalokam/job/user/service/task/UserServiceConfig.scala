package org.shikshalokam.job.user.service.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.user.service.domain.Event

class UserServiceConfig(override val config: Config) extends BaseJobConfig(config, "UserManagement") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val outputTopic: String = config.getString("kafka.output.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("user-service-output-event")

  // Parallelism
  val userServiceParallelism: Int = config.getInt("task.sl.user.service.parallelism")
  val notificationServiceParallelism: Int = config.getInt("task.sl.notification.parallelism")

  // Consumers
  val userServiceConsumer: String = "user-service-consumer"
  val notificationServiceProducer: String = "notification-service-producer"

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

  // Metabase connection config
  val metabaseUrl: String = config.getString("metabase.url")
  val metabaseUsername: String = config.getString("metabase.username")
  val metabasePassword: String = config.getString("metabase.password")
  val metabaseDomainName: String = config.getString("metabase.domainName")

  // Domain static name
  val domainName: String = config.getString("domain.name")

  // Notification config
  val notificationType = config.getString("notify.type")
  val notificationApiUrl = config.getString("notify.api.url")
  val notificationEmailTemplate = config.getString("notify.email.template")
  val notificationSmsTemplate = config.getString("notify.sms.template")

}
