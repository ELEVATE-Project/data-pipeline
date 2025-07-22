package org.shikshalokam.job.user.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.user.stream.processor.domain.Event

class UserStreamConfig(override val config: Config) extends BaseJobConfig(config, "UsersStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val outputTopic: String = config.getString("kafka.output.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("user-dashboard-output-event")

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val usersStreamParallelism: Int = config.getInt("task.sl.users.stream.parallelism")
  val metabaseDashboardParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val usersStreamConsumer: String = "user-stream-consumer"
  val metabaseDashboardProducer = "metabase-users-dashboard-producer"

  // Functions
  val usersStreamFunction: String = "UserStreamFunction"

  // user submissions job metrics
  val usersCleanupHit = "user-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-user-events-count"


  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val shikshagraha: String = config.getString("postgres.tables.shikshagrahaTable")
  val shikshalokam: String = config.getString("postgres.tables.shikshalokamTable")


  val createshikshagrahaTable =
    s"""CREATE TABLE IF NOT EXISTS $shikshagraha (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    tenant_code TEXT,
       |    user_id INT,
       |    username TEXT,
       |    name TEXT,
       |    eventType TEXT,
       |    status TEXT,
       |    is_deleted BOOLEAN,
       |    created_by INT,
       |    professional_role_id TEXT,
       |    professional_role_name TEXT,
       |    professional_subrole_id TEXT,
       |    professional_subrole_name TEXT,
       |    organizations_id TEXT,
       |    organizations_name TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    user_profile_one_id TEXT,
       |    user_profile_one_name TEXT,
       |    user_profile_one_external_id TEXT,
       |    user_profile_two_id  TEXT,
       |    user_profile_two_name TEXT,
       |    user_profile_two_external_id TEXT,
       |    user_profile_three_id TEXT,
       |    user_profile_three_name TEXT,
       |    user_profile_three_external_id TEXT,
       |    user_profile_four_id TEXT,
       |    user_profile_four_name TEXT,
       |    user_profile_four_external_id TEXT,
       |    user_profile_five_id TEXT,
       |    user_profile_five_name TEXT,
       |    user_profile_five_external_id TEXT
       |);""".stripMargin

  val createshikshalokamTable =
    s"""CREATE TABLE IF NOT EXISTS $shikshalokam (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    tenant_code TEXT,
       |    user_id INT,
       |    username TEXT,
       |    name TEXT,
       |    eventType TEXT,
       |    status TEXT,
       |    is_deleted BOOLEAN,
       |    created_by INT,
       |    professional_role_id TEXT,
       |    professional_role_name TEXT,
       |    professional_subrole_id TEXT,
       |    professional_subrole_name TEXT,
       |    organizations_id TEXT,
       |    organizations_name TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    user_profile_one_id TEXT,
       |    user_profile_one_name TEXT,
       |    user_profile_one_external_id TEXT,
       |    user_profile_two_id  TEXT,
       |    user_profile_two_name TEXT,
       |    user_profile_two_external_id TEXT,
       |    user_profile_three_id TEXT,
       |    user_profile_three_name TEXT,
       |    user_profile_three_external_id TEXT,
       |    user_profile_four_id TEXT,
       |    user_profile_four_name TEXT,
       |    user_profile_four_external_id TEXT,
       |    user_profile_five_id TEXT,
       |    user_profile_five_name TEXT,
       |    user_profile_five_external_id TEXT
       |);""".stripMargin


}