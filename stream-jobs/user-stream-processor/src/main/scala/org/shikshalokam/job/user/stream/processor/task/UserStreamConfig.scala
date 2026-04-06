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
  val mentoringOutputTopic: String = config.getString("kafka.output.mentoring.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("user-dashboard-output-event")
  val mentoringEventOutputTag: OutputTag[String] = OutputTag[String]("user-mentoring-output-event")
  
  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val usersStreamParallelism: Int = config.getInt("task.sl.users.stream.parallelism")
  val metabaseDashboardParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val usersStreamConsumer: String = "user-stream-consumer"
  val metabaseDashboardProducer: String = "metabase-users-dashboard-producer"
  val mentoringDashboardProducer: String = "metabase-mentoring-dashboard-producer"

  // Functions
  val usersStreamFunction: String = "UserStreamFunction"

  // user submissions job metrics
  val usersCleanupHit: String = "user-cleanup-hit"
  val skipCount: String = "skipped-message-count"
  val successCount: String = "success-message-count"
  val totalEventsCount: String = "total-user-events-count"


  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val userMetrics: String = config.getString("postgres.tables.userMetrics")
  val dashboardMetadata: String = config.getString("postgres.tables.dashboardMetadataTable")

  val createTenantUserMetadataTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS @tenantTable (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    user_id INT,
       |    attribute_code TEXT,
       |    attribute_value TEXT,
       |    attribute_label TEXT,
       |    UNIQUE (user_id, attribute_value)
       |);
    """.stripMargin

  val createOrgRolesTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS @orgRolesTable (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    user_id INT,
       |    org_id INT,
       |    org_name TEXT,
       |    role_id INT,
       |    role_name TEXT,
       |    UNIQUE (user_id, org_id, role_id)
       |);
    """.stripMargin

  val createTenantUserTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS @usersTable (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    user_id INT UNIQUE,
       |    tenant_code TEXT,
       |    username TEXT,
       |    name TEXT,
       |    status TEXT,
       |    is_deleted BOOLEAN,
       |    created_by INT,
       |    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       |    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
       |);
    """.stripMargin

  val createUserMetricsTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS $userMetrics  (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    tenant_code TEXT UNIQUE,
       |    total_users INT,
       |    active_users INT,
       |    deleted_users INT,
       |    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       |);
    """.stripMargin

  val createDashboardMetadataTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS $dashboardMetadata (
       |    id SERIAL PRIMARY KEY,
       |    entity_type TEXT NOT NULL,
       |    entity_name TEXT NOT NULL,
       |    entity_id TEXT UNIQUE NOT NULL,
       |    report_type TEXT,
       |    is_rubrics Boolean,
       |    parent_name TEXT,
       |    linked_to TEXT,
       |    main_metadata JSON,
       |    mi_metadata JSON,
       |    comparison_metadata JSON,
       |    status TEXT,
       |    error_message TEXT,
       |    state_details_url_state TEXT,
       |    state_details_url_admin TEXT,
       |    district_details_url_district TEXT,
       |    district_details_url_state TEXT,
       |    district_details_url_admin TEXT
       |);
    """.stripMargin

}