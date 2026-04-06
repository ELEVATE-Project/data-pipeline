package org.shikshalokam.job.mentoring.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.mentoring.stream.processor.domain.Event

class MentoringStreamConfig(override val config: Config) extends BaseJobConfig(config, "MentoringStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val outputTopic: String = config.getString("kafka.output.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("mentoring-dashboard-output-event")

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val mentoringStreamParallelism: Int = config.getInt("task.sl.mentoring.stream.parallelism")
  val metabaseDashboardParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val mentoringStreamConsumer: String = "mentoring-stream-consumer"
  val metabaseDashboardProducer: String = "metabase-mentoring-dashboard-producer"

  // Functions
  val mentoringStreamFunction: String = "MentoringStreamFunction"

  // mentoring submissions job metrics
  val mentoringCleanupHit: String = "mentoring-cleanup-hit"
  val skipCount: String = "skipped-message-count"
  val successCount: String = "success-message-count"
  val totalEventsCount: String = "total-mentoring-events-count"


  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val dashboardMetadata: String = config.getString("postgres.tables.dashboardMetadataTable")

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

  val createTenantSessionTable: String =
    s"""
       CREATE TABLE IF NOT EXISTS @sessions (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    session_id INT UNIQUE,
       |    mentor_id INT,
       |    name TEXT,
       |    description TEXT,
       |    type TEXT,
       |    status TEXT,
       |    start_date TIMESTAMPTZ,
       |    end_date TIMESTAMPTZ,
       |    org_id INT,
       |    org_code TEXT,
       |    org_name TEXT,
       |    platform TEXT,
       |    started_at TIMESTAMPTZ,
       |    completed_at TIMESTAMPTZ,
       |    created_at TIMESTAMPTZ,
       |    updated_at TIMESTAMPTZ,
       |    deleted_at TIMESTAMPTZ,
       |    recommended_for TEXT,
       |    categories TEXT,
       |    medium TEXT,
       |    created_by INT,
       |    updated_by INT
       |);
    """.stripMargin

  val createTenantSessionAttendanceTable: String =
    s"""
       CREATE TABLE IF NOT EXISTS @sessionAttendance (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    attendance_id INT UNIQUE,
       |    session_id INT,
       |    mentee_id INT,
       |    joined_at TIMESTAMPTZ,
       |    left_at TIMESTAMPTZ,
       |    is_feedback_skipped BOOLEAN DEFAULT FALSE,
       |    type TEXT,
       |    created_at TIMESTAMPTZ,
       |    updated_at TIMESTAMPTZ,
       |    deleted_at TIMESTAMPTZ,
       |    UNIQUE (session_id, mentee_id)
       |);
    """.stripMargin

  val createTenantConnectionsTable: String =
    s"""
       |CREATE TABLE IF NOT EXISTS @connectionsTable (
       |    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    connection_id INT UNIQUE,
       |    user_id INT,
       |    friend_id INT,
       |    status TEXT,
       |    org_id INT,
       |    created_by INT,
       |    updated_by INT,
       |    created_at TIMESTAMPTZ,
       |    updated_at TIMESTAMPTZ,
       |    deleted_at TIMESTAMPTZ
       |);
    """.stripMargin

  val createOrgMentorRatingTable: String =
    s"""
       CREATE TABLE IF NOT EXISTS @orgMentorRating (
       |    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
       |    org_id INT,
       |    org_name TEXT,
       |    mentor_id INT,
       |    rating INT,
       |    rating_updated_at TIMESTAMPTZ
       |);
    """.stripMargin
}