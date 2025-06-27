package org.shikshalokam.job.survey.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.survey.stream.processor.domain.Event
import org.shikshalokam.job.BaseJobConfig
import scala.collection.JavaConverters._

class SurveyStreamConfig(override val config: Config) extends BaseJobConfig(config, "SurveysStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val outputTopic: String = config.getString("kafka.output.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("survey-dashboard-output-event")

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val surveysStreamParallelism: Int = config.getInt("task.sl.surveys.stream.parallelism")
  val metabaseDashboardParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val surveysStreamConsumer: String = "survey-stream-consumer"
  val metabaseDashboardProducer = "metabase-dashboard-producer"

  // Functions
  val surveysStreamFunction: String = "SurveyStreamFunction"

  // Survey submissions job metrics
  val surveysCleanupHit = "survey-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-survey-events-count"

  //report-config
  val reportsEnabled: Set[String] = config.getStringList("reports.enabled").asScala.toSet

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val solutions: String = config.getString("postgres.tables.solutionsTable")
  val dashboard_metadata: String = config.getString("postgres.tables.dashboardMetadataTable")

  val createSolutionsTable =
    s"""CREATE TABLE IF NOT EXISTS $solutions (
       |    solution_id TEXT PRIMARY KEY,
       |    external_id TEXT,
       |    name TEXT,
       |    description TEXT,
       |    duration TEXT,
       |    categories TEXT,
       |    program_id TEXT,
       |    program_name TEXT,
       |    program_external_id TEXT,
       |    program_description TEXT,
       |    private_program BOOLEAN
       |);""".stripMargin

  val createDashboardMetadataTable =
    s"""CREATE TABLE IF NOT EXISTS $dashboard_metadata (
       |    id SERIAL PRIMARY KEY,
       |    entity_type TEXT NOT NULL,
       |    entity_name TEXT NOT NULL,
       |    entity_id TEXT UNIQUE NOT NULL,
       |    report_type TEXT,
       |    is_rubrics Boolean,
       |    parent_name TEXT,
       |    linked_to TEXT,
       |    collection_id TEXT,
       |    dashboard_id TEXT,
       |    question_ids TEXT,
       |    status TEXT,
       |    error_message TEXT
       |);
       |""".stripMargin

}