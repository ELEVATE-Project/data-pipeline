package org.shikshalokam.job.project.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.project.stream.processor.domain.Event
import org.shikshalokam.job.BaseJobConfig
import scala.collection.JavaConverters._

class ProjectStreamConfig(override val config: Config) extends BaseJobConfig(config, "ProjectsStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val outputTopic: String = config.getString("kafka.output.topic")

  // Output Tags
  val eventOutputTag: OutputTag[String] = OutputTag[String]("project-dashboard-output-event")

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val projectsStreamParallelism: Int = config.getInt("task.sl.projects.stream.parallelism")
  val metabaseDashboardParallelism: Int = config.getInt("task.sl.metabase.dashboard.parallelism")

  // Consumers
  val projectsStreamConsumer: String = "project-stream-consumer"
  val metabaseDashboardProducer = "metabase-dashboard-producer"

  // Functions
  val projectsStreamFunction: String = "ProjectStreamFunction"

  // Project submissions job metrics
  val projectsCleanupHit = "project-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-project-events-count"

  //report-config
  val reportsEnabled: Set[String] = config.getStringList("reports.enabled").asScala.toSet

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val solutions: String = config.getString("postgres.tables.solutionsTable")
  val projects: String = config.getString("postgres.tables.projectsTable")
  val tasks: String = config.getString("postgres.tables.tasksTable")
  val dashboard_metadata: String = config.getString("postgres.tables.dashboardMetadataTable")

  val createSolutionsTable: String =
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
       |    private_program BOOLEAN,
       |    org_id TEXT
       |);""".stripMargin

  val createProjectTable: String =
    s"""CREATE TABLE IF NOT EXISTS $projects (
       |    project_id TEXT PRIMARY KEY,
       |    solution_id TEXT REFERENCES $solutions(solution_id),
       |    created_by TEXT,
       |    created_date TIMESTAMP WITHOUT TIME ZONE,
       |    completed_date TEXT,
       |    last_sync TEXT,
       |    updated_date TEXT,
       |    status TEXT,
       |    remarks TEXT,
       |    evidence TEXT,
       |    evidence_count TEXT,
       |    program_id TEXT,
       |    program_name TEXT,
       |    task_count TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    tenant_id TEXT,
       |    org_id TEXT,
       |    org_name TEXT,
       |    org_code TEXT,
       |    state_id TEXT,
       |    state_name TEXT,
       |    district_id TEXT,
       |    district_name TEXT,
       |    block_id TEXT,
       |    block_name TEXT,
       |    cluster_id TEXT,
       |    cluster_name TEXT,
       |    school_id TEXT,
       |    school_name TEXT,
       |    certificate_template_id TEXT,
       |    certificate_template_url TEXT,
       |    certificate_issued_on TEXT,
       |    certificate_status TEXT,
       |    certificate_pdf_path TEXT
       |);""".stripMargin

  val AlterProjectTable: String =
    s"""ALTER TABLE IF EXISTS $projects
       |ADD COLUMN IF NOT EXISTS tenant_id TEXT;""".stripMargin

  val createTasksTable: String =
    s"""CREATE TABLE IF NOT EXISTS $tasks (
       |    task_id TEXT PRIMARY KEY,
       |    project_id TEXT REFERENCES $projects(project_id),
       |    name TEXT,
       |    assigned_to TEXT,
       |    start_date TEXT,
       |    end_date TEXT,
       |    synced_at TEXT,
       |    is_deleted TEXT,
       |    is_deletable TEXT,
       |    remarks TEXT,
       |    status TEXT,
       |    evidence TEXT,
       |    evidence_count TEXT
       |);""".stripMargin

  val createDashboardMetadataTable: String =
    s"""CREATE TABLE IF NOT EXISTS $dashboard_metadata (
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
       |""".stripMargin

}