package org.shikshalokam.job.project.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.project.stream.processor.domain.Event
import org.shikshalokam.job.BaseJobConfig

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

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")

  // PostgreSQL query config
  val solutionsTable = "Solutions"
  val projectsTable = "Projects"
  val tasksTable = "Tasks"
  val dashboardMetadataTable = "dashboard_metadata"

  val createSolutionsTable =
    """CREATE TABLE IF NOT EXISTS Solutions (
      |    solutionId TEXT PRIMARY KEY,
      |    externalId TEXT,
      |    name TEXT,
      |    description TEXT,
      |    programId TEXT,
      |    programName TEXT,
      |    programExternalId TEXT,
      |    programDescription TEXT,
      |    privateProgram BOOLEAN
      |);""".stripMargin

  val createProjectTable =
    """CREATE TABLE IF NOT EXISTS Projects (
      |    projectId TEXT PRIMARY KEY,
      |    solutionId TEXT REFERENCES Solutions(solutionId),
      |    createdBy TEXT,
      |    createdDate TEXT,
      |    completedDate TEXT,
      |    lastSync TEXT,
      |    updatedDate TEXT,
      |    status TEXT,
      |    remarks TEXT,
      |    evidence TEXT,
      |    evidenceCount TEXT,
      |    programId TEXT,
      |    taskCount TEXT,
      |    userRoleIds TEXT,
      |    userRoles TEXT,
      |    orgId TEXT,
      |    orgName TEXT,
      |    orgCode TEXT,
      |    stateId TEXT,
      |    stateName TEXT,
      |    districtId TEXT,
      |    districtName TEXT,
      |    blockId TEXT,
      |    blockName TEXT,
      |    clusterId TEXT,
      |    clusterName TEXT,
      |    schoolId TEXT,
      |    schoolName TEXT,
      |    certificateTemplateId TEXT,
      |    certificateTemplateUrl TEXT,
      |    certificateIssuedOn TEXT,
      |    certificateStatus TEXT,
      |    certificatePdfPath TEXT
      |);""".stripMargin

  val createTasksTable =
    """CREATE TABLE IF NOT EXISTS Tasks (
      |    taskId TEXT PRIMARY KEY,
      |    projectId TEXT REFERENCES Projects(projectId),
      |    name TEXT,
      |    assignedTo TEXT,
      |    startDate TEXT,
      |    endDate TEXT,
      |    syncedAt TEXT,
      |    isDeleted TEXT,
      |    isDeletable TEXT,
      |    remarks TEXT,
      |    status TEXT,
      |    evidence TEXT,
      |    evidenceCount TEXT
      |);""".stripMargin

  val createDashboardMetadataTable =
    """CREATE TABLE IF NOT EXISTS dashboard_metadata (
      |    id SERIAL PRIMARY KEY,
      |    entity_type TEXT NOT NULL,
      |    entity_name TEXT NOT NULL,
      |    entity_id TEXT UNIQUE NOT NULL,
      |    collection_id TEXT,
      |    dashboard_id TEXT,
      |    question_ids TEXT,
      |    status TEXT,
      |    error_message TEXT
      |);
      |""".stripMargin

}