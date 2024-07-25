package org.shikshalokam.job.project.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.BaseJobConfig


class ProjectStreamConfig(override val config: Config) extends BaseJobConfig(config, "ProjectsStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // Parallelism
  val mlProjectsParallelism: Int = config.getInt("task.ml.projects.parallelism")

  // Consumers
  val mlProjectsConsumer: String = "ml-project-consumer"

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
  val organisationsTable = "Organisations"

  val createSolutionsTable = """CREATE TABLE IF NOT EXISTS Solutions (
                         |    solutionId TEXT PRIMARY KEY,
                         |    externalId TEXT,
                         |    name TEXT,
                         |    description TEXT,
                         |    duration TEXT,
                         |    hasAcceptedTAndC TEXT,
                         |    isDeleted BOOLEAN,
                         |    createdType TEXT,
                         |    programId TEXT,
                         |    programName TEXT,
                         |    programExternalId TEXT,
                         |    programDescription TEXT,
                         |    privateProgram BOOLEAN
                         |);
                         |""".stripMargin

  val createProjectTable = """CREATE TABLE IF NOT EXISTS Projects (
                             |    projectId TEXT PRIMARY KEY,
                             |    createdBy TEXT,
                             |    solutionId TEXT,
                             |    programId TEXT,
                             |    completedDate TEXT,
                             |    createdDate TEXT,
                             |    evidence TEXT,
                             |    evidenceCount TEXT,
                             |    lastSync TEXT,
                             |    remarks TEXT,
                             |    updatedDate DATE,
                             |    projectStatus TEXT,
                             |    organisationId TEXT,
                             |    stateCode TEXT,
                             |    stateExternalId TEXT,
                             |    stateName TEXT,
                             |    districtCode TEXT,
                             |    districtExternalId TEXT,
                             |    districtName TEXT,
                             |    blockCode TEXT,
                             |    blockExternalId TEXT,
                             |    blockName TEXT,
                             |    clusterCode TEXT,
                             |    clusterExternalId TEXT,
                             |    clusterName TEXT,
                             |    schoolCode TEXT,
                             |    schoolExternalId TEXT,
                             |    schoolName TEXT,
                             |    boardName TEXT
                             |);""".stripMargin

}