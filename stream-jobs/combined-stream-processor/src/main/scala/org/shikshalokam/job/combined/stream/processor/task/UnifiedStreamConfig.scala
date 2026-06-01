package org.shikshalokam.job.combined.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig

import scala.collection.JavaConverters._

class UnifiedStreamConfig(override val config: Config) extends BaseJobConfig(config, "CombinedStreamProcessorJob") {

  // === Project ===
  val projectInputTopic: String = config.getString("kafka.project.stream.input.topic")
  val projectOutputTopic: String = config.getString("kafka.project.stream.output.topic")
  val projectConsumerParallelism: Int = config.getInt("task.project.stream.consumer.parallelism")
  val projectProcessParallelism: Int = config.getInt("task.project.stream.parallelism")
  val projectSinkParallelism: Int = config.getInt("task.project.sink.parallelism")
  val projectOutputTag = new OutputTag[String]("project-dashboard-events")
  val isProjectStreamEnabled: Boolean = if (config.hasPath("combined.project.stream.job.enabled")) config.getBoolean("combined.project.stream.job.enabled") else false
  private val projectGroupId: String = config.getString("kafka.project.stream.groupId")
  lazy val projectKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(projectGroupId)

  // === Survey ===
  val surveyInputTopic: String = config.getString("kafka.survey.stream.input.topic")
  val surveyOutputTopic: String = config.getString("kafka.survey.stream.output.topic")
  val surveyConsumerParallelism: Int = config.getInt("task.survey.stream.consumer.parallelism")
  val surveyProcessParallelism: Int = config.getInt("task.survey.stream.parallelism")
  val surveySinkParallelism: Int = config.getInt("task.survey.sink.parallelism")
  val surveyOutputTag = new OutputTag[String]("survey-dashboard-events")
  val isTestMode: Boolean = config.hasPath("test.mode") && config.getBoolean("test.mode")
  val isSurveyStreamEnabled: Boolean = if (config.hasPath("combined.survey.stream.job.enabled")) config.getBoolean("combined.survey.stream.job.enabled") else false
  private val surveyGroupId: String = config.getString("kafka.survey.stream.groupId")
  lazy val surveyKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(surveyGroupId)

  // Output Tags
  val projectEventOutputTag: OutputTag[String] = OutputTag[String]("project-dashboard-output-event")
  val surveyEventOutputTag: OutputTag[String] = OutputTag[String]("survey-dashboard-output-event")
  val eventOutputTag: OutputTag[String] = OutputTag[String]("observation-dashboard-events")
  val mentoringEventOutputTag: OutputTag[String] = OutputTag[String]("mentoring-dashboard-events")
  val userOutputTag: OutputTag[String] = OutputTag[String]("user-dashboard-events")

  // === Observation ===
  val observationInputTopic: String = config.getString("kafka.observation.stream.input.topic")
  val observationOutputTopic: String = config.getString("kafka.observation.stream.output.topic")
  val observationConsumerParallelism: Int = config.getInt("task.observation.stream.consumer.parallelism")
  val observationProcessParallelism: Int = config.getInt("task.observation.stream.parallelism")
  val observationSinkParallelism: Int = config.getInt("task.observation.sink.parallelism")
  val observationCleanupHit = "observation-cleanup-hit"
  val isObservationStreamEnabled: Boolean = if (config.hasPath("combined.observation.stream.job.enabled")) config.getBoolean("combined.observation.stream.job.enabled") else false
  private val observationGroupId: String = config.getString("kafka.observation.stream.groupId")
  lazy val observationKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(observationGroupId)

  // === User ===
  val userInputTopic: String = config.getString("kafka.user.stream.input.topic")
  val userOutputTopic: String = config.getString("kafka.user.stream.output.topic")
  val userConsumerParallelism: Int = config.getInt("task.user.stream.consumer.parallelism")
  val userProcessParallelism: Int = config.getInt("task.user.stream.parallelism")
  val userSinkParallelism: Int = config.getInt("task.user.sink.stream.parallelism")
  val usersCleanupHit = "user-cleanup-hit"
  val userMetrics: String = config.getString("postgres.tables.userMetrics")
  val isUserStreamEnabled: Boolean = if (config.hasPath("combined.user.stream.job.enabled")) config.getBoolean("combined.user.stream.job.enabled") else false
  private val userGroupId: String = config.getString("kafka.user.stream.groupId")
  lazy val userKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(userGroupId)

  // === Mentoring ===
  val mentoringInputTopic: String = config.getString("kafka.mentoring.stream.input.topic")
  val mentoringOutputTopic: String = config.getString("kafka.mentoring.stream.output.topic")
  val mentoringConsumerParallelism: Int = config.getInt("task.mentoring.stream.consumer.parallelism")
  val mentoringProcessParallelism: Int = config.getInt("task.mentoring.stream.parallelism")
  val mentoringSinkParallelism: Int = config.getInt("task.mentoring.stream.sink.parallelism")
  val mentoringCleanupHit = "mentoring-cleanup-hit"
  val isMentoringStreamEnabled: Boolean = if (config.hasPath("combined.mentoring.stream.job.enabled")) config.getBoolean("combined.mentoring.stream.job.enabled") else false
  private val mentoringGroupId: String = config.getString("kafka.mentoring.stream.groupId")
  lazy val mentoringKafkaConsumerProperties: java.util.Properties = kafkaConsumerPropertiesWithGroupId(mentoringGroupId)

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Project submissions job metrics
  val projectsCleanupHit = "project-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-project-events-count"

  //report-config
  val projectReportsEnabled: Set[String] = config.getStringList("project.reports.enabled").asScala.toSet
  val surveyReportsEnabled: Set[String] = config.getStringList("survey.reports.enabled").asScala.toSet
  val observationReportsEnabled: Set[String] = config.getStringList("observation.reports.enabled").asScala.toSet

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

  val createSurveyStatusTableQuery: String =
    s"""CREATE TABLE IF NOT EXISTS @surveyStatusTable (
       |    survey_id TEXT PRIMARY KEY,
       |    user_id TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
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
       |    tenant_id TEXT,
       |    organisation_id TEXT,
       |    organisation_name TEXT,
       |    organisation_code TEXT,
       |    program_name TEXT,
       |    program_id TEXT,
       |    solution_name TEXT,
       |    solution_id TEXT,
       |    status TEXT,
       |    submission_date TEXT
       |);""".stripMargin

  val createSurveyQuestionsTableQuery: String =
    s"""CREATE TABLE IF NOT EXISTS "@surveyQuestionTable" (
       |    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
       |    survey_id TEXT,
       |    user_id TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
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
       |    tenant_id TEXT,
       |    organisation_id TEXT,
       |    organisation_name TEXT,
       |    organisation_code TEXT,
       |    program_name TEXT,
       |    program_id TEXT,
       |    solution_name TEXT,
       |    solution_id TEXT,
       |    question_id TEXT,
       |    question_text TEXT,
       |    question_type TEXT,
       |    labels TEXT,
       |    value TEXT,
       |    has_parent_question BOOLEAN,
       |    parent_question_text TEXT,
       |    report_type TEXT,
       |    evidence TEXT,
       |    remarks TEXT
       |);""".stripMargin

  // Observation Tables
  val createStatusTable: String =
    s"""CREATE TABLE IF NOT EXISTS @statusTable (
       |    submission_id TEXT PRIMARY KEY,
       |    submission_number INTEGER,
       |    user_id TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    solution_id TEXT,
       |    solution_name TEXT,
       |    program_id TEXT,
       |    program_name TEXT,
       |    observation_id TEXT,
       |    observation_name TEXT,
       |    user_one_profile_name TEXT,
       |    user_one_profile_id TEXT,
       |    user_two_profile_name TEXT,
       |    user_two_profile_id TEXT,
       |    user_three_profile_name TEXT,
       |    user_three_profile_id TEXT,
       |    user_four_profile_name TEXT,
       |    user_four_profile_id TEXT,
       |    user_five_profile_name TEXT,
       |    user_five_profile_id TEXT,
       |    tenant_id TEXT,
       |    org_id TEXT,
       |    org_code TEXT,
       |    org_name TEXT,
       |    status_of_submission TEXT,
       |    submitted_at TEXT,
       |    entity_type TEXT,
       |    entity_id TEXT,
       |    entity_name TEXT,
       |    entity_external_id TEXT,
       |    parent_one_name TEXT,
       |    parent_one_id TEXT,
       |    parent_two_name TEXT,
       |    parent_two_id TEXT,
       |    parent_three_name TEXT,
       |    parent_three_id TEXT,
       |    parent_four_name TEXT,
       |    parent_four_id TEXT,
       |    parent_five_name TEXT,
       |    parent_five_id TEXT
       |);""".stripMargin

  val createDomainsTable: String =
    s"""CREATE TABLE IF NOT EXISTS @domainTable (
       |    id SERIAL PRIMARY KEY,
       |    user_id TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    solution_id TEXT,
       |    solution_name TEXT,
       |    submission_id TEXT,
       |    submission_number INTEGER,
       |    program_name TEXT,
       |    program_id TEXT,
       |    observation_name TEXT,
       |    observation_id TEXT,
       |    tenant_id TEXT,
       |    org_name TEXT,
       |    org_id TEXT,
       |    org_code TEXT,
       |    user_one_profile_name TEXT,
       |    user_one_profile_id TEXT,
       |    user_two_profile_name TEXT,
       |    user_two_profile_id TEXT,
       |    user_three_profile_name TEXT,
       |    user_three_profile_id TEXT,
       |    user_four_profile_name TEXT,
       |    user_four_profile_id TEXT,
       |    user_five_profile_name TEXT,
       |    user_five_profile_id TEXT,
       |    domain TEXT,
       |    domain_level TEXT,
       |    criteria TEXT,
       |    criteria_level TEXT,
       |    completed_date TEXT,
       |    entity_type TEXT,
       |    entity_id TEXT,
       |    entity_name TEXT,
       |    entity_external_id TEXT,
       |    parent_one_name TEXT,
       |    parent_one_id TEXT,
       |    parent_two_name TEXT,
       |    parent_two_id TEXT,
       |    parent_three_name TEXT,
       |    parent_three_id TEXT,
       |    parent_four_name TEXT,
       |    parent_four_id TEXT,
       |    parent_five_name TEXT,
       |    parent_five_id TEXT
       |);""".stripMargin

  val createQuestionsTable: String =
    s"""CREATE TABLE IF NOT EXISTS @questionTable (
       |    id SERIAL PRIMARY KEY,
       |    user_id TEXT,
       |    user_role_ids TEXT,
       |    user_roles TEXT,
       |    solution_id TEXT,
       |    solution_name TEXT,
       |    submission_id TEXT,
       |    submission_number INTEGER,
       |    program_name TEXT,
       |    program_id TEXT,
       |    observation_name TEXT,
       |    observation_id TEXT,
       |    value TEXT,
       |    user_one_profile_name TEXT,
       |    user_one_profile_id TEXT,
       |    user_two_profile_name TEXT,
       |    user_two_profile_id TEXT,
       |    user_three_profile_name TEXT,
       |    user_three_profile_id TEXT,
       |    user_four_profile_name TEXT,
       |    user_four_profile_id TEXT,
       |    user_five_profile_name TEXT,
       |    user_five_profile_id TEXT,
       |    tenant_id TEXT,
       |    org_id TEXT,
       |    org_code TEXT,
       |    org_name TEXT,
       |    status_of_submission TEXT,
       |    submitted_at TEXT,
       |    entity_type TEXT,
       |    entity_id TEXT,
       |    entity_name TEXT,
       |    entity_external_id TEXT,
       |    parent_one_name TEXT,
       |    parent_one_id TEXT,
       |    parent_two_name TEXT,
       |    parent_two_id TEXT,
       |    parent_three_name TEXT,
       |    parent_three_id TEXT,
       |    parent_four_name TEXT,
       |    parent_four_id TEXT,
       |    parent_five_name TEXT,
       |    parent_five_id TEXT,
       |    domain_name TEXT,
       |    criteria_name TEXT,
       |    score INTEGER,
       |    has_parent_question BOOLEAN,
       |    parent_question_text TEXT,
       |    evidence TEXT,
       |    remarks TEXT,
       |    report_type TEXT,
       |    question_id TEXT,
       |    question_text TEXT,
       |    question_type TEXT,
       |    labels TEXT
       |);""".stripMargin

  // User Tables
  val createTenantUserMetadataTable: String =
    s"""CREATE TABLE IF NOT EXISTS @tenantTable (
       |    id SERIAL PRIMARY KEY,
       |    user_id TEXT NOT NULL,
       |    attribute_code TEXT NOT NULL,
       |    attribute_value TEXT NOT NULL,
       |    attribute_label TEXT NOT NULL,
       |    UNIQUE(user_id, attribute_value)
       |);""".stripMargin

  val createOrgRolesTable: String =
    s"""CREATE TABLE IF NOT EXISTS @orgRolesTable (
       |    id SERIAL PRIMARY KEY,
       |    user_id TEXT NOT NULL,
       |    org_id TEXT NOT NULL,
       |    org_name TEXT NOT NULL,
       |    role_id TEXT NOT NULL,
       |    role_name TEXT NOT NULL,
       |    UNIQUE(user_id, org_id, role_id)
       |);""".stripMargin

  val createTenantUserTable: String =
    s"""CREATE TABLE IF NOT EXISTS @usersTable (
       |    id SERIAL PRIMARY KEY,
       |    user_id TEXT UNIQUE NOT NULL,
       |    tenant_code TEXT,
       |    username TEXT,
       |    name TEXT,
       |    status TEXT,
       |    is_deleted BOOLEAN,
       |    created_by TEXT,
       |    created_at TEXT,
       |    updated_at TEXT,
       |    user_profile_one_id TEXT,
       |    user_profile_one_name TEXT,
       |    user_profile_one_external_id TEXT,
       |    user_profile_two_id TEXT,
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

  val createUserMetricsTable: String =
    s"""CREATE TABLE IF NOT EXISTS $userMetrics (
       |    tenant_code TEXT PRIMARY KEY,
       |    total_users BIGINT,
       |    active_users BIGINT,
       |    deleted_users BIGINT,
       |    last_updated TIMESTAMP
       |);""".stripMargin

  // Mentoring Tables
  val createTenantSessionTable: String =
    s"""CREATE TABLE IF NOT EXISTS @sessions (
       |    id SERIAL PRIMARY KEY,
       |    session_id TEXT UNIQUE NOT NULL,
       |    mentor_id INTEGER,
       |    name TEXT,
       |    description TEXT,
       |    type TEXT,
       |    status TEXT,
       |    start_date TEXT,
       |    end_date TEXT,
       |    org_id INTEGER,
       |    org_code TEXT,
       |    org_name TEXT,
       |    platform TEXT,
       |    started_at TEXT,
       |    completed_at TEXT,
       |    created_at TEXT,
       |    updated_at TEXT,
       |    deleted_at TEXT,
       |    recommended_for TEXT,
       |    categories TEXT,
       |    medium TEXT,
       |    created_by INTEGER,
       |    updated_by INTEGER
       |);""".stripMargin

  val createTenantSessionAttendanceTable: String =
    s"""CREATE TABLE IF NOT EXISTS @sessionAttendance (
       |    id SERIAL PRIMARY KEY,
       |    attendance_id TEXT UNIQUE NOT NULL,
       |    session_id TEXT,
       |    mentee_id INTEGER,
       |    joined_at TEXT,
       |    left_at TEXT,
       |    is_feedback_skipped BOOLEAN,
       |    type TEXT,
       |    created_at TEXT,
       |    updated_at TEXT,
       |    deleted_at TEXT
       |);""".stripMargin

  val createTenantConnectionsTable: String =
    s"""CREATE TABLE IF NOT EXISTS @connectionsTable (
       |    id SERIAL PRIMARY KEY,
       |    connection_id TEXT UNIQUE NOT NULL,
       |    user_id INTEGER,
       |    friend_id INTEGER,
       |    status TEXT,
       |    org_id INTEGER,
       |    created_by INTEGER,
       |    updated_by INTEGER,
       |    created_at TEXT,
       |    updated_at TEXT,
       |    deleted_at TEXT
       |);""".stripMargin

  val createOrgMentorRatingTable: String =
    s"""CREATE TABLE IF NOT EXISTS @orgMentorRating (
       |    id SERIAL PRIMARY KEY,
       |    org_id INTEGER,
       |    org_name TEXT,
       |    mentor_id INTEGER,
       |    rating TEXT,
       |    rating_updated_at TEXT
       |);""".stripMargin
}