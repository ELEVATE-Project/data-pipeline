package org.shikshalokam.job.project.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.project.stream.processor.domain.Event
import org.shikshalokam.job.project.stream.processor.task.ProjectStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable._

class ProjectStreamFunction(config: ProjectStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectStreamFunction])

  override def metricsList(): List[String] = {
    List(config.projectsCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    println(s"***************** Start of Processing the Project Event with Id = ${event._id} *****************")

    //TODO: TO be removed later
    val (projectEvidences, projectEvidencesCount) = extractEvidenceData(event.projectAttachments)
    val (roleIds, roles) = extractUserRolesData(event.userRoles)
    val tasksData = extractTasksData(event.tasks)
    val projectCategories = Option(event.projectCategories).map(extractProjectCategories).getOrElse("")

    //TODO: TO be removed later
    println("\n==> Solutions data ")
    println("solutionId = " + event.solutionId)
    println("solutionExternalId = " + event.solutionExternalId)
    println("solutionName = " + event.solutionName)
    println("solutionDescription = " + event.solutionDescription)
    println("duration = " + event.projectDuration)
    println("categories = " + projectCategories)
    println("privateProgram = " + event.privateProgram)
    println("programId = " + event.programId)
    println("programExternalId = " + event.programExternalId)
    println("programName = " + event.programName)
    println("programDescription = " + event.programDescription)

    println("\n==> Project data")
    println("projectId = " + event.projectId)
    println("solutionId = " + event.solutionId)
    println("createdBy = " + event.createdBy)
    println("completedDate = " + event.completedDate)
    println("createdAt = " + event.createdAt)
    println("projectLastSync = " + event.projectLastSync)
    println("projectUpdatedDate = " + event.projectUpdatedDate)
    println("projectStatus = " + event.projectStatus)
    println("projectRemarks = " + event.projectRemarks)
    println("projectEvidences = " + projectEvidences)
    println("projectEvidencesCount = " + projectEvidencesCount)
    println("programId = " + event.programId)
    println("taskCount = " + event.taskCount)
    println("userRoleIds = " + roleIds)
    println("userRoles = " + roles)
    println("organisationId = " + event.organisationId)
    println("organisationName = " + event.organisationName)
    println("organisationCode = " + event.organisationCode)
    println("stateId = " + event.stateId)
    println("stateName = " + event.stateName)
    println("districtId = " + event.districtId)
    println("districtName = " + event.districtName)
    println("blockId = " + event.blockId)
    println("blockName = " + event.blockName)
    println("clusterId = " + event.clusterId)
    println("clusterName = " + event.clusterName)
    println("schoolId = " + event.schoolId)
    println("schoolName = " + event.schoolName)
    println("certificateTemplateId = " + event.certificateTemplateId)
    println("certificateTemplateUrl = " + event.certificateTemplateUrl)
    println("certificateIssuedOn = " + event.certificateIssuedOn)
    println("certificateStatus = " + event.certificateStatus)
    println("certificatePdfPath = " + event.certificatePdfPath)

    println("\n==> Tasks data")
    println(tasksData)

    // Uncomment the bellow lines to create table schema for the first time.
    postgresUtil.createTable(config.createSolutionsTable, config.solutions)
    postgresUtil.createTable(config.createProjectTable, config.projects)
    postgresUtil.createTable(config.createTasksTable, config.tasks)
    postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)

    /**
     * Extracting Solutions data
     */
    val solutionId = event.solutionId
    val solutionExternalId = event.solutionExternalId
    val solutionName = event.solutionName
    val solutionDescription = event.solutionDescription
    val projectDuration = event.projectDuration
    val programId = event.programId
    val programName = event.programName
    val programExternalId = event.programExternalId
    val programDescription = event.programDescription
    val privateProgram = event.privateProgram

    val upsertSolutionQuery =
      s"""INSERT INTO ${config.solutions} (solution_id, external_id, name, description, duration, categories, program_id, program_name, program_external_id, program_description, private_program)
         |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |ON CONFLICT (solution_id) DO UPDATE SET
         |    external_id = ?,
         |    name = ?,
         |    description = ?,
         |    duration = ?,
         |    categories = ?,
         |    program_id = ?,
         |    program_name = ?,
         |    program_external_id = ?,
         |    program_description = ?,
         |    private_program = ?;
         |""".stripMargin

    val solutionParams = Seq(
      // Insert parameters
      solutionId, solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram,

      // Update parameters (matching columns in the ON CONFLICT clause)
      solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram
    )

    postgresUtil.executePreparedUpdate(upsertSolutionQuery, solutionParams, config.solutions, solutionId)

    /**
     * Extracting Project data
     */
    val projectId = event.projectId
    val createdBy = event.createdBy
    val createdDate = event.createdAt
    val completedDate = event.completedDate
    val lastSync = event.projectLastSync
    val updatedDate = event.projectUpdatedDate
    val status = event.projectStatus
    val remarks = event.projectRemarks
    val (evidence, evidenceCount) = extractEvidenceData(event.projectAttachments)
    val taskCount = event.taskCount
    val (userRoleIds, userRoles) = extractUserRolesData(event.userRoles)
    val orgId = event.organisationId
    val orgName = event.organisationName
    val orgCode = event.organisationCode
    val stateId = event.stateId
    val stateName = event.stateName
    val districtId = event.districtId
    val districtName = event.districtName
    val blockId = event.blockId
    val blockName = event.blockName
    val clusterId = event.clusterId
    val clusterName = event.clusterName
    val schoolId = event.schoolId
    val schoolName = event.schoolName
    val certificateTemplateId = event.certificateTemplateId
    val certificateTemplateUrl = event.certificateTemplateUrl
    val certificateIssuedOn = event.certificateIssuedOn
    val certificateStatus = event.certificateStatus
    val certificatePdfPath = event.certificatePdfPath

    val upsertProjectQuery =
      s"""INSERT INTO ${config.projects} (
         |    project_id, solution_id, created_by, created_date, completed_date, last_sync, updated_date, status, remarks,
         |    evidence, evidence_count, program_id, task_count, user_role_ids, user_roles, org_id, org_name, org_code, state_id,
         |    state_name, district_id, district_name, block_id, block_name, cluster_id, cluster_name, school_id, school_name,
         |    certificate_template_id, certificate_template_url, certificate_issued_on, certificate_status, certificate_pdf_path
         |) VALUES (
         |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         |) ON CONFLICT (project_id) DO UPDATE SET
         |    solution_id = ?, created_by = ?, created_date = ?, completed_date = ?, last_sync = ?, updated_date = ?,
         |    status = ?, remarks = ?, evidence = ?, evidence_count = ?, program_id = ?, task_count = ?, user_role_ids = ?,
         |    user_roles = ?, org_id = ?, org_name = ?, org_code = ?, state_id = ?, state_name = ?, district_id = ?,
         |    district_name = ?, block_id = ?, block_name = ?, cluster_id = ?, cluster_name = ?, school_id = ?, school_name = ?,
         |    certificate_template_id = ?, certificate_template_url = ?, certificate_issued_on = ?, certificate_status = ?, certificate_pdf_path = ?;
         |""".stripMargin

    val projectParams = Seq(
      // Insert parameters
      projectId, solutionId, createdBy, createdDate, completedDate, lastSync, updatedDate, status, remarks,
      evidence, evidenceCount, programId, taskCount, userRoleIds, userRoles, orgId, orgName, orgCode, stateId,
      stateName, districtId, districtName, blockId, blockName, clusterId, clusterName, schoolId, schoolName,
      certificateTemplateId, certificateTemplateUrl, certificateIssuedOn, certificateStatus, certificatePdfPath,

      // Update parameters (matching columns in the ON CONFLICT clause)
      solutionId, createdBy, createdDate, completedDate, lastSync, updatedDate, status, remarks, evidence,
      evidenceCount, programId, taskCount, userRoleIds, userRoles, orgId, orgName, orgCode, stateId, stateName,
      districtId, districtName, blockId, blockName, clusterId, clusterName, schoolId, schoolName,
      certificateTemplateId, certificateTemplateUrl, certificateIssuedOn, certificateStatus, certificatePdfPath
    )

    postgresUtil.executePreparedUpdate(upsertProjectQuery, projectParams, config.projects, projectId)

    /**
     * Extracting Tasks data
     */
    tasksData.foreach { task =>
      val taskId = task("taskId").toString
      val taskName = task("taskName")
      val taskAssignedTo = task("taskAssignedTo")
      val taskStartDate = task("taskStartDate")
      val taskEndDate = task("taskEndDate")
      val taskSyncedAt = task("taskSyncedAt")
      val taskIsDeleted = task("taskIsDeleted")
      val taskIsDeletable = task("taskIsDeletable")
      val taskRemarks = task("taskRemarks")
      val taskStatus = task("taskStatus")
      val taskEvidence = task("taskEvidence")
      val taskEvidenceCount = task("taskEvidenceCount")

      val upsertTaskQuery =
        s"""INSERT INTO ${config.tasks} (
           |    task_id, project_id, name, assigned_to, start_date, end_date, synced_at, is_deleted, is_deletable,
           |    remarks, status, evidence, evidence_count
           |) VALUES (
           |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           |) ON CONFLICT (task_id) DO UPDATE SET
           |    name = ?, project_id = ?, assigned_to = ?, start_date = ?, end_date = ?, synced_at = ?,
           |    is_deleted = ?, is_deletable = ?, remarks = ?, status = ?, evidence = ?, evidence_count = ?;
           |""".stripMargin

      val taskParams = Seq(
        // Insert parameters
        taskId, projectId, taskName, taskAssignedTo, taskStartDate, taskEndDate, taskSyncedAt, taskIsDeleted,
        taskIsDeletable, taskRemarks, taskStatus, taskEvidence, taskEvidenceCount,

        // Update parameters (matching columns in the ON CONFLICT clause)
        taskName, projectId, taskAssignedTo, taskStartDate, taskEndDate, taskSyncedAt, taskIsDeleted,
        taskIsDeletable, taskRemarks, taskStatus, taskEvidence, taskEvidenceCount
      )

      postgresUtil.executePreparedUpdate(upsertTaskQuery, taskParams, config.tasks, taskId)

    }

    /**
     * Logic to populate kafka messages for creating metabase dashboard
     */
    val dashboardData = new java.util.HashMap[String, String]()
    val dashboardConfig = Seq(
      ("admin", "1", "admin"),
      ("program", event.programId, "targetedProgram"),
      ("state", event.stateId, "targetedState"),
      ("district", event.districtId, "targetedDistrict")
    )

    dashboardConfig
      .filter { case (key, _, _) => config.reportsEnabled.contains(key) }
      .foreach { case (key, value, target) =>
        checkAndInsert(key, value, dashboardData, target)
      }

    if (!dashboardData.isEmpty) {
      pushProjectDashboardEvents(dashboardData, context)
    }

    println(s"\n***************** End of Processing the Project Event *****************")

  }

  def extractEvidenceData(attachments: List[Map[String, Any]]): (String, Int) = {
    val evidenceList = attachments.map { attachment =>
      if (attachment.get("type").contains("link")) {
        attachment.get("name").map(_.toString).getOrElse("")
      } else {
        attachment.get("sourcePath").map(_.toString).getOrElse("")
      }
    }
    (evidenceList.mkString(", "), evidenceList.length)
  }

  def extractUserRolesData(roles: List[Map[String, Any]]): (String, String) = {
    if (roles == null || roles.isEmpty) {
      ("", "")
    } else {
      val roleId = roles.map { role => role.get("id").map(_.toString).getOrElse("") }
      val roleName = roles.map { role => role.get("title").map(_.toString).getOrElse("") }
      (roleId.mkString(", "), roleName.mkString(", "))
    }
  }

  def extractProjectCategories(categories: List[Map[String, Any]]): String = {
    Option(categories)
      .getOrElse(Nil)
      .flatMap(_.get("name") match {
        case Some(value) if value != null && value.toString.trim.nonEmpty => Some(value.toString)
        case _ => None
      })
      .mkString(",")
  }

  def extractLocationsData(locations: List[Map[String, Any]]): List[Map[String, String]] = {
    locations.flatMap { location =>
      location.get("type").map(_.toString.trim).filter(_.nonEmpty).flatMap { locationType =>
        val code = location.get("code").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        val externalId = location.get("id").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        val name = location.get("name").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        Some(Map(
          s"${locationType}Code" -> code,
          s"${locationType}ExternalId" -> externalId,
          s"${locationType}Name" -> name
        ))
      }
    }
  }

  def extractTasksData(tasks: List[Map[String, Any]]): List[Map[String, Any]] = {
    tasks.map { task =>
      def extractField(field: String): String = task.get(field).map(key => if (key.toString.trim.isEmpty) "Null" else key.toString).getOrElse("Null")

      val taskEvidenceList: List[Map[String, Any]] = task.get("attachments").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty[Map[String, Any]])
      val (taskEvidence, taskEvidenceCount) = extractEvidenceData(taskEvidenceList)

      Map(
        "taskId" -> extractField("_id"),
        "taskName" -> extractField("name"),
        "taskAssignedTo" -> extractField("assignee"),
        "taskStartDate" -> extractField("startDate"),
        "taskEndDate" -> extractField("endDate"),
        "taskSyncedAt" -> extractField("syncedAt"),
        "taskIsDeleted" -> extractField("isDeleted"),
        "taskIsDeletable" -> extractField("isDeletable"),
        "taskRemarks" -> extractField("remarks"),
        "taskStatus" -> extractField("status"),
        "taskEvidence" -> taskEvidence,
        "taskEvidenceCount" -> taskEvidenceCount
      )
    }
  }

  def extractLocationDetail(locationsData: List[Map[String, String]], key: String): String = {
    locationsData.collectFirst {
      case location if location.contains(key) => location(key)
    }.getOrElse("Null")
  }

  def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
    val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboard_metadata} WHERE entity_id = '$targetedId') AS is_${entityType}_present"
    val result = postgresUtil.fetchData(query)

    result.foreach { row =>
      row.get(s"is_${entityType}_present") match {
        case Some(isPresent: Boolean) if isPresent =>
          println(s"$entityType details already exist.")
        case _ =>
          if (entityType == "admin") {
            val insertQuery = s"INSERT INTO ${config.dashboard_metadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', 'Admin', '$targetedId')"
            val affectedRows = postgresUtil.insertData(insertQuery)
            println(s"Inserted Admin details. Affected rows: $affectedRows")
            dashboardData.put(dashboardKey, "1")
          } else {
            val getEntityNameQuery = s"SELECT DISTINCT ${entityType}_name AS ${entityType}_name FROM ${if (entityType == "program") config.solutions else config.projects} WHERE ${entityType}_id = '$targetedId'"
            val result = postgresUtil.fetchData(getEntityNameQuery)
            result.foreach { id =>
              val entityName = id.get(s"${entityType}_name").map(_.toString).getOrElse("")
              val upsertMetaDataQuery =
                s"""INSERT INTO ${config.dashboard_metadata} (
                   |    entity_type, entity_name, entity_id
                   |) VALUES (
                   |    ?, ?, ?
                   |) ON CONFLICT (entity_id) DO UPDATE SET
                   |    entity_type = ?, entity_name = ?;
                   |""".stripMargin

              val dashboardParams = Seq(
                entityType, entityName, targetedId, // Insert parameters
                entityType, entityName // Update parameters (matching columns in the ON CONFLICT clause)
              )
              postgresUtil.executePreparedUpdate(upsertMetaDataQuery, dashboardParams, config.dashboard_metadata, targetedId)
              println(s"Inserted [$entityName : $targetedId] details.")
              dashboardData.put(dashboardKey, targetedId)
            }
          }
      }
    }
  }

  def pushProjectDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context): util.HashMap[String, AnyRef] = {
    val objects = new util.HashMap[String, AnyRef]() {
      put("_id", java.util.UUID.randomUUID().toString)
      put("reportType", "Project")
      put("publishedAt", DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
      put("dashboardData", dashboardData)
    }
    val event = ScalaJsonUtil.serialize(objects)
    context.output(config.eventOutputTag, event)
    println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
    println(objects)
    objects
  }

}