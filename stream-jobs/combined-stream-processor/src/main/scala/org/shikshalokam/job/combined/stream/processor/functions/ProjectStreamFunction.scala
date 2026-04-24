package org.shikshalokam.job.combined.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.combined.stream.processor.domain.ProjectEvent
import org.shikshalokam.job.combined.stream.processor.task.UnifiedStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable._

class ProjectStreamFunction(config: UnifiedStreamConfig)(implicit val mapTypeInfo: TypeInformation[ProjectEvent], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[ProjectEvent, ProjectEvent](config) {

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

  override def processElement(event: ProjectEvent, context: ProcessFunction[ProjectEvent, ProjectEvent]#Context, metrics: Metrics): Unit = {
    try {
      if (event.projectStatus.toLowerCase() == "started" || event.projectStatus.toLowerCase() == "inprogress" || event.projectStatus.toLowerCase() == "submitted" || event.projectStatus.toLowerCase() == "completed") {
        logger.info(s"***************** Start of Processing the Project Event with Id = ${event._id} *****************")

      //TODO: TO be removed later
      val (projectEvidences, projectEvidencesCount) = extractEvidenceData(event.projectAttachments)
      val tasksData = extractTasksData(event.tasks)
      val projectCategories = Option(event.projectCategories).map(extractProjectCategories).getOrElse("")
      var userRoleIds: String = ""
      var userRoles: String = ""
      var orgId: String = ""
      var orgName: String = ""
      var orgCode: String = ""

      event.organisation.foreach { org =>
        if (org.get("code").contains(event.organisationId)) {
          orgName = org.get("name").map(_.toString).getOrElse("")
          orgId = org.get("id").map(_.toString).getOrElse("")
          orgCode = org.get("code").map(_.toString).getOrElse("")
          val roles = org.get("roles").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(List.empty)
          val (userRoleIdsExtracted, userRolesExtracted) = extractUserRolesData(roles)
          userRoleIds = userRoleIdsExtracted
          userRoles = userRolesExtracted
        } else {
          logger.info(s"Organisation with ID ${event.organisationId} not found in the event data.")
        }
      }

      //TODO: TO be removed later
      logger.info("==> Solutions data ")
      logger.info("solutionId = " + event.solutionId)
      logger.info("solutionExternalId = " + event.solutionExternalId)
      logger.info("solutionName = " + event.solutionName)
      logger.info("solutionDescription = " + event.solutionDescription)
      logger.info("duration = " + event.projectDuration)
      logger.info("categories = " + projectCategories)
      logger.info("privateProgram = " + event.privateProgram)
      logger.info("programId = " + event.programId)
      logger.info("programExternalId = " + event.programExternalId)
      logger.info("programName = " + event.programName)
      logger.info("programDescription = " + event.programDescription)

      logger.info("==> Project data")
      logger.info("projectId = " + event.projectId)
      logger.info("solutionId = " + event.solutionId)
      logger.info("createdBy = " + event.createdBy)
      logger.info("completedDate = " + event.completedDate)
      logger.info("createdAt = " + event.createdAt)
      logger.info("projectLastSync = " + event.projectLastSync)
      logger.info("projectUpdatedDate = " + event.projectUpdatedDate)
      logger.info("projectStatus = " + event.projectStatus)
      logger.info("projectRemarks = " + event.projectRemarks)
      logger.info("projectEvidences = " + projectEvidences)
      logger.info("projectEvidencesCount = " + projectEvidencesCount)
      logger.info("programId = " + event.programId)
      logger.info("taskCount = " + event.taskCount)
      logger.info("tenantId = " + event.tenantId)
      logger.info("stateId = " + event.stateId)
      logger.info("stateName = " + event.stateName)
      logger.info("districtId = " + event.districtId)
      logger.info("districtName = " + event.districtName)
      logger.info("blockId = " + event.blockId)
      logger.info("blockName = " + event.blockName)
      logger.info("clusterId = " + event.clusterId)
      logger.info("clusterName = " + event.clusterName)
      logger.info("schoolId = " + event.schoolId)
      logger.info("schoolName = " + event.schoolName)
      logger.info("certificateTemplateId = " + event.certificateTemplateId)
      logger.info("certificateTemplateUrl = " + event.certificateTemplateUrl)
      logger.info("certificateIssuedOn = " + event.certificateIssuedOn)
      logger.info("certificateStatus = " + event.certificateStatus)
      logger.info("certificatePdfPath = " + event.certificatePdfPath)
      logger.info("certificateEligibility = " + event.certificateEligibility)
      logger.info("certificateTransactionId = " + event.certificateTransactionId)
      logger.info("userRoleIds = " + userRoleIds)
      logger.info("userRoles = " + userRoles)
      logger.info("organisationId = " + event.organisationId)
      logger.info("organisationName = " + orgName)
      logger.info("organisationCode = " + orgCode)
      logger.info("parentOrgId = " + event.parentOrgId)
      logger.info("==> Tasks data")

      // Uncomment the bellow lines to create table schema for the first time.
      postgresUtil.createTable(config.createSolutionsTable, config.solutions)
      postgresUtil.createTable(config.createProjectTable, config.projects)
      postgresUtil.createTable(config.createTasksTable, config.tasks)
      postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)

      // Alter the project table to add tenant_id column if it does not exist
      postgresUtil.executeUpdate(config.AlterProjectTable, config.projects, "tenant_id")

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
      val parentOrgId = event.parentOrgId

      val upsertSolutionQuery =
        s"""INSERT INTO ${config.solutions} (solution_id, external_id, name, description, duration, categories, program_id, program_name, program_external_id, program_description, private_program, org_id)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
           |    private_program = ?,
           |    org_id = ?;
           |""".stripMargin

      val solutionParams = Seq(
        // Insert parameters
        solutionId, solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram, parentOrgId,

        // Update parameters (matching columns in the ON CONFLICT clause)
        solutionExternalId, solutionName, solutionDescription, projectDuration, projectCategories, programId, programName, programExternalId, programDescription, privateProgram, parentOrgId
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
      val tenantId = event.tenantId
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
      var certificateStatus = event.certificateStatus
      val certificatePdfPath = event.certificatePdfPath

      if (certificateStatus == "active") {
        certificateStatus = "Issued"
      } else if (event.certificateEligibility == "true") {
        certificateStatus = "Eligible"
      } else if (event.certificateEligibility == "false") {
        certificateStatus = "Not Eligible"
      } else if (event.certificateTransactionId != null && event.certificateTransactionId.nonEmpty && (certificatePdfPath == null || certificatePdfPath.isEmpty)) {
        certificateStatus = "In-Progress"
      }

      val projectsDashboardFilters: List[Map[String, String]] = List(
        Map(
          "state_name" -> stateName,
          "district_name" -> districtName,
          "block_name" -> blockName,
          "cluster_name" -> clusterName,
          "school_name" -> schoolName,
          "program_name" -> programName,
          "org_name" -> orgName,
          "table_name" -> config.projects
        )
      )

      val upsertProjectQuery =
        s"""INSERT INTO ${config.projects} (
           |    project_id, solution_id, created_by, created_date, completed_date, last_sync, updated_date, status, remarks,
           |    evidence, evidence_count, program_id, program_name, task_count, user_role_ids, user_roles, tenant_id, org_id, org_name, org_code, state_id,
           |    state_name, district_id, district_name, block_id, block_name, cluster_id, cluster_name, school_id, school_name,
           |    certificate_template_id, certificate_template_url, certificate_issued_on, certificate_status, certificate_pdf_path
           |) VALUES (
           |    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           |) ON CONFLICT (project_id) DO UPDATE SET
           |    solution_id = ?, created_by = ?, created_date = ?, completed_date = ?, last_sync = ?, updated_date = ?,
           |    status = ?, remarks = ?, evidence = ?, evidence_count = ?, program_id = ?, program_name = ?, task_count = ?, user_role_ids = ?,
           |    user_roles = ?, tenant_id = ?, org_id = ?, org_name = ?, org_code = ?, state_id = ?, state_name = ?, district_id = ?,
           |    district_name = ?, block_id = ?, block_name = ?, cluster_id = ?, cluster_name = ?, school_id = ?, school_name = ?,
           |    certificate_template_id = ?, certificate_template_url = ?, certificate_issued_on = ?, certificate_status = ?, certificate_pdf_path = ?;
           |""".stripMargin

      val projectParams = Seq(
        // Insert parameters
        projectId, solutionId, createdBy, createdDate, completedDate, lastSync, updatedDate, status, remarks,
        evidence, evidenceCount, programId, programName, taskCount, userRoleIds, userRoles, tenantId, orgId, orgName, orgCode, stateId,
        stateName, districtId, districtName, blockId, blockName, clusterId, clusterName, schoolId, schoolName,
        certificateTemplateId, certificateTemplateUrl, certificateIssuedOn, certificateStatus, certificatePdfPath,

        // Update parameters (matching columns in the ON CONFLICT clause)
        solutionId, createdBy, createdDate, completedDate, lastSync, updatedDate, status, remarks, evidence,
        evidenceCount, programId, programName, taskCount, userRoleIds, userRoles, tenantId, orgId, orgName, orgCode, stateId, stateName,
        districtId, districtName, blockId, blockName, clusterId, clusterName, schoolId, schoolName,
        certificateTemplateId, certificateTemplateUrl, certificateIssuedOn, certificateStatus, certificatePdfPath
      )
      checkExistenceOfFilterData(projectsDashboardFilters, context, solutionId)
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
        ("state", event.stateId, "targetedState"),
        ("district", event.districtId, "targetedDistrict"),
        ("program", event.programId, "targetedProgram"),
        ("solution", event.solutionId, "targetedSolution")
      )

      dashboardConfig
        .filter { case (key, _, _) => config.projectReportsEnabled.contains(key) }
        .foreach { case (key, value, target) =>
          checkAndInsert(key, value, dashboardData, target)
        }

      if (!dashboardData.isEmpty) {
        pushProjectDashboardEvents(dashboardData, context)
      }

      logger.info(s"***************** End of Processing the Project Event *****************")
    } else {
      logger.info(s"Skipping the project event with Id = ${event._id} and status = ${event.projectStatus} as it is not in a valid status.")
    }

    def checkAndInsert(entityType: String, targetedId: String, dashboardData: java.util.HashMap[String, String], dashboardKey: String): Unit = {
      val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboard_metadata} WHERE entity_id = '$targetedId') AS is_${entityType}_present"
      val result = postgresUtil.fetchData(query)

      result.foreach { row =>
        row.get(s"is_${entityType}_present") match {
          case Some(isPresent: Boolean) if isPresent =>
            logger.info(s"$entityType details already exist.")
          case _ =>
            if (entityType == "admin") {
              val insertQuery = s"INSERT INTO ${config.dashboard_metadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', 'Admin', '$targetedId')"
              val affectedRows = postgresUtil.insertData(insertQuery)
              logger.info(s"Inserted Admin details. Affected rows: $affectedRows")
              dashboardData.put(dashboardKey, "1")
            } else {
              val (entityColumn, sourceTable) = entityType match {
                case "program" => (s"${entityType}_name", config.solutions)
                case "solution" => ("name", config.solutions)
                case "state" => (s"${entityType}_name", config.projects)
                case "district" => (s"${entityType}_name", config.projects)
              }

              val getEntityNameQuery =
                s"""
                   |SELECT DISTINCT $entityColumn AS ${entityType}_name
                   |FROM $sourceTable
                   |WHERE ${entityType}_id = '$targetedId'
                   |""".stripMargin.replaceAll("\n", " ")

              val result = postgresUtil.fetchData(getEntityNameQuery)
              result.foreach { id =>
                val entityName = id.get(s"${entityType}_name").map(_.toString).getOrElse("")

                if (entityType == "solution") {
                  // Special insert/upsert logic for solution only
                  val upsertQuery =
                    s"""INSERT INTO ${config.dashboard_metadata} (
                       |    entity_type, entity_name, entity_id,
                       |    report_type, is_rubrics, parent_name, linked_to
                       |) VALUES (?, ?, ?, ?, ?, ?, ?)
                       |ON CONFLICT (entity_id) DO UPDATE SET
                       |    entity_type = EXCLUDED.entity_type,
                       |    entity_name = EXCLUDED.entity_name,
                       |    report_type = EXCLUDED.report_type,
                       |    is_rubrics = EXCLUDED.is_rubrics,
                       |    parent_name = EXCLUDED.parent_name,
                       |    linked_to = EXCLUDED.linked_to
                       |""".stripMargin

                  val params = Seq(
                    entityType, entityName, targetedId,
                    "improvementProject", false, event.entityType, event.programId
                  )
                  postgresUtil.executePreparedUpdate(upsertQuery, params, config.dashboard_metadata, targetedId)
                  logger.info(s"Inserted [$entityName : $targetedId] with reportType=improvementProject, isRubric=false, parent_name=${event.entityType}, linked_to=${event.programId}.")
                } else {
                  // Default logic for program or others
                  val insertQuery =
                    s"""INSERT INTO ${config.dashboard_metadata} (
                       |    entity_type, entity_name, entity_id
                       |) VALUES ('$entityType', '$entityName', '$targetedId')
                       |ON CONFLICT (entity_id) DO NOTHING
                       |""".stripMargin.replaceAll("\n", " ")

                  val affectedRows = postgresUtil.insertData(insertQuery)
                  logger.info(s"Inserted [$entityName : $targetedId] with default metadata. Affected rows: $affectedRows")
                }

                dashboardData.put(dashboardKey, targetedId)
              }
            }
        }
      }
    }

    def checkExistenceOfFilterData(filterList: List[Map[String, String]], context: ProcessFunction[ProjectEvent, ProjectEvent]#Context, solutionId: String): Unit = {
      logger.info(">>>>>>>>>>>>>>>>>>Checking existence of filter data...")

      filterList.foreach { filter =>
        val tableName = filter.getOrElse("table_name", "")
        val columnValuePairs = filter.filterKeys(_ != "table_name").filter { case (_, v) => v != null && v.nonEmpty }

        if (tableName.nonEmpty && columnValuePairs.nonEmpty) {
          // Step 1: Check if table exists
          val tableExistsQuery =
            s"SELECT to_regclass('$tableName') IS NOT NULL AS table_exists"
          val tableExists = postgresUtil.executeQuery(tableExistsQuery) { rs =>
            if (rs.next()) rs.getBoolean("table_exists") else false
          }

          if (tableExists) {
            // Step 2: Check if data exists
            val whereClause = columnValuePairs.map { case (col, _) => s"""$col = ?""" }.mkString(" AND ")
            val params = columnValuePairs.values.toSeq
            val queryWithParams = params.foldLeft(
              s"SELECT EXISTS (SELECT 1 FROM $tableName WHERE $whereClause) AS data_exists"
            ) {
              case (q, v) => q.replaceFirst("\\?", s"'${v.replace("'", "''")}'")
            }

            val dataExists = postgresUtil.executeQuery(queryWithParams) { rs =>
              if (rs.next()) rs.getBoolean("data_exists") else false
            }
            val rowCountQuery = s"SELECT COUNT(*) AS row_count FROM $tableName"
            val rowCount = postgresUtil.executeQuery(rowCountQuery) { rs =>
              if (rs.next()) rs.getLong("row_count") else 0
            }
            if ((!dataExists && rowCount > 0)) {
              val eventData = new java.util.HashMap[String, String]()
              eventData.put("filterTable", tableName.stripPrefix("\"").stripSuffix("\""))
              eventData.put("filterSync", "Yes")
              eventData.put("targetedSolution", solutionId)
              logger.info(s"As DataExists = $dataExists & rowCount = $rowCount hence pushing event")
              pushProjectDashboardEvents(eventData, context)
            } else {
              logger.info(s"As DataExists = $dataExists & rowCount = $rowCount hence no event is pushed")
            }
          } else {
            logger.info(s"Table $tableName does not exist in Postgres")
          }
        }
      }
    }

    def pushProjectDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[ProjectEvent, ProjectEvent]#Context): util.HashMap[String, AnyRef] = {
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
      context.output(config.projectOutputTag, event)
      logger.info(s"----> Pushed new Kafka message to ${config.projectOutputTopic} topic")
      logger.info(objects.toString)
      objects
    }

    } catch {
      case e: Exception =>
        logger.error(s"Error processing project stream event with id: ${event._id}", e)
    }
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
}

