package org.shikshalokam.job.project.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.project.task.ProjectStreamConfig
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

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

    val (projectEvidences, projectEvidencesCount) = extractEvidenceData(event.projectAttachments)
    println("projectEvidences = " + projectEvidences)
    println("projectEvidencesCount = " + projectEvidencesCount)

    //val organisationsData = extractOrganisationsData(event.organisations)
    //println(organisationsData)

    /** Required to feed orgIds into projects table */
    //val orgIdsString = organisationsData.map(_("orgId")).mkString(", ")
    //println(orgIdsString)

    //val locationsData = extractLocationsData(event.locations)
    //println(locationsData)

    val tasksData = extractTasksData(event.tasks)
    println("=======$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$========")
    println(tasksData)

    postgresUtil.createTable(config.createSolutionsTable, config.solutionsTable)
    postgresUtil.createTable(config.createProjectTable, config.projectsTable)
    postgresUtil.createTable(config.createTasksTable, config.tasksTable)

    /**
     * Extracting Solutions data
     */
    val solutionId = event.solutionId
    val solutionExternalId = event.solutionExternalId
    val solutionName = event.solutionName
    val solutionDescription = event.solutionDescription
    val projectDuration = event.projectDuration
    val hasAcceptedTAndC = event.hasAcceptedTAndC
    val projectIsDeleted = event.projectIsDeleted
    val projectCreatedType = event.projectCreatedType
    val programId = event.programId
    val programName = event.programName
    val programExternalId = event.programExternalId
    val programDescription = event.programDescription
    val privateProgram = event.privateProgram

    val upsertSolutionQuery =
      s"""INSERT INTO Solutions (solutionId, externalId, name, description, duration, hasAcceptedTAndC, isDeleted, createdType, programId, programName, programExternalId, programDescription, privateProgram)
         |VALUES ('$solutionId', '$solutionExternalId', '$solutionName', '$solutionDescription', '$projectDuration', '$hasAcceptedTAndC', $projectIsDeleted, '$projectCreatedType', '$programId', '$programName', '$programExternalId', '$programDescription', $privateProgram)
         |ON CONFLICT (solutionId) DO UPDATE SET
         |    externalId = '$solutionExternalId',
         |    name = '$solutionName',
         |    description = '$solutionDescription',
         |    duration = '$projectDuration',
         |    hasAcceptedTAndC = '$hasAcceptedTAndC',
         |    isDeleted = $projectIsDeleted,
         |    createdType = '$projectCreatedType',
         |    programId = '$programId',
         |    programName = '$programName',
         |    programExternalId = '$programExternalId',
         |    programDescription = '$programDescription',
         |    privateProgram = $privateProgram;
         |""".stripMargin

    postgresUtil.executeUpdate(upsertSolutionQuery, config.solutionsTable, solutionId)

    /**
     * Extracting Project data
     */
    val projectId = event.projectId
    val createdBy = event.createdBy
    val completedDate = event.completedDate
    val createdDate = event.createdAt
    val (evidence, evidenceCount) = extractEvidenceData(event.projectAttachments)
    val lastSync = event.projectLastSync
    val remarks = event.projectRemarks
    val updatedDate = event.projectUpdatedDate
    val projectStatus = event.projectStatus
//    val organisationId = organisationsData.map(_("orgId")).mkString(", ")
//    val stateCode = extractLocationDetail(locationsData, "stateCode")
//    val stateExternalId = extractLocationDetail(locationsData, "stateExternalId")
//    val stateName = extractLocationDetail(locationsData, "stateName")
//    val districtCode = extractLocationDetail(locationsData, "districtCode")
//    val districtExternalId = extractLocationDetail(locationsData, "districtExternalId")
//    val districtName = extractLocationDetail(locationsData, "districtName")
//    val blockCode = extractLocationDetail(locationsData, "blockCode")
//    val blockExternalId = extractLocationDetail(locationsData, "blockExternalId")
//    val blockName = extractLocationDetail(locationsData, "blockName")
//    val clusterCode = extractLocationDetail(locationsData, "clusterCode")
//    val clusterExternalId = extractLocationDetail(locationsData, "clusterExternalId")
//    val clusterName = extractLocationDetail(locationsData, "clusterName")
//    val schoolCode = extractLocationDetail(locationsData, "schoolCode")
//    val schoolExternalId = extractLocationDetail(locationsData, "schoolExternalId")
//    val schoolName = extractLocationDetail(locationsData, "schoolName")
    val boardName = event.boardName
    val taskCount = event.taskCount

    val upsertProjectQuery =
      s"""INSERT INTO Projects (
         |    projectId, createdBy, solutionId, programId, taskCount, completedDate, createdDate, evidence,
         |    evidenceCount, lastSync, remarks, updatedDate, projectStatus, boardName
         |) VALUES (
         |    '$projectId', '$createdBy', '$solutionId', '$programId', '$taskCount', '$completedDate',
         |    '$createdDate', '$evidence', '$evidenceCount', '$lastSync', '$remarks', '$updatedDate',
         |    '$projectStatus', '$boardName'
         |) ON CONFLICT (projectId) DO UPDATE SET
         |    createdBy = '$createdBy',
         |    solutionId = '$solutionId',
         |    programId = '$programId',
         |    taskCount = '$taskCount',
         |    completedDate = '$completedDate',
         |    createdDate = '$createdDate',
         |    evidence = '$evidence',
         |    evidenceCount = '$evidenceCount',
         |    lastSync = '$lastSync',
         |    remarks = '$remarks',
         |    updatedDate = '$updatedDate',
         |    projectStatus = '$projectStatus',
         |    boardName = '$boardName';
         |""".stripMargin

    postgresUtil.executeUpdate(upsertProjectQuery, config.projectsTable, projectId)

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
        s"""INSERT INTO Tasks (taskId, projectId, name, assignedTo, startDate, endDate, syncedAt, isDeleted, isDeletable, remarks, status, evidence, evidenceCount)
           |VALUES ('$taskId', '$projectId', '$taskName', '$taskAssignedTo', '$taskStartDate', '$taskEndDate', '$taskSyncedAt', $taskIsDeleted, $taskIsDeletable, '$taskRemarks', '$taskStatus', '$taskEvidence', $taskEvidenceCount)
           |ON CONFLICT (taskId) DO UPDATE SET
           |    name = '$taskName',
           |    projectId = '$projectId',
           |    assignedTo = '$taskAssignedTo',
           |    startDate = '$taskStartDate',
           |    endDate = '$taskEndDate',
           |    syncedAt = '$taskSyncedAt',
           |    isDeleted = $taskIsDeleted,
           |    isDeletable = $taskIsDeletable,
           |    remarks = '$taskRemarks',
           |    status = '$taskStatus',
           |    evidence = '$taskEvidence',
           |    evidenceCount = $taskEvidenceCount;
           |""".stripMargin

      postgresUtil.executeUpdate(upsertTaskQuery, config.tasksTable, taskId)
    }

  }


  def extractEvidenceData(Attachments: List[Map[String, Any]]): (String, Int) = {
    val evidenceList = Attachments.map { attachment =>
      if (attachment.get("type").contains("link")) {
        attachment.get("name").map(_.toString).getOrElse("")
      } else {
        attachment.get("sourcePath").map(_.toString).getOrElse("")
      }
    }
    (evidenceList.mkString(", "), evidenceList.length)
  }

  //  def extractOrganisationsData(Organisations: List[Map[String, Any]]): (String, String) = {
  //    val orgId = Organisations.map(organisation => organisation.get("organisationId").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null"))
  //    val orgName = Organisations.map(organisation => organisation.get("orgName").map(name => if (name.toString.trim.isEmpty) "Null" else name.toString).getOrElse("Null"))
  //    (orgId.mkString(", "), orgName.mkString(", "))
  //  }

  def extractOrganisationsData(organisations: List[Map[String, Any]]): List[Map[String, String]] = {
    organisations.map { organisation =>
      val id = organisation.get("organisationId").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
      val name = organisation.get("orgName").map(name => if (name.toString.trim.isEmpty) "Null" else name.toString).getOrElse("Null")
      Map("orgId" -> id, "orgName" -> name)
    }
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