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

    val organisationsData = extractOrganisationsData(event.organisations)
    println(organisationsData)

    /** Required to feed orgIds into projects table */
    //val orgIdsString = organisationsData.map(_("orgId")).mkString(", ")
    //println(orgIdsString)

    val locationsData = extractLocationsData(event.locations)
    println(locationsData)

    postgresUtil.createTable(config.createSolutionsTable, config.solutionsTable)
    postgresUtil.createTable(config.createProjectTable, config.projectsTable)

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


    val insertSolutionQuery =
      s"""INSERT INTO Solutions (solutionId, externalId, name, description, duration, hasAcceptedTAndC, isDeleted, createdType, programId, programName, programExternalId, programDescription, privateProgram)
         |VALUES ('$solutionId', '$solutionExternalId', '$solutionName', '$solutionDescription', '$projectDuration', '$hasAcceptedTAndC', $projectIsDeleted, '$projectCreatedType', '$programId', '$programName', '$programExternalId', '$programDescription', $privateProgram);
         |""".stripMargin

//    postgresUtil.insertData(insertSolutionQuery)

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
    val organisationId = organisationsData.map(_("orgId")).mkString(", ")
    val stateCode = extractLocationDetail(locationsData, "stateCode")
    val stateExternalId = extractLocationDetail(locationsData, "stateExternalId")
    val stateName = extractLocationDetail(locationsData, "stateName")
    val districtCode = extractLocationDetail(locationsData, "districtCode")
    val districtExternalId = extractLocationDetail(locationsData, "districtExternalId")
    val districtName = extractLocationDetail(locationsData, "districtName")
    val blockCode = extractLocationDetail(locationsData, "blockCode")
    val blockExternalId = extractLocationDetail(locationsData, "blockExternalId")
    val blockName = extractLocationDetail(locationsData, "blockName")
    val clusterCode = extractLocationDetail(locationsData, "clusterCode")
    val clusterExternalId = extractLocationDetail(locationsData, "clusterExternalId")
    val clusterName = extractLocationDetail(locationsData, "clusterName")
    val schoolCode = extractLocationDetail(locationsData, "schoolCode")
    val schoolExternalId = extractLocationDetail(locationsData, "schoolExternalId")
    val schoolName = extractLocationDetail(locationsData, "schoolName")
    val boardName = event.boardName

    val updateProjectQuery =
      s"""UPDATE Projects
         |SET
         |    createdBy = '$createdBy',
         |    solutionId = '$solutionId',
         |    programId = '$programId',
         |    completedDate = '$completedDate',
         |    createdDate = '$createdDate',
         |    evidence = '$evidence',
         |    evidenceCount = '$evidenceCount',
         |    lastSync = '$lastSync',
         |    remarks = '$remarks',
         |    updatedDate = '$updatedDate',
         |    projectStatus = '$projectStatus',
         |    organisationId = '$organisationId',
         |    stateCode = '$stateCode',
         |    stateExternalId = '$stateExternalId',
         |    stateName = '$stateName',
         |    districtCode = '$districtCode',
         |    districtExternalId = '$districtExternalId',
         |    districtName = '$districtName',
         |    blockCode = '$blockCode',
         |    blockExternalId = '$blockExternalId',
         |    blockName = '$blockName',
         |    clusterCode = '$clusterCode',
         |    clusterExternalId = '$clusterExternalId',
         |    clusterName = '$clusterName',
         |    schoolCode = '$schoolCode',
         |    schoolExternalId = '$schoolExternalId',
         |    schoolName = '$schoolName',
         |    boardName = '$boardName'
         |WHERE
         |    projectId = '$projectId';
         |""".stripMargin


    try {
      postgresUtil.insertData(updateProjectQuery) // Assuming insertData can also execute UPDATE statements
      println("Record updated successfully.")
    } catch {
      case e: Exception =>
        println("Error occurred while updating the record: " + e.getMessage)
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

  def extractLocationDetail(locationsData: List[Map[String, String]], key: String): String = {
    locationsData.collectFirst {
      case location if location.contains(key) => location(key)
    }.getOrElse("Null")
  }


}