package org.shikshalokam.job.project.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def solutionId: String = readOrDefault[String]("solutionInformation._id", "")

  def solutionExternalId: String = readOrDefault[String]("solutionInformation.externalId", "")

  def solutionName: String = readOrDefault[String]("solutionInformation.name", "")

  def solutionDescription: String = readOrDefault[String]("solutionInformation.description", "")

  def privateProgram: Boolean = readOrDefault[Boolean]("isAPrivateProgram", false)

  def projectDuration: String = readOrDefault[String]("metaInformation.duration", "")

  def projectCategories: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("categories", null)

  def programId: String = readOrDefault[String]("programInformation._id", "")

  def programExternalId: String = readOrDefault[String]("programInformation.externalId", "")

  def programName: String = readOrDefault[String]("programInformation.name", "")

  def programDescription: String = readOrDefault[String]("programInformation.description", "")

  def projectId: String = readOrDefault[String]("_id", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def createdAt: Timestamp = {
    val dateString = readOrDefault[String]("createdAt", "")
    if (dateString.isEmpty) new Timestamp(System.currentTimeMillis())
    else {
      try {
        Timestamp.from(Instant.parse(dateString))
      } catch {
        case _: Exception =>
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          new Timestamp(formatter.parse(dateString).getTime)
      }
    }
  }

  def completedDate: String = if (readOrDefault[String]("status", "") == "submitted") readOrDefault[String]("updatedAt", "None") else "None"

  def projectLastSync: String = readOrDefault[String]("syncedAt", "")

  def projectRemarks: String = readOrDefault[String]("remarks", "")

  def projectUpdatedDate: String = readOrDefault[String]("updatedAt", "")

  def projectStatus: String = readOrDefault[String]("status", "")

  def tenantId: String = readOrDefault[String]("tenantId", "")

  def organisation: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.organizations", List.empty)

  def organisationId: String = readOrDefault[Int]("orgId", 0).toString

  def parentOrgId: String = readOrDefault[String]("programInformation.orgId", "")

  def stateId: String = readOrDefault[String]("userProfile.state.value", "")

  def stateName: String = readOrDefault[String]("userProfile.state.label", "")

  def districtId: String = readOrDefault[String]("userProfile.district.value", "")

  def districtName: String = readOrDefault[String]("userProfile.district.label", "")

  def blockId: String = readOrDefault[String]("userProfile.block.value", "")

  def blockName: String = readOrDefault[String]("userProfile.block.label", "")

  def clusterId: String = readOrDefault[String]("userProfile.cluster.value", "")

  def clusterName: String = readOrDefault[String]("userProfile.cluster.label", "")

  def schoolId: String = readOrDefault[String]("userProfile.school.externalId", "")

  def schoolName: String = readOrDefault[String]("userProfile.school.label", "")

  def userRoles: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.user_roles", List.empty)

  def projectAttachments: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("attachments", null)

  def tasks: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("tasks", null)

  val taskCount = tasks.size

  def certificateTemplateId: String = readOrDefault[String]("certificate.templateId", "")

  def certificateTemplateUrl: String = readOrDefault[String]("certificate.templateUrl", "")

  def certificateIssuedOn: String = readOrDefault[String]("certificate.issuedOn", "")

  def certificateStatus: String = readOrDefault[String]("certificate.status", "")

  def certificateEligibility: String = read[Boolean]("certificate.eligible").map(_.toString).getOrElse("")

  def certificateTransactionId: String = readOrDefault[String]("certificate.transactionId", "")

  def certificatePdfPath: String = readOrDefault[String]("certificate.pdfPath", "")

  def entityType: String = readOrDefault[String]("entityInformation.entityType", null)

}

