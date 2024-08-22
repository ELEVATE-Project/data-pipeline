package org.shikshalokam.job.project.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

 def _id: String = readOrDefault[String]("_id", "")

  def solutionId: String = readOrDefault[String]("solutionInformation._id", "")

  def solutionExternalId: String = readOrDefault[String]("solutionInformation.externalId", "")

  def solutionName: String = readOrDefault[String]("solutionInformation.name", "")

  def solutionDescription: String = readOrDefault[String]("solutionInformation.programDescription", "")

  def projectDuration: String = readOrDefault[String]("metaInformation.duration", "")

  def hasAcceptedTAndC: Boolean = readOrDefault[Boolean]("hasAcceptedTAndC", false)

  def projectIsDeleted: Boolean = readOrDefault[Boolean]("isDeleted", false)

  def projectCreatedType: String = if (readOrDefault[String]("projectTemplateId", "").nonEmpty) "project imported from library" else "user created project"

  def privateProgram: Boolean = readOrDefault[Boolean]("isAPrivateProgram", false)

  def programId: String = readOrDefault[String]("programInformation._id", "")

  def programExternalId: String = readOrDefault[String]("programInformation.externalId", "")

  def programName: String = readOrDefault[String]("programInformation.name", "")

  def programDescription: String = readOrDefault[String]("programInformation.description", "")

  def projectId: String = readOrDefault[String]("_id", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def createdAt: String = readOrDefault[String]("createdAt", "")

  def completedDate: String = if (readOrDefault[String]("status", "") == "submitted") readOrDefault[String]("updatedAt", "None") else "None"

  def projectLastSync: String = readOrDefault[String]("syncedAt", "")

  def projectRemarks: String = readOrDefault[String]("remarks", "")

  def projectUpdatedDate: String = readOrDefault[String]("updatedAt", "")

  def projectStatus: String = readOrDefault[String]("status", "")

  def organisationId: String = readOrDefault[Int]("userProfile.organization.id", 0).toString

  def organisationName: String = readOrDefault[String]("userProfile.organization.name", "")

  def organisationCode: String = readOrDefault[String]("userProfile.organization.code", "")

  def stateId: String = readOrDefault[String]("userProfile.state.value", "")

  def stateName: String = readOrDefault[String]("userProfile.state.label", "")

  def districtId: String = readOrDefault[String]("userProfile.district.value", "")

  def districtName: String = readOrDefault[String]("userProfile.district.label", "")

  def blockId: String = readOrDefault[String]("userProfile.block.value", "")

  def blockName: String = readOrDefault[String]("userProfile.block.label", "")

  def clusterId: String = readOrDefault[String]("userProfile.cluster.value", "")

  def clusterName: String = readOrDefault[String]("userProfile.cluster.label", "")

  def schoolId: String = readOrDefault[String]("userProfile.school.value", "")

  def schoolName: String = readOrDefault[String]("userProfile.school.label", "")

  def userRoles: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.user_roles", null)

  def projectAttachments: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("attachments", null)

  def tasks: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("tasks", null)

  val taskCount = tasks.size

}

