package org.shikshalokam.job.survey.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def programId: String = readOrDefault[String]("programInfo._id", "")

  def programExternalId: String = readOrDefault[String]("programInfo.programExternalId", "")

  def programName: String = readOrDefault[String]("programInfo.name", "")

  def programDescription: String = readOrDefault[String]("programInfo.description", "")

  def solutionId: String = readOrDefault[String]("surveyInformation.solutionId", "")

  def solutionExternalId: String = readOrDefault[String]("surveyInformation.solutionExternalId", "")

  def solutionName: String = readOrDefault[String]("surveyInformation.name", "")

  def solutionDescription: String = readOrDefault[String]("surveyInformation.description", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def tenantId: String = readOrDefault[String]("tenantId", "")

  def organisation: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.organizations", List.empty)

  def organisationId: String = readOrDefault[Int]("orgId", 0).toString

  def parentOrgId: String = readOrDefault[String]("programInformation.orgId", "")

  def status: String = readOrDefault[String]("status", "")

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

  def answers: Map[String, Any] = readOrDefault[Map[String, Any]]("answers", null)

  def completedDate: Timestamp = {
    val dateString = readOrDefault[String]("completedDate", "")
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

}

