package org.shikshalokam.job.observation.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def solutionId: String = readOrDefault[String]("solutionId", "")

  def solutionName : String = readOrDefault[String]("solutionInfo.name", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def status: String = readOrDefault[String]("status", "")

  def submissionNumber: Integer = readOrDefault[Integer]("submissionNumber", 0)

  def programName: String = readOrDefault[String]("programInfo.name", "")

  def programId: String = readOrDefault[String]("programInfo._id", "")

  def observationName : String = readOrDefault[String]("observationInformation.name", "")

  def observationId : String = readOrDefault[String]("observationId", "")

  def stateName: String = readOrDefault[String]("userProfile.state.label", "")

  def stateId: String = readOrDefault[String]("userProfile.state.value", "")

  def districtName: String = readOrDefault[String]("userProfile.district.label", "")

  def districtId: String = readOrDefault[String]("userProfile.district.value", "")

  def blockName: String = readOrDefault[String]("userProfile.block.label", "")

  def blockId: String = readOrDefault[String]("userProfile.block.value", "")

  def clusterName: String = readOrDefault[String]("userProfile.cluster.label", "")

  def clusterId: String = readOrDefault[String]("userProfile.cluster.value", "")

  def orgName: String = readOrDefault[String]("userProfile.organization.name", "")

  def orgId: String = readOrDefault[Int]("userProfile.organization.id", 0).toString

  def schoolName: String = readOrDefault[String]("entityInformation.name", "")

  def schoolId : String = readOrDefault[String]("entityId", "")

  def pointsBasedMaxScore: Integer = readOrDefault[Integer]("pointsBasedMaxScore", 0)

  def pointsBasedScoreAchieved: Integer = readOrDefault[Integer]("pointsBasedScoreAchieved", 0)

  def themes: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("themes", null)

  def criteria: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("criteria", null)

  def answers: Map[String, Any] = readOrDefault[Map[String, Any]]("answers", null)

  def userRoles: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("userProfile.user_roles", null)

  def isRubric: Boolean = readOrDefault[Boolean]("isRubricDriven", false)

  def completedDate: String = readOrDefault[String]("completedDate", "")

}

