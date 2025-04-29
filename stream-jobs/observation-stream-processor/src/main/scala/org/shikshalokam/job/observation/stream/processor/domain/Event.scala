package org.shikshalokam.job.observation.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def solutionId: String = readOrDefault[String]("solutionId", "")

  def createdBy: String = readOrDefault[String]("createdBy", "")

  def status: String = readOrDefault[String]("status", "")

  def submissionNumber: Integer = readOrDefault[Integer]("submissionNumber", 0)

  def stateName: String = readOrDefault[String]("userProfile.state.label", "")

  def districtName: String = readOrDefault[String]("userProfile.district.label", "")

  def blockName: String = readOrDefault[String]("userProfile.block.label", "")

  def clusterName: String = readOrDefault[String]("userProfile.cluster.label", "")

  def schoolName: String = readOrDefault[String]("entityInformation.name", "")

  def pointsBasedMaxScore: Integer = readOrDefault[Integer]("pointsBasedMaxScore", 0)

  def pointsBasedScoreAchieved: Integer = readOrDefault[Integer]("pointsBasedScoreAchieved", 0)

  def themes: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("themes", null)

  def criteria: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("criteria", null)

  def answers: Map[String, Any] = readOrDefault[Map[String, Any]]("answers", null)

  def isRubric: Boolean = readOrDefault[Boolean]("isRubricDriven", false)

  def solutionName : String = readOrDefault[String]("solutionInfo.name", "")
}

