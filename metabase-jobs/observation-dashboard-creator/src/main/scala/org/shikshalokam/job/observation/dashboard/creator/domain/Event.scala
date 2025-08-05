package org.shikshalokam.job.observation.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def reportType: String = readOrDefault[String]("reportType", "")

  def admin: String = readOrDefault("dashboardData.admin", "")

  def isRubric: String = readOrDefault("dashboardData.isRubric", "")

  def entityType: String = readOrDefault("dashboardData.entityType", "")

  def targetedProgram: String = readOrDefault("dashboardData.targetedProgram", "")

  def targetedSolution: String = readOrDefault("dashboardData.targetedSolution", "")

}
