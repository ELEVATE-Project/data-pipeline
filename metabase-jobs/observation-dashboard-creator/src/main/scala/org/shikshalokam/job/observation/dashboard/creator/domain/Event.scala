package org.shikshalokam.job.observation.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def reportType: String = readOrDefault[String]("reportType", "")

  def solution_id: String = readOrDefault("solution_id", "")

  def chartType: List[String] = readOrDefault[List[String]]("chart_type", List.empty[String])

  def solutionName: String = readOrDefault("solutionName", "")

  def admin: String = readOrDefault("dashboardData.admin", "")

  def isRubric: String = readOrDefault("isRubric", "")

  def targetedProgram: String = readOrDefault("dashboardData.targetedProgram", "")

}
