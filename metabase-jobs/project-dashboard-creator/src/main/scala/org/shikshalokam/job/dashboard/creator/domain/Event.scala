package org.shikshalokam.job.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def reportType: String = readOrDefault[String]("reportType", "")

  def admin: String = readOrDefault("dashboardData.admin", "")

  def targetedState: String = readOrDefault("dashboardData.targetedState", "")

  def targetedDistrict: String = readOrDefault("dashboardData.targetedDistrict", "")

  def targetedProgram: String = readOrDefault("dashboardData.targetedProgram", "")

  def targetedSolution: String = readOrDefault("dashboardData.targetedSolution", "")

}
