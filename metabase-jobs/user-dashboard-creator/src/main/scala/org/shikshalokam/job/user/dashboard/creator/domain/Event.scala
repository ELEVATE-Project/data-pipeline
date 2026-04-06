package org.shikshalokam.job.user.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def tenantCode: String = readOrDefault[String]("dashboardData.tenantCode", null)

  def filterSync: String = readOrDefault("dashboardData.filterSync", "")

  def filterTable: String = readOrDefault("dashboardData.filterTable", "")
}
