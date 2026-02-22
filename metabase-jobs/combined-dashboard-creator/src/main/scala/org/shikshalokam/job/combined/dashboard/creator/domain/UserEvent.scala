package org.shikshalokam.job.combined.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class UserEvent(eventMap: java.util.Map[String, Any], override val partition: Int, override val offset: Long) extends JobRequest(eventMap, partition, offset) {

  def tenantCode: String = readOrDefault[String]("dashboardData.tenantCode", null)

  def filterSync: String = readOrDefault("dashboardData.filterSync", "")

  def filterTable: String = readOrDefault("dashboardData.filterTable", "")
}
