package org.shikshalokam.job.combined.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class MentoringEvent(eventMap: java.util.Map[String, Any], override val partition: Int, override val offset: Long) extends JobRequest(eventMap, partition, offset) {

  def tenantCode: String = readOrDefault[String]("dashboardData.tenantCode", "")

  def orgId: String = readOrDefault[String]("dashboardData.orgId", "")

  def orgName: String = readOrDefault[String]("dashboardData.orgName", "")

  def filterSync: String = readOrDefault("dashboardData.filterSync", "")

  def filterTable: String = readOrDefault("dashboardData.filterTable", "")
}
