package org.shikshalokam.job.mentoring.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def tenantCode: String = readOrDefault[String]("dashboardData.tenantCode", "")

  def orgId: String = readOrDefault[String]("dashboardData.orgId", "")

  def orgName: String = readOrDefault[String]("dashboardData.orgName", "")

  def filterSync: String = readOrDefault("dashboardData.filterSync", "")

  def filterTable: String = readOrDefault("dashboardData.filterTable", "")
}
