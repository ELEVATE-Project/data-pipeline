package org.shikshalokam.job.user.service.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def entity: String = readOrDefault[String]("entity", null)

  def eventType: String = readOrDefault[String]("eventType", null)

  def name: String = readOrDefault[String]("name", null)

  def username: String = readOrDefault[String]("username", null)

  def tenantCode: String = readOrDefault[String]("tenant_code", null)

  def email: String = readOrDefault[String]("email", null)

  def phone: String = readOrDefault[String]("phone", null)

  def organizations: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("organizations", List.empty)

  def stateId: String = readOrDefault("state.id", null)

  def districtId: String = readOrDefault("district.id", null)

  def status: String = readOrDefault[String]("status", null)

  def isUserDeleted: Boolean = readOrDefault[Boolean]("deleted", false)

  def oldValues: Map[String, Any] = readOrDefault[Map[String, Any]]("oldValues", Map.empty)

  def newValues: Map[String, Any] = readOrDefault[Map[String, Any]]("newValues", Map.empty)

  def programId: String = readOrDefault("meta.programInformation.id", null)

}
