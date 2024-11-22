package org.shikshalokam.job.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest
//import scala.jdk.CollectionConverters._  // For Scala 2.13+
import scala.collection.JavaConverters._  // Use this if you're on Scala 2.12 or below

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def reportType: String = readOrDefault[String]("reportType", "")

  def publishedAt: String = readOrDefault[String]("publishedAt", "")

  def admin: String = readOrDefault("dashboardData.admin","")

  def targetedProgram: String = readOrDefault("dashboardData.targetedProgram","")

  def targetedDistrict: String = readOrDefault("dashboardData.targetedDistrict","")

  def targetedState: String = readOrDefault("dashboardData.targetedState","")

  // Helper function to read list fields, handling both Java and Scala lists
}
