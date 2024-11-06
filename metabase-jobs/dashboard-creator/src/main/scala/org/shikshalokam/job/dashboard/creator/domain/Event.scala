package org.shikshalokam.job.dashboard.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest
//import scala.jdk.CollectionConverters._  // For Scala 2.13+
import scala.collection.JavaConverters._  // Use this if you're on Scala 2.12 or below

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

  def reportType: String = readOrDefault[String]("reportType", "")

  def publishedAt: String = readOrDefault[String]("publishedAt", "")

  def admin: List[String] = readList("dashboardData.admin")

  def targetedProgram: List[String] = readList("dashboardData.targetedProgram")

  def targetedDistrict: List[String] = readList("dashboardData.targetedDistrict")

  def targetedState: List[String] = readList("dashboardData.targetedState")

  // Helper function to read list fields, handling both Java and Scala lists
  private def readList(key: String): List[String] = {
    readOrDefault[Any](key, List.empty[String]) match {
      case list: java.util.List[_] => list.asScala.toList.map(_.toString)
      case list: List[_] => list.map(_.toString)
      case _ => List.empty[String]
    }
  }
}
