package org.shikshalokam.job.project.creator.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {
  def _id: String = readOrDefault[String]("_id", "")
}
