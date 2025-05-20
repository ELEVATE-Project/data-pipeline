package org.shikshalokam.job.user.service.domain

import org.shikshalokam.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def status: String = readOrDefault[String]("status", "")

}
