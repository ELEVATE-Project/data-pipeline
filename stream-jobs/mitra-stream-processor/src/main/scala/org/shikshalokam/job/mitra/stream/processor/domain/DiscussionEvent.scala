package org.shikshalokam.job.mitra.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

class DiscussionEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def _id: String = readOrDefault[String]("_id", "")

}

