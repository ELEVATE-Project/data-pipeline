package org.shikshalokam.job.mitra.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest
import org.shikshalokam.job.mitra.stream.processor.utils.ClassificationResponse

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

class DiscussionEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  var thematicResult: ClassificationResponse = _ // Stores the LLM result

  def id: Int = readOrDefault[String]("id", null).replace(",", "").toInt

  def title: String = readOrDefault[String]("Title", null)

  def discussionDate: String = readOrDefault[String]("Date of Discussion", null)

  def createdAt: Timestamp = parseStringToTimestamp(readOrDefault[String]("event_pushed_at", ""))

  def role: String = readOrDefault[String]("Role", "Women Leader")

  def state: String = readOrDefault[String]("state", null)

  def district: String = readOrDefault[String]("District", null)

  def challenges: String = readOrDefault[String]("Challenges", null)

  private def parseStringToTimestamp(dateString: String): Timestamp = {
    if (dateString.isEmpty) new Timestamp(System.currentTimeMillis())
    else {
      try {
        Timestamp.from(Instant.parse(dateString))
      } catch {
        case _: Exception =>
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          new Timestamp(formatter.parse(dateString).getTime)
      }
    }
  }

}
