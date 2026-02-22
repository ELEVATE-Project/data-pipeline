package org.shikshalokam.job.combined.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

class MentoringEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def tenantCode: String = readOrDefault[String]("tenant_code", readOrDefault[String]("tenantCode", ""))
  def eventType: String = readOrDefault[String]("eventType", "")
  def entity: String = readOrDefault[String]("entity", "")
  def name: String = readOrDefault[String]("name", "")
  def status: String = readOrDefault[String]("status", "")
  def createdBy: String = readOrDefault[Any]("created_by", readOrDefault[Any]("createdBy", "")).toString
  def updatedBy: String = readOrDefault[Any]("updated_by", readOrDefault[Any]("updatedBy", "")).toString

  def createdAt: Timestamp = parseDate(readOrDefault[String]("created_at", readOrDefault[String]("createdAt", "")))
  def updatedAt: Timestamp = parseDate(readOrDefault[String]("updated_at", readOrDefault[String]("updatedAt", "")))
  def deletedAt: Timestamp = parseDate(readOrDefault[String]("deleted_at", readOrDefault[String]("deletedAt", "")))
  def isDeleted: Boolean = readOrDefault[Boolean]("deleted", readOrDefault[Boolean]("isDeleted", false))

  private def readInt(key: String, altKey: String = ""): Int = {
    val value = readOrDefault[Any](key, readOrDefault[Any](altKey, 0))
    value match {
      case i: Int => i
      case s: String => scala.util.Try(s.toInt).getOrElse(0)
      case _ => 0
    }
  }

  def sessionId: Int = readInt("session_id", "sessionId")
  def mentorId: Int = readInt("mentor_id", "mentorId")
  def sessionName: String = readOrDefault[String]("name", readOrDefault[String]("sessionName", ""))
  def sessionDesc: String = readOrDefault[String]("description", readOrDefault[String]("sessionDesc", ""))
  def sessionType: String = readOrDefault[String]("type", readOrDefault[String]("sessionType", ""))
  def sessionStatus: String = readOrDefault[String]("status", readOrDefault[String]("sessionStatus", ""))
  def platform: String = readOrDefault[String]("platform", "")
  def startedAt: Timestamp = parseDate(readOrDefault[String]("started_at", readOrDefault[String]("startedAt", "")))
  def completedAt: Timestamp = parseDate(readOrDefault[String]("completed_at", readOrDefault[String]("completedAt", "")))
  def startDate: Timestamp = parseDate(readOrDefault[String]("start_date", readOrDefault[String]("startDate", "")))
  def endDate: Timestamp = parseDate(readOrDefault[String]("end_date", readOrDefault[String]("endDate", "")))
  def recommendedFor: String = readOrDefault[Any]("recommended_for", readOrDefault[Any]("recommendedFor", "")).toString
  def categories: String = readOrDefault[Any]("categories", "").toString
  def medium: String = readOrDefault[Any]("medium", "").toString

  def attendanceId: Int = readInt("attendance_id", "attendanceId")
  def attendanceSessionId: Int = readInt("session_id", "attendanceSessionId")
  def menteeId: String = readOrDefault[Any]("mentee_id", readOrDefault[Any]("menteeId", "")).toString
  def joinedAt: Timestamp = parseDate(readOrDefault[String]("joined_at", readOrDefault[String]("joinedAt", "")))
  def leftAt: Timestamp = parseDate(readOrDefault[String]("left_at", readOrDefault[String]("leftAt", "")))
  def isFeedbackSkipped: Boolean = readOrDefault[Boolean]("is_feedback_skipped", readOrDefault[Boolean]("isFeedbackSkipped", false))

  def connectionId: String = readOrDefault[Any]("connection_id", readOrDefault[Any]("connectionId", "")).toString
  def userId: String = readOrDefault[Any]("user_id", readOrDefault[Any]("userId", "")).toString
  def friendId: String = readOrDefault[Any]("friend_id", readOrDefault[Any]("friendId", "")).toString
  def orgId: String = readOrDefault[Any]("org_id", readOrDefault[Any]("orgId", "")).toString
  def orgCode: String = readOrDefault[String]("org_code", readOrDefault[String]("orgCode", ""))
  def orgName: String = readOrDefault[String]("org_name", readOrDefault[String]("orgName", ""))

  def rating: Double = {
    val r = readOrDefault[Any]("rating", 0.0)
    r match {
      case d: Double => d
      case i: Int => i.toDouble
      case s: String => scala.util.Try(s.toDouble).getOrElse(0.0)
      case _ => 0.0
    }
  }
  def ratingUpdatedAt: Timestamp = parseDate(readOrDefault[String]("rating_updated_at", readOrDefault[String]("ratingUpdatedAt", "")))

  private def parseDate(dateString: String): Timestamp = {
    if (dateString.isEmpty) new Timestamp(System.currentTimeMillis())
    else {
      try {
        Timestamp.from(Instant.parse(dateString))
      } catch {
        case _: Exception =>
          try {
            val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            new Timestamp(formatter.parse(dateString).getTime)
          } catch {
            case _: Exception => new Timestamp(System.currentTimeMillis())
          }
      }
    }
  }
}
